package grpctransport

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/analytics"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/hazards"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var errStoreDown = errors.New("remote wall time is too far ahead (4m59s) to be trustworthy (SQLSTATE XXUUU)")

// outageStore is the store behind the read RPCs. down fails every data
// query; healthDown fails the completeness lookup (the real store keeps its
// last completeness answer through an outage, so the two differ).
type outageStore struct {
	down       atomic.Bool
	healthDown atomic.Bool
}

func (s *outageStore) ListHazards(context.Context, hazards.ListFilter) ([]domain.HazardEvent, error) {
	if s.down.Load() {
		return nil, errStoreDown
	}
	return []domain.HazardEvent{{HazardID: "h-1", Source: domain.SourceBMKG, HazardType: domain.HazardSeismic, Severity: domain.SeverityWaspada}}, nil
}

func (s *outageStore) GetHazard(_ context.Context, id string) (domain.HazardEvent, error) {
	if s.down.Load() {
		return domain.HazardEvent{}, errStoreDown
	}
	if id != "h-1" {
		return domain.HazardEvent{}, domain.ErrNotFound
	}
	return domain.HazardEvent{HazardID: "h-1"}, nil
}

func (s *outageStore) GetSourceStatus(_ context.Context, source domain.Source) (domain.SourceStatus, error) {
	if s.down.Load() {
		return domain.SourceStatus{}, errStoreDown
	}
	return domain.SourceStatus{Source: source, Status: domain.SourceStateUnknown}, nil
}

func (s *outageStore) Aggregate(context.Context, analytics.Filter) (analytics.Snapshot, error) {
	if s.down.Load() {
		return analytics.Snapshot{}, errStoreDown
	}
	asOf := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	return analytics.Snapshot{AsOf: asOf, Rows: []analytics.Row{{Bucket: asOf.Truncate(24 * time.Hour), Category: domain.HazardSeismic, Severity: domain.SeverityWaspada, AreaName: "Cilacap", Count: 7}}}, nil
}

func (s *outageStore) UnavailableUnits(context.Context) ([]string, error) {
	if s.healthDown.Load() {
		return nil, errStoreDown
	}
	return nil, nil
}

// cacheTTL is short so a test reaches "older than ttl" by sleeping a moment.
const cacheTTL = 5 * time.Millisecond

func outageHandler() (*queryHandler, *outageStore) {
	store := &outageStore{}
	return newQueryHandler(hazards.NewQueries(store).WithListCache(cacheTTL), nil, analytics.NewQueries(store).WithCache(cacheTTL), nil, store), store
}

// headerStream records the response headers a handler sets.
type headerStream struct {
	mu     sync.Mutex
	header metadata.MD
}

func (s *headerStream) Method() string { return "/bnpb.aggregator.Aggregator/Test" }
func (s *headerStream) SetHeader(md metadata.MD) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.header = metadata.Join(s.header, md)
	return nil
}
func (s *headerStream) SendHeader(md metadata.MD) error { return s.SetHeader(md) }
func (s *headerStream) SetTrailer(metadata.MD) error    { return nil }

func (s *headerStream) staleSince() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if values := s.header.Get(staleSinceHeader); len(values) > 0 {
		return values[0]
	}
	return ""
}

func streamContext() (context.Context, *headerStream) {
	stream := &headerStream{}
	return grpc.NewContextWithServerTransportStream(context.Background(), stream), stream
}

// The 18 Nov outage seen by the Client API: a listing answered a moment ago
// keeps being answered while the store refuses every statement, marked
// partial with a non-empty unavailable_units and the age of the page.
func TestListHazardsStaysAnsweredMarkedPartialWhileTheStoreIsDown(t *testing.T) {
	handler, store := outageHandler()
	request := &aggregatorpb.ListHazardsRequest{PageSize: 50}
	ctx, stream := streamContext()
	response, err := handler.ListHazards(ctx, request)
	if err != nil || response.GetPartial() || len(response.GetHazards()) != 1 || stream.staleSince() != "" {
		t.Fatalf("before the outage: %v, %v, stale since %q; want a live complete page", response, err, stream.staleSince())
	}

	store.down.Store(true)
	for wait := time.Now().Add(2 * time.Second); ; time.Sleep(cacheTTL) {
		ctx, stream = streamContext()
		response, err = handler.ListHazards(ctx, request)
		if err != nil {
			t.Fatalf("the store error reached the caller although the page was answered before: %v", err)
		}
		if response.GetPartial() {
			break
		}
		if time.Now().After(wait) {
			t.Fatal("the page was never marked partial during the outage")
		}
	}
	if len(response.GetHazards()) != 1 || response.GetHazards()[0].GetHazardId() != "h-1" {
		t.Fatalf("hazards = %v, want the last good page", response.GetHazards())
	}
	if units := response.GetUnavailableUnits(); len(units) != 1 || units[0] != "store" {
		t.Fatalf("unavailable_units = %v, want [store]", units)
	}
	if _, err := time.Parse(time.RFC3339, stream.staleSince()); err != nil {
		t.Fatalf("%s = %q, want the RFC3339 time the page was read", staleSinceHeader, stream.staleSince())
	}
}

func TestGetAnalyticsStaysAnsweredMarkedPartialWhileTheStoreIsDown(t *testing.T) {
	handler, store := outageHandler()
	request := &aggregatorpb.AnalyticsRequest{Bucket: "day"}
	ctx, _ := streamContext()
	first, err := handler.GetAnalytics(ctx, request)
	if err != nil || first.GetPartial() || len(first.GetHazardTypes()) != 1 {
		t.Fatalf("before the outage: %v, %v; want live complete counts", first, err)
	}

	store.down.Store(true)
	var (
		response *aggregatorpb.AnalyticsResponse
		stream   *headerStream
	)
	for wait := time.Now().Add(2 * time.Second); ; time.Sleep(cacheTTL) {
		ctx, stream = streamContext()
		response, err = handler.GetAnalytics(ctx, request)
		if err != nil {
			t.Fatalf("the store error reached the caller although the window was answered before: %v", err)
		}
		if response.GetPartial() {
			break
		}
		if time.Now().After(wait) {
			t.Fatal("the counts were never marked partial during the outage")
		}
	}
	if len(response.GetHazardTypes()) != 1 || response.GetHazardTypes()[0].GetCount() != 7 || response.GetAsOf() != first.GetAsOf() {
		t.Fatalf("response = %v, want the last counts with their own as_of %s", response, first.GetAsOf())
	}
	if units := response.GetUnavailableUnits(); len(units) != 1 || units[0] != "store" {
		t.Fatalf("unavailable_units = %v, want [store]", units)
	}
	if _, err := time.Parse(time.RFC3339, stream.staleSince()); err != nil {
		t.Fatalf("%s = %q, want the RFC3339 time the counts were read", staleSinceHeader, stream.staleSince())
	}
}

// Nothing answered before: the store failure is Unavailable (503 at the
// Client API, retried on another instance), not Internal (502) and, for one
// hazard, not NotFound.
func TestReadsAnswerUnavailableWhenTheStoreIsDownAndNothingWasAnsweredBefore(t *testing.T) {
	handler, store := outageHandler()
	store.down.Store(true)
	ctx := context.Background()
	calls := map[string]func() error{
		"ListHazards": func() error {
			_, err := handler.ListHazards(ctx, &aggregatorpb.ListHazardsRequest{})
			return err
		},
		"GetHazard": func() error {
			_, err := handler.GetHazard(ctx, &aggregatorpb.GetHazardRequest{HazardId: "h-1"})
			return err
		},
		"GetSourceStatus": func() error {
			_, err := handler.GetSourceStatus(ctx, &aggregatorpb.GetSourceStatusRequest{})
			return err
		},
		"GetAnalytics": func() error {
			_, err := handler.GetAnalytics(ctx, &aggregatorpb.AnalyticsRequest{})
			return err
		},
	}
	for name, call := range calls {
		if err := call(); status.Code(err) != codes.Unavailable {
			t.Errorf("%s: code = %s (%v), want Unavailable", name, status.Code(err), err)
		}
	}
}

func TestReadsKeepCallerMistakesApartFromStoreFailures(t *testing.T) {
	handler, _ := outageHandler()
	ctx := context.Background()
	if _, err := handler.GetHazard(ctx, &aggregatorpb.GetHazardRequest{HazardId: "missing"}); status.Code(err) != codes.NotFound {
		t.Errorf("missing hazard: code = %s, want NotFound", status.Code(err))
	}
	if _, err := handler.ListHazards(ctx, &aggregatorpb.ListHazardsRequest{PageToken: "h-1"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("bad page token: code = %s, want InvalidArgument", status.Code(err))
	}
	if _, err := handler.GetAnalytics(ctx, &aggregatorpb.AnalyticsRequest{Bucket: "year"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("bad bucket: code = %s, want InvalidArgument", status.Code(err))
	}
}

// The completeness lookup failing must neither fail the request nor be
// reported as "complete" (it was silently ignored for listings and an error
// for analytics).
func TestReadsAnswerPartialWhenCompletenessIsUnknown(t *testing.T) {
	handler, store := outageHandler()
	store.healthDown.Store(true)
	list, err := handler.ListHazards(context.Background(), &aggregatorpb.ListHazardsRequest{})
	if err != nil || !list.GetPartial() || len(list.GetUnavailableUnits()) != 1 || list.GetUnavailableUnits()[0] != "store" {
		t.Errorf("ListHazards = %v, %v; want the page marked partial with [store]", list, err)
	}
	counts, err := handler.GetAnalytics(context.Background(), &aggregatorpb.AnalyticsRequest{})
	if err != nil || !counts.GetPartial() || len(counts.GetUnavailableUnits()) != 1 || counts.GetUnavailableUnits()[0] != "store" {
		t.Errorf("GetAnalytics = %v, %v; want the counts marked partial with [store]", counts, err)
	}
}
