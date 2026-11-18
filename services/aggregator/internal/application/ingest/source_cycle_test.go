package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/adapters/memory"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/google/uuid"
)

func TestWarningsByEventIDCorrelatesUsingRelatedEventID(t *testing.T) {
	warning := map[string]json.RawMessage{
		"warning_id":       json.RawMessage(`"warning-1"`),
		"related_event_id": json.RawMessage(`"event-1"`),
	}

	warnings := warningsByEventID([]map[string]json.RawMessage{warning})
	if warnings["event-1"]["warning_id"] == nil {
		t.Fatalf("warning was not indexed by related event ID: %#v", warnings)
	}
	if _, indexedByWarningID := warnings["warning-1"]; indexedByWarningID {
		t.Fatal("warning was indexed by warning ID instead of related event ID")
	}
}

// fakeSource is a scriptable Source for scheduling tests.
type fakeSource struct {
	name  domain.Source
	fetch func(ctx context.Context, since time.Time, correlationID string) ([]domain.HazardEvent, error)
}

func (f *fakeSource) Name() domain.Source { return f.name }

func (f *fakeSource) Fetch(ctx context.Context, since time.Time, correlationID string) ([]domain.HazardEvent, error) {
	return f.fetch(ctx, since, correlationID)
}

func testEvent(id string, source domain.Source) domain.HazardEvent {
	return domain.HazardEvent{HazardID: id, Source: source, SourceRefID: id, HazardType: domain.HazardSeismic, Severity: domain.SeverityNormal, OccurredAt: time.Now().UTC(), IngestedAt: time.Now().UTC(), Attributes: map[string]any{}}
}

func testPoller(repository Repository) *Poller {
	return &Poller{repository: repository, interval: time.Hour, timeout: time.Second, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

type observingRepository struct {
	*memory.Repository
	advanced chan time.Time
	// failed is signalled once per recorded failure so tests can wait for a
	// park without racing the poller goroutine.
	failed   chan struct{}
	mu       sync.Mutex
	failures []domain.IngestFailure
}

func (s *observingRepository) SaveIngestFailure(_ context.Context, failure domain.IngestFailure) error {
	s.mu.Lock()
	s.failures = append(s.failures, failure)
	s.mu.Unlock()
	if s.failed != nil {
		select {
		case s.failed <- struct{}{}:
		default:
		}
	}
	return nil
}

func (s *observingRepository) recordedFailures() []domain.IngestFailure {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.IngestFailure(nil), s.failures...)
}

func (s *observingRepository) AdvancePollCursor(ctx context.Context, source domain.Source, cursor time.Time) error {
	if err := s.Repository.AdvancePollCursor(ctx, source, cursor); err != nil {
		return err
	}
	s.advanced <- cursor
	return nil
}

func TestRunSourceLoadsAndAdvancesPersistedCursorAfterSuccess(t *testing.T) {
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1)}
	starting := time.Now().UTC().Add(-time.Hour)
	if err := repository.Repository.AdvancePollCursor(context.Background(), domain.SourceBMKG, starting); err != nil {
		t.Fatal(err)
	}

	called := make(chan time.Time, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	source := &fakeSource{name: domain.SourceBMKG, fetch: func(_ context.Context, since time.Time, _ string) ([]domain.HazardEvent, error) {
		called <- since
		return nil, nil
	}}
	go poller.runSource(ctx, source)

	select {
	case since := <-called:
		if since.Before(starting.Add(-3*time.Second)) || since.After(starting) {
			t.Fatalf("poll did not use persisted cursor: since=%s cursor=%s", since, starting)
		}
	case <-time.After(time.Second):
		t.Fatal("poll did not start")
	}

	select {
	case advanced := <-repository.advanced:
		if !advanced.After(starting) {
			t.Fatalf("successful batch did not advance cursor: %s", advanced)
		}
	case <-time.After(time.Second):
		t.Fatal("cursor was not advanced after successful batch")
	}
	cancel()
}

func TestRunSourceDoesNotAdvanceCursorWhenFetchFails(t *testing.T) {
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1)}
	starting := time.Now().UTC().Add(-time.Hour)
	if err := repository.Repository.AdvancePollCursor(context.Background(), domain.SourcePVMBG, starting); err != nil {
		t.Fatal(err)
	}

	called := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	source := &fakeSource{name: domain.SourcePVMBG, fetch: func(_ context.Context, _ time.Time, _ string) ([]domain.HazardEvent, error) {
		called <- struct{}{}
		return nil, errors.New("source unavailable")
	}}
	go poller.runSource(ctx, source)

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("poll did not start")
	}
	select {
	case advanced := <-repository.advanced:
		t.Fatalf("failed batch advanced cursor to %s", advanced)
	case <-time.After(20 * time.Millisecond):
	}
	cursor, err := repository.GetPollCursor(context.Background(), domain.SourcePVMBG)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Equal(starting) {
		t.Fatalf("failed batch changed cursor: got %s, want %s", cursor, starting)
	}
	status, err := repository.GetSourceStatus(context.Background(), domain.SourcePVMBG)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != domain.SourceStateStale {
		t.Fatalf("failed batch status = %q, want %q", status.Status, domain.SourceStateStale)
	}
	cancel()
}

func TestRunSourcePersistsFetchedEvents(t *testing.T) {
	repository := memory.NewRepository()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	source := &fakeSource{name: domain.SourceBMKG, fetch: func(_ context.Context, _ time.Time, _ string) ([]domain.HazardEvent, error) {
		return []domain.HazardEvent{testEvent("h-1", domain.SourceBMKG)}, nil
	}}
	go poller.runSource(ctx, source)

	deadline := time.After(time.Second)
	for {
		event, err := repository.GetHazard(context.Background(), "h-1")
		if err == nil {
			if event.SourceRefID != "h-1" {
				t.Fatalf("persisted event = %+v", event)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatalf("event was not persisted: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestRunSourceAdvancesPastPoisonPayloads(t *testing.T) {
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	source := &fakeSource{name: domain.SourcePVMBG, fetch: func(_ context.Context, _ time.Time, _ string) ([]domain.HazardEvent, error) {
		return []domain.HazardEvent{testEvent("h-2", domain.SourcePVMBG)}, &MappingError{Source: domain.SourcePVMBG, Err: errors.New("bad payload")}
	}}
	go poller.runSource(ctx, source)

	select {
	case <-repository.advanced:
	case <-time.After(time.Second):
		t.Fatal("mapping errors must not block cursor advancement")
	}
	if _, err := repository.GetHazard(context.Background(), "h-2"); err != nil {
		t.Fatalf("successfully mapped event was not persisted: %v", err)
	}
	cancel()
}

func TestNewPollerValidatesConfig(t *testing.T) {
	repository := memory.NewRepository()
	source := &fakeSource{name: domain.SourceBMKG, fetch: func(context.Context, time.Time, string) ([]domain.HazardEvent, error) { return nil, nil }}

	if _, err := NewPoller(PollerConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty config error = %v, want ErrInvalidConfig", err)
	}
	if _, err := NewPoller(PollerConfig{Repository: repository, Sources: []Source{source}, Interval: time.Second, Timeout: 0}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("zero timeout error = %v, want ErrInvalidConfig", err)
	}
	poller, err := NewPoller(PollerConfig{Repository: repository, Sources: []Source{source}, Interval: time.Second, Timeout: time.Second})
	if err != nil {
		t.Fatalf("valid config failed: %v", err)
	}
	if poller == nil || len(poller.sources) != 1 {
		t.Fatalf("poller = %+v", poller)
	}
}

type atomicLeadership struct{ value atomic.Bool }

func (l *atomicLeadership) Allowed() bool { return l.value.Load() }

// staleLeadership looks healthy locally but fails the database-clock check,
// the state of a process that was frozen past its lease TTL.
type staleLeadership struct{}

func (staleLeadership) Allowed() bool { return true }

func (staleLeadership) StillLeader(context.Context) (bool, error) { return false, nil }

func TestRunSourceDoesNotFetchWithoutLeadership(t *testing.T) {
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1)}
	leadership := &atomicLeadership{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	poller.leadership = leadership
	poller.interval = 10 * time.Millisecond

	called := make(chan struct{}, 1)
	source := &fakeSource{name: domain.SourceBMKG, fetch: func(context.Context, time.Time, string) ([]domain.HazardEvent, error) {
		called <- struct{}{}
		return nil, nil
	}}
	go poller.runSource(ctx, source)

	select {
	case <-called:
		t.Fatal("source was fetched without leadership")
	case <-time.After(60 * time.Millisecond):
	}
}

func TestRunSourceDropsBatchWhenLeadershipIsLostDuringFetch(t *testing.T) {
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1)}
	leadership := &atomicLeadership{}
	leadership.value.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	poller.leadership = leadership
	poller.interval = 10 * time.Millisecond

	source := &fakeSource{name: domain.SourceBMKG, fetch: func(context.Context, time.Time, string) ([]domain.HazardEvent, error) {
		leadership.value.Store(false)
		return []domain.HazardEvent{testEvent("h-lost", domain.SourceBMKG)}, nil
	}}
	go poller.runSource(ctx, source)

	select {
	case <-repository.advanced:
		t.Fatal("cursor advanced after leadership was lost")
	case <-time.After(80 * time.Millisecond):
	}
	if _, err := repository.GetHazard(context.Background(), "h-lost"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("event persisted after leadership was lost: %v", err)
	}
}

func TestRunSourceDoesNotPersistWithStaleLease(t *testing.T) {
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	poller.leadership = staleLeadership{}
	poller.interval = 10 * time.Millisecond

	source := &fakeSource{name: domain.SourceBMKG, fetch: func(context.Context, time.Time, string) ([]domain.HazardEvent, error) {
		return []domain.HazardEvent{testEvent("h-stale", domain.SourceBMKG)}, nil
	}}
	go poller.runSource(ctx, source)

	select {
	case <-repository.advanced:
		t.Fatal("cursor advanced with a stale lease")
	case <-time.After(80 * time.Millisecond):
	}
	if _, err := repository.GetHazard(context.Background(), "h-stale"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("event persisted with a stale lease: %v", err)
	}
}

func TestRunSourceDoesNotFetchWithStaleLease(t *testing.T) {
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	poller.leadership = staleLeadership{}
	poller.interval = 10 * time.Millisecond

	called := make(chan struct{}, 1)
	source := &fakeSource{name: domain.SourceBMKG, fetch: func(context.Context, time.Time, string) ([]domain.HazardEvent, error) {
		select {
		case called <- struct{}{}:
		default:
		}
		return nil, nil
	}}
	go poller.runSource(ctx, source)

	select {
	case <-called:
		t.Fatal("upstream was called by an instance whose lease is no longer valid")
	case <-time.After(60 * time.Millisecond):
	}
}

// lostTermLeadership passed the database check, but its lease term was
// cleared before the write (the renew loop saw the lease lost in between).
type lostTermLeadership struct{}

func (lostTermLeadership) Allowed() bool { return true }

func (lostTermLeadership) StillLeader(context.Context) (bool, error) { return true, nil }

func (lostTermLeadership) Fence() (string, int64) { return "", 0 }

func TestRunSourceDoesNotWriteUnfencedWhenTermVanishesBeforeWrite(t *testing.T) {
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	poller.leadership = lostTermLeadership{}
	poller.leaseName = "ingestion"
	poller.interval = 10 * time.Millisecond

	source := &fakeSource{name: domain.SourceBMKG, fetch: func(context.Context, time.Time, string) ([]domain.HazardEvent, error) {
		return []domain.HazardEvent{testEvent("h-unfenced", domain.SourceBMKG)}, nil
	}}
	go poller.runSource(ctx, source)

	select {
	case <-repository.advanced:
		t.Fatal("cursor advanced without a lease term")
	case <-time.After(80 * time.Millisecond):
	}
	if _, err := repository.GetHazard(context.Background(), "h-unfenced"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("event persisted without a lease term: %v", err)
	}
}

func TestRunSourcePersistsRejectedPayloadForReplay(t *testing.T) {
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	raw := map[string]json.RawMessage{"report_id": json.RawMessage(`"r-1"`), "level": json.RawMessage(`"Awas"`)}
	source := &fakeSource{name: domain.SourcePVMBG, fetch: func(context.Context, time.Time, string) ([]domain.HazardEvent, error) {
		missing := fmt.Errorf("missing alert_level: %w", domain.ErrInvalidSourcePayload)
		return nil, &MappingError{Source: domain.SourcePVMBG, Err: missing, Rejected: []RejectedPayload{{Source: domain.SourcePVMBG, Raw: raw, Err: missing}}}
	}}
	go poller.runSource(ctx, source)

	select {
	case <-repository.advanced:
	case <-time.After(time.Second):
		t.Fatal("cursor did not advance after rejected payload")
	}
	failures := repository.recordedFailures()
	if len(failures) != 1 {
		t.Fatalf("failures = %d, want 1", len(failures))
	}
	if failures[0].Status != domain.FailureParked || failures[0].Retryable {
		t.Fatalf("failure = %+v", failures[0])
	}
}

func TestRecordFailuresMarksTransientAsRetrying(t *testing.T) {
	repository := memory.NewRepository()
	poller := testPoller(repository)
	transient := errors.New("temporary mapping failure")
	raw := map[string]json.RawMessage{"event_id": json.RawMessage(`"e-1"`)}
	poller.recordFailures(context.Background(), &MappingError{
		Source: domain.SourceBMKG,
		Err:    transient,
		Rejected: []RejectedPayload{{
			Source: domain.SourceBMKG,
			Raw:    raw,
			Err:    transient,
		}},
	}, "correlation-1")

	items, err := repository.ListIngestFailures(context.Background(), domain.FailureFilter{Source: domain.SourceBMKG, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("failures = %d, want 1", len(items))
	}
	if !items[0].Retryable || items[0].Status != domain.FailureRetrying {
		t.Fatalf("failure = %+v, want retrying", items[0])
	}
}

func TestMalformedPayloadKeepsRawBodyForParking(t *testing.T) {
	bad := &domain.MalformedPayloadError{Body: []byte(`{"broken"`), Err: errors.New("unexpected end")}
	mapping, ok := malformedPayload(domain.SourceBMKG, bad)
	if !ok {
		t.Fatal("malformed payload was not recognised")
	}
	if len(mapping.Rejected) != 1 || string(mapping.Rejected[0].Payload) != `{"broken"` {
		t.Fatalf("rejected = %+v", mapping.Rejected)
	}
}

func TestRunSourceParksUndecodableResponseWithoutAdvancingCursor(t *testing.T) {
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1), failed: make(chan struct{}, 1)}
	starting := time.Now().UTC().Add(-time.Hour)
	if err := repository.Repository.AdvancePollCursor(context.Background(), domain.SourcePVMBG, starting); err != nil {
		t.Fatal(err)
	}
	called := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	source := &fakeSource{name: domain.SourcePVMBG, fetch: func(context.Context, time.Time, string) ([]domain.HazardEvent, error) {
		bad := &domain.MalformedPayloadError{Body: []byte(`[{"report_id":`), Err: errors.New("invalid_json: unexpected end of JSON input")}
		mapping, _ := malformedPayload(domain.SourcePVMBG, bad)
		called <- struct{}{}
		return nil, mapping
	}}
	go poller.runSource(ctx, source)

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("poll did not start")
	}
	select {
	case <-repository.failed:
	case <-time.After(time.Second):
		t.Fatal("undecodable response was not parked")
	}
	select {
	case advanced := <-repository.advanced:
		t.Fatalf("undecodable response advanced cursor to %s", advanced)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	failures := repository.recordedFailures()
	if len(failures) != 1 {
		t.Fatalf("failures = %d, want 1", len(failures))
	}
	failure := failures[0]
	if failure.Retryable || failure.Status != domain.FailureParked || string(failure.Payload) != `[{"report_id":` {
		t.Fatalf("failure = %+v", failure)
	}
	if !strings.Contains(failure.Reason, "invalid_json") {
		t.Fatalf("reason %q lost the decode detail", failure.Reason)
	}
}

type stubClient struct {
	records []map[string]json.RawMessage
	err     error
}

func (c stubClient) Get(context.Context, string, string) ([]map[string]json.RawMessage, error) {
	return c.records, c.err
}

// One persistently corrupt record must not block the window (P3.3): the valid
// record is stored, the bad bytes are parked, and the cursor moves on.
func TestRunSourceParksBadRecordAndAdvancesPastIt(t *testing.T) {
	good := map[string]json.RawMessage{
		"report_id":           json.RawMessage(`"pvmbg-ok"`),
		"volcano_id":          json.RawMessage(`"merapi"`),
		"alert_level":         json.RawMessage(`"Siaga"`),
		"eruption_count_24h":  json.RawMessage(`3`),
		"ash_column_height_m": json.RawMessage(`1200`),
		"reported_at":         json.RawMessage(`"2026-11-15T01:00:00Z"`),
	}
	badBody := []byte("{\"report_id\":\"pvmbg-bad\",\"volcano_id\":\"merapi\xff\"}")
	source, err := NewPVMBGSource(PVMBGSourceConfig{
		Client:    stubClient{records: []map[string]json.RawMessage{good}, err: &domain.MalformedRecordsError{Records: []domain.MalformedRecord{{Body: badBody, Err: errors.New("invalid_utf8: record is not valid UTF-8")}}}},
		Volcanoes: map[string]domain.VolcanoReference{"merapi": {Name: "Merapi", Latitude: -7.54, Longitude: 110.44}},
	})
	if err != nil {
		t.Fatal(err)
	}
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go testPoller(repository).runSource(ctx, source)

	select {
	case <-repository.advanced:
	case <-time.After(time.Second):
		t.Fatal("cursor did not advance past the bad record")
	}
	cancel()
	if _, err := repository.Repository.GetHazard(context.Background(), uuid.NewSHA1(uuid.NameSpaceURL, []byte("PVMBG:pvmbg-ok")).String()); err != nil {
		t.Fatalf("valid record not stored: %v", err)
	}
	failures := repository.recordedFailures()
	if len(failures) != 1 {
		t.Fatalf("failures = %d, want 1", len(failures))
	}
	failure := failures[0]
	if failure.Retryable || failure.Status != domain.FailureParked || string(failure.Payload) != string(badBody) || !strings.Contains(failure.Reason, "invalid_utf8") {
		t.Fatalf("failure = %+v", failure)
	}
}
