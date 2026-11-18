package grpctransport

import (
	"context"
	"errors"
	"time"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/analytics"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/degraded"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/fieldreports"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/hazards"
	applicationingest "github.com/fathurwithyou/microservices/services/aggregator/internal/application/ingest"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type queryHandler struct {
	aggregatorpb.UnimplementedAggregatorServer
	queries      *hazards.Queries
	fieldReports *fieldreports.Service
	analytics    *analytics.Queries
	submitter    *applicationingest.Submitter
	health       interface {
		UnavailableUnits(context.Context) ([]string, error)
	}
}

func newQueryHandler(queries *hazards.Queries, reports *fieldreports.Service, metrics *analytics.Queries, submitter *applicationingest.Submitter, health interface {
	UnavailableUnits(context.Context) ([]string, error)
}) *queryHandler {
	return &queryHandler{queries: queries, fieldReports: reports, analytics: metrics, submitter: submitter, health: health}
}

func (h *queryHandler) ListHazards(ctx context.Context, request *aggregatorpb.ListHazardsRequest) (*aggregatorpb.ListHazardsResponse, error) {
	items, next, stale, err := h.queries.List(ctx, hazards.ListFilter{Source: sourceFromProto(request.GetSource()), HazardType: hazardTypeFromProto(request.GetHazardType()), Severity: severityFromProto(request.GetSeverity()), PageSize: int(request.GetPageSize()), PageToken: request.GetPageToken()})
	if errors.Is(err, hazards.ErrInvalidPageToken) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err != nil {
		return nil, storeStatus(ctx, err)
	}
	response := &aggregatorpb.ListHazardsResponse{Hazards: make([]*eventspb.HazardEvent, 0, len(items)), NextPageToken: next}
	for _, item := range items {
		response.Hazards = append(response.Hazards, hazardToProto(item))
	}
	var lookup func(context.Context) ([]string, error)
	if h.health != nil {
		lookup = h.health.UnavailableUnits
	}
	response.Partial, response.UnavailableUnits, stale = degraded.Completeness(ctx, lookup, stale)
	markDegraded(ctx, "hazards", stale)
	return response, nil
}

func (h *queryHandler) GetHazard(ctx context.Context, request *aggregatorpb.GetHazardRequest) (*eventspb.HazardEvent, error) {
	item, err := h.queries.Get(ctx, request.GetHazardId())
	if errors.Is(err, domain.ErrNotFound) {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if err != nil {
		// Until 18 Nov every error was NotFound, so a store outage told
		// clients that existing hazards did not exist.
		return nil, storeStatus(ctx, err)
	}
	return hazardToProto(item), nil
}

func (h *queryHandler) GetSourceStatus(ctx context.Context, request *aggregatorpb.GetSourceStatusRequest) (*eventspb.SourceStatus, error) {
	item, err := h.queries.SourceStatus(ctx, sourceFromProto(request.GetSource()))
	if err != nil {
		return nil, storeStatus(ctx, err)
	}
	return sourceStatusToProto(item, request.GetSource()), nil
}

func (h *queryHandler) HealthCheck(context.Context, *aggregatorpb.HealthCheckRequest) (*aggregatorpb.HealthCheckResponse, error) {
	return &aggregatorpb.HealthCheckResponse{Status: "ok"}, nil
}

// staleSinceHeader carries, on a degraded answer, the time the data was read
// from the store (RFC3339). It is response metadata rather than a message
// field so that telling the client the age of a stale answer needs no
// contract change; the Client API turns it into the JSON field stale_since.
const staleSinceHeader = "x-stale-since"

// storeStatus maps the failure of a read that could not be answered, not
// even from an earlier answer. The read handlers reject bad input before
// they query, so what is left is the caller's own deadline or the store.
// The store is reported as Unavailable, not Internal: the Client API retries
// Unavailable on another instance (which may still hold an answer) and
// otherwise answers 503, where Internal became 502 (18 Nov).
func storeStatus(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}

// markDegraded counts and logs a degraded answer of endpoint and tells the
// caller how old its data is. A live answer is left untouched.
func markDegraded(ctx context.Context, endpoint string, stale degraded.Info) {
	if !stale.Degraded() {
		return
	}
	degraded.Record(endpoint, stale)
	if !stale.Since.IsZero() {
		// Best effort: the marker itself (partial) is in the message.
		_ = grpc.SetHeader(ctx, metadata.Pairs(staleSinceHeader, stale.Since.UTC().Format(time.RFC3339)))
	}
}
