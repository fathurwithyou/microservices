package grpctransport

import (
	"context"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/hazards"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type queryHandler struct {
	aggregatorpb.UnimplementedAggregatorServer
	queries *hazards.Queries
}

func newQueryHandler(queries *hazards.Queries) *queryHandler {
	return &queryHandler{queries: queries}
}

func (h *queryHandler) ListHazards(ctx context.Context, request *aggregatorpb.ListHazardsRequest) (*aggregatorpb.ListHazardsResponse, error) {
	items, err := h.queries.List(ctx, hazards.ListFilter{Source: sourceFromProto(request.GetSource()), HazardType: hazardTypeFromProto(request.GetHazardType()), Severity: severityFromProto(request.GetSeverity()), PageSize: int(request.GetPageSize()), PageToken: request.GetPageToken()})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	response := &aggregatorpb.ListHazardsResponse{Hazards: make([]*eventspb.HazardEvent, 0, len(items))}
	for _, item := range items {
		response.Hazards = append(response.Hazards, hazardToProto(item))
	}
	return response, nil
}

func (h *queryHandler) GetHazard(ctx context.Context, request *aggregatorpb.GetHazardRequest) (*eventspb.HazardEvent, error) {
	item, err := h.queries.Get(ctx, request.GetHazardId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return hazardToProto(item), nil
}

func (h *queryHandler) GetSourceStatus(ctx context.Context, request *aggregatorpb.GetSourceStatusRequest) (*eventspb.SourceStatus, error) {
	item, err := h.queries.SourceStatus(ctx, sourceFromProto(request.GetSource()))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return sourceStatusToProto(item, request.GetSource()), nil
}

func (h *queryHandler) HealthCheck(context.Context, *aggregatorpb.HealthCheckRequest) (*aggregatorpb.HealthCheckResponse, error) {
	return &aggregatorpb.HealthCheckResponse{Status: "ok"}, nil
}
