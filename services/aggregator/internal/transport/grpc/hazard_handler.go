package grpctransport

import (
	"context"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (h *queryHandler) SubmitHazard(ctx context.Context, request *aggregatorpb.SubmitHazardRequest) (*aggregatorpb.SubmitHazardResponse, error) {
	if h.submitter == nil {
		return nil, status.Error(codes.Unavailable, "hazard submission is not configured")
	}
	event, err := hazardFromProto(request.GetEvent())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "event timestamps must be RFC3339")
	}
	id, err := h.submitter.Submit(ctx, event, request.GetCorrelationId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &aggregatorpb.SubmitHazardResponse{HazardId: id}, nil
}
