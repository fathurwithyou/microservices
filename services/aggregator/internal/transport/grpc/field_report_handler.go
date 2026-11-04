package grpctransport

import (
	"context"
	"errors"
	"time"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/fieldreports"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (h *queryHandler) SubmitFieldReport(ctx context.Context, request *aggregatorpb.SubmitFieldReportRequest) (*aggregatorpb.SubmitFieldReportResponse, error) {
	if h.fieldReports == nil {
		return nil, status.Error(codes.Unavailable, "field reports are not configured")
	}
	occurredAt, err := time.Parse(time.RFC3339, request.GetOccurredAt())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "occurred_at must be RFC3339")
	}
	report, err := h.fieldReports.Submit(ctx, domain.FieldReport{
		ReportID:    request.GetReportId(),
		HazardType:  hazardTypeFromProto(request.GetHazardType()),
		Severity:    severityFromProto(request.GetSeverity()),
		AreaName:    request.GetAreaName(),
		Latitude:    request.GetLatitude(),
		Longitude:   request.GetLongitude(),
		OccurredAt:  occurredAt,
		Description: request.GetDescription(),
	})
	if err != nil {
		if errors.Is(err, fieldreports.ErrInvalidReport) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &aggregatorpb.SubmitFieldReportResponse{ReportId: report.ReportID, Status: string(report.Status)}, nil
}
