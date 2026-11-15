package grpctransport

import (
	"context"
	"time"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/analytics"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (h *queryHandler) GetAnalytics(ctx context.Context, request *aggregatorpb.AnalyticsRequest) (*aggregatorpb.AnalyticsResponse, error) {
	if h.analytics == nil {
		return nil, status.Error(codes.Unavailable, "analytics is not configured")
	}
	since, err := optionalTime(request.GetSince())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "since must be RFC3339")
	}
	until, err := optionalTime(request.GetUntil())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "until must be RFC3339")
	}
	result, err := h.analytics.Get(ctx, analytics.Filter{Since: since, Until: until, Bucket: analytics.Bucket(request.GetBucket())})
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	response := &aggregatorpb.AnalyticsResponse{Partial: result.Partial, UnavailableUnits: result.UnavailableUnits}
	for _, item := range result.HazardTypes {
		response.HazardTypes = append(response.HazardTypes, &aggregatorpb.HazardTypeCount{HazardType: hazardTypeToProto(canonicalType(item.HazardType)), Count: item.Count, Category: string(item.HazardType)})
	}
	for _, item := range result.Severities {
		response.Severities = append(response.Severities, &aggregatorpb.SeverityCount{Severity: severityToProto(item.Severity), Count: item.Count})
	}
	for _, item := range result.Areas {
		response.Areas = append(response.Areas, &aggregatorpb.AreaCount{AreaName: item.AreaName, Count: item.Count})
	}
	for _, item := range result.Buckets {
		response.TimeBuckets = append(response.TimeBuckets, &aggregatorpb.TimeBucket{BucketStart: item.Start.UTC().Format(time.RFC3339), HazardType: hazardTypeToProto(canonicalType(item.HazardType)), Count: item.Count, Category: string(item.HazardType)})
	}
	return response, nil
}

func optionalTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, value)
}

// canonicalType maps a dashboard category back to its HazardEvent type: the
// TSUNAMI category is made of SEISMIC events.
func canonicalType(category domain.HazardType) domain.HazardType {
	if category == analytics.CategoryTsunami {
		return domain.HazardSeismic
	}
	return category
}
