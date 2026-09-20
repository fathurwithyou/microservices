package ports

import (
	"context"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
)

type Aggregator interface {
	List(context.Context, *aggregatorpb.ListHazardsRequest) (*aggregatorpb.ListHazardsResponse, error)
	GetHazard(context.Context, *aggregatorpb.GetHazardRequest) (*eventspb.HazardEvent, error)
	Status(context.Context, *aggregatorpb.GetSourceStatusRequest) (*eventspb.SourceStatus, error)
}
