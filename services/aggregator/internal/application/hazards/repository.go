package hazards

import (
	"context"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type Repository interface {
	ListHazards(context.Context, ListFilter) ([]domain.HazardEvent, error)
	GetHazard(context.Context, string) (domain.HazardEvent, error)
	GetSourceStatus(context.Context, domain.Source) (domain.SourceStatus, error)
}

type ListFilter struct {
	Source     domain.Source
	HazardType domain.HazardType
	Severity   domain.Severity
	PageSize   int
	PageToken  string
}
