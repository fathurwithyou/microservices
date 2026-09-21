package hazards

import (
	"context"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type Queries struct {
	repo Repository
}

func NewQueries(repo Repository) *Queries {
	return &Queries{repo: repo}
}

func (q *Queries) List(ctx context.Context, filter ListFilter) ([]domain.HazardEvent, error) {
	return q.repo.ListHazards(ctx, filter)
}

func (q *Queries) Get(ctx context.Context, hazardID string) (domain.HazardEvent, error) {
	return q.repo.GetHazard(ctx, hazardID)
}

func (q *Queries) SourceStatus(ctx context.Context, source domain.Source) (domain.SourceStatus, error) {
	return q.repo.GetSourceStatus(ctx, source)
}
