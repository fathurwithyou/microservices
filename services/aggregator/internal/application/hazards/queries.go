package hazards

import (
	"context"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/ports"
)

type Queries struct {
	store ports.Store
}

func NewQueries(store ports.Store) *Queries {
	return &Queries{store: store}
}

func (q *Queries) List(ctx context.Context, filter ports.ListFilter) ([]domain.HazardEvent, error) {
	return q.store.ListHazards(ctx, filter)
}

func (q *Queries) Get(ctx context.Context, hazardID string) (domain.HazardEvent, error) {
	return q.store.GetHazard(ctx, hazardID)
}

func (q *Queries) SourceStatus(ctx context.Context, source domain.Source) (domain.SourceStatus, error) {
	return q.store.GetSourceStatus(ctx, source)
}
