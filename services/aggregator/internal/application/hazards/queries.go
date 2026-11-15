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

// List returns one page ordered by occurred_at desc, hazard_id desc, and the
// token for the next page. The token is empty once a page comes back short.
// A full last page yields one more, empty, page.
func (q *Queries) List(ctx context.Context, filter ListFilter) ([]domain.HazardEvent, string, error) {
	if _, _, err := DecodePageToken(filter.PageToken); err != nil {
		return nil, "", err
	}
	filter.PageSize = NormalizePageSize(filter.PageSize)
	items, err := q.repo.ListHazards(ctx, filter)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(items) == filter.PageSize {
		next = EncodePageToken(items[len(items)-1])
	}
	return items, next, nil
}

func (q *Queries) Get(ctx context.Context, hazardID string) (domain.HazardEvent, error) {
	return q.repo.GetHazard(ctx, hazardID)
}

func (q *Queries) SourceStatus(ctx context.Context, source domain.Source) (domain.SourceStatus, error) {
	return q.repo.GetSourceStatus(ctx, source)
}
