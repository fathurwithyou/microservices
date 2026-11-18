package hazards

import (
	"context"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/degraded"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type Queries struct {
	repo Repository
	list *listCache
}

func NewQueries(repo Repository) *Queries {
	return &Queries{repo: repo}
}

// WithListCache makes identical concurrent listings share one store query and
// reuses its answer for ttl (see listCache). A ttl <= 0 disables it.
func (q *Queries) WithListCache(ttl time.Duration) *Queries {
	if ttl > 0 {
		q.list = newListCache(ttl)
	}
	return q
}

// List returns one page ordered by occurred_at desc, hazard_id desc, and the
// token for the next page. The token is empty once a page comes back short.
// A full last page yields one more, empty, page.
//
// When the store does not answer and this exact listing was answered before,
// the last page is returned with a degraded.Info saying since when (see
// listCache); the error is returned only for a listing never answered.
func (q *Queries) List(ctx context.Context, filter ListFilter) ([]domain.HazardEvent, string, degraded.Info, error) {
	if _, _, err := DecodePageToken(filter.PageToken); err != nil {
		return nil, "", degraded.Info{}, err
	}
	filter.PageSize = NormalizePageSize(filter.PageSize)
	if q.list != nil {
		return q.list.get(ctx, filter, func(ctx context.Context) ([]domain.HazardEvent, string, error) {
			return q.listFromStore(ctx, filter)
		})
	}
	items, next, err := q.listFromStore(ctx, filter)
	return items, next, degraded.Info{}, err
}

func (q *Queries) listFromStore(ctx context.Context, filter ListFilter) ([]domain.HazardEvent, string, error) {
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
