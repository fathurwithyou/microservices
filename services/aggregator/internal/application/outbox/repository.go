package outbox

import (
	"context"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type Repository interface {
	ClaimPendingOutbox(context.Context, int, time.Duration) ([]domain.OutboxMessage, error)
	MarkOutboxPublished(context.Context, int64, string) error
	ReleaseOutbox(context.Context, int64, string, time.Time) error
}
