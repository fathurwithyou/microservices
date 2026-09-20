package ports

import (
	"context"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type Publisher interface {
	Publish(context.Context, domain.OutboxMessage) error
	Close() error
}
