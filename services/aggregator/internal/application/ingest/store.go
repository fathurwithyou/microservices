package ingest

import (
	"context"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type Store interface {
	PersistHazardAndEnqueue(context.Context, domain.HazardEvent, domain.OutboxMessage) error
	SetSourceStatus(context.Context, domain.SourceStatus) error
	GetSourceStatus(context.Context, domain.Source) (domain.SourceStatus, error)
	GetPollCursor(context.Context, domain.Source) (time.Time, error)
	AdvancePollCursor(context.Context, domain.Source, time.Time) error
}
