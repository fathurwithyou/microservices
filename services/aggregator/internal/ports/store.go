package ports

import (
	"context"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type Store interface {
	PersistHazardAndEnqueue(context.Context, domain.HazardEvent, domain.OutboxMessage) error
	GetHazard(context.Context, string) (domain.HazardEvent, error)
	ListHazards(context.Context, ListFilter) ([]domain.HazardEvent, error)
	SetSourceStatus(context.Context, domain.SourceStatus) error
	GetSourceStatus(context.Context, domain.Source) (domain.SourceStatus, error)
	GetPollCursor(context.Context, domain.Source) (time.Time, error)
	AdvancePollCursor(context.Context, domain.Source, time.Time) error
	LoadVolcanoReferences(context.Context) (map[string]domain.VolcanoReference, error)
	ClaimPendingOutbox(context.Context, int, time.Duration) ([]domain.OutboxMessage, error)
	MarkOutboxPublished(context.Context, int64, string) error
	ReleaseOutbox(context.Context, int64, string, time.Time) error
}

type ListFilter struct {
	Source     domain.Source
	HazardType domain.HazardType
	Severity   domain.Severity
	PageSize   int
	PageToken  string
}
