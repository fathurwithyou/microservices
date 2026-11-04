package ports

import (
	"context"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
)

// State persists rejected readings and the most recently accepted batch, so a
// restart or an outage can still serve the last known good data.
type State interface {
	SaveQuarantine(ctx context.Context, records []domain.QuarantineRecord) error
	SaveLastGood(ctx context.Context, readings []domain.Reading, capturedAt time.Time) error
	// LastGood returns the most recently persisted accepted batch and when it
	// was captured. Both are zero when nothing has been stored yet.
	LastGood(ctx context.Context) ([]domain.Reading, time.Time, error)
}
