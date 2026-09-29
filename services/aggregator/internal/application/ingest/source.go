package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

// Source is the contract every external hazard feed must satisfy. A Source
// owns exactly two concerns: which endpoints to call and how to map raw
// payloads into domain events. Scheduling, cursors, source status,
// persistence, and outbox enqueueing are owned by the Poller, so adding a
// new feed means implementing this interface and wiring it in main.
type Source interface {
	// Name identifies the feed for poll cursors, source status, and logs.
	Name() domain.Source
	// Fetch retrieves payloads observed since the given cursor and maps
	// them to domain events. Transport failures are returned as plain
	// errors; payloads that fail mapping are skipped (and reported through
	// a *MappingError) while the successfully mapped events are still
	// returned.
	Fetch(ctx context.Context, since time.Time, correlationID string) ([]domain.HazardEvent, error)
}

// MappingError reports source payloads that could not be mapped to domain
// events. It is returned together with the events that did map, allowing
// the poller to advance past poison payloads instead of retrying them
// forever. Rejected payloads are logged by the Source that produced them.
type MappingError struct {
	Source domain.Source
	Err    error
}

func (e *MappingError) Error() string {
	return fmt.Sprintf("map %s payloads: %v", e.Source, e.Err)
}

func (e *MappingError) Unwrap() error { return e.Err }
