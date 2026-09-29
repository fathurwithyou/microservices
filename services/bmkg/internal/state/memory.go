// Package state keeps the in-memory simulation state for the BMKG service.
package state

import (
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/bmkg/internal/domain"
	"github.com/fathurwithyou/microservices/services/bmkg/internal/seed"
	"github.com/google/uuid"
)

// Runtime event values appended by the producer. Kept as named constants so
// the hard-coded simulation payload is documented in one place; the values
// are identical to the previous inline literals.
const (
	runtimeMagnitude    = 5.1
	runtimeDepthKM      = 12.0
	runtimeEpicenterLat = -7.1
	runtimeEpicenterLon = 107.6
	runtimeRegionName   = "Jawa Barat"
	// runtimeTsunamiPotential: every runtime event simulates a
	// tsunami-generating earthquake.
	runtimeTsunamiPotential = true
)

// Config configures a Memory store.
type Config struct {
	// SeedCount is the number of historical events seeded at startup.
	SeedCount int
	// Now returns the current time. Defaults to time.Now().UTC() so tests
	// can inject a deterministic clock.
	Now func() time.Time
}

// Memory is the in-memory event and warning store.
type Memory struct {
	mu       sync.RWMutex
	now      func() time.Time
	events   []domain.SeismicEvent
	warnings []domain.TsunamiWarning
}

// New creates a Memory store seeded with cfg.SeedCount historical records.
func New(cfg Config) *Memory {
	now := cfg.Now
	if now == nil {
		now = utcNow
	}
	events, warnings := seed.InitialData(now(), cfg.SeedCount)
	return &Memory{now: now, events: events, warnings: warnings}
}

// utcNow is the default clock: current wall time in UTC.
func utcNow() time.Time { return time.Now().UTC() }

// newRuntimeEvent builds the hard-coded event the producer appends on each
// tick, stamped with the given time and a unique ID.
func newRuntimeEvent(occurredAt time.Time) domain.SeismicEvent {
	return domain.SeismicEvent{
		EventID:          "bmkg-seismic-" + uuid.NewString(),
		Magnitude:        runtimeMagnitude,
		DepthKM:          runtimeDepthKM,
		EpicenterLat:     runtimeEpicenterLat,
		EpicenterLon:     runtimeEpicenterLon,
		RegionName:       runtimeRegionName,
		OccurredAt:       occurredAt.Format(time.RFC3339),
		PotentialTsunami: runtimeTsunamiPotential,
	}
}

// AppendEvent appends the runtime simulation event using the injected clock.
func (s *Memory) AppendEvent() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = append(s.events, newRuntimeEvent(s.now()))
}

// EventsSince returns all events that occurred at or after since.
func (s *Memory) EventsSince(since time.Time) []domain.SeismicEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]domain.SeismicEvent, 0)
	for _, event := range s.events {
		occurredAt, err := time.Parse(time.RFC3339, event.OccurredAt)
		if err == nil && !occurredAt.Before(since) {
			result = append(result, event)
		}
	}
	return result
}

// WarningsSince returns all warnings with an estimated arrival at or after
// since.
func (s *Memory) WarningsSince(since time.Time) []domain.TsunamiWarning {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]domain.TsunamiWarning, 0)
	for _, warning := range s.warnings {
		estimatedArrival, err := time.Parse(time.RFC3339, warning.EstimatedArrival)
		if err == nil && !estimatedArrival.Before(since) {
			result = append(result, warning)
		}
	}
	return result
}
