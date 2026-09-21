package state

import (
	"strconv"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/bmkg/internal/domain"
	"github.com/fathurwithyou/microservices/services/bmkg/internal/seed"
)

type Memory struct {
	mu       sync.RWMutex
	events   []domain.SeismicEvent
	warnings []domain.TsunamiWarning
}

func New(count int) *Memory {
	events, warnings := seed.InitialData(time.Now().UTC(), count)
	return &Memory{events: events, warnings: warnings}
}

func (s *Memory) AppendEvent() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	s.events = append(s.events, domain.SeismicEvent{
		EventID: "bmkg-seismic-" + strconv.Itoa(len(s.events)+1), Magnitude: 5.1, DepthKM: 12,
		EpicenterLat: -7.1, EpicenterLon: 107.6, RegionName: "Jawa Barat",
		OccurredAt: now.Format(time.RFC3339), PotentialTsunami: true,
	})
}

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
