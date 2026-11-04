// Package memory provides an in-memory State adapter for unit tests.
package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

// State is a concurrency-safe in-memory implementation of ports.State.
type State struct {
	mu         sync.Mutex
	offset     int64
	detections map[string]domain.Detection
	fires      map[string]domain.Fire
}

// New returns an empty in-memory state.
func New() *State {
	return &State{
		detections: map[string]domain.Detection{},
		fires:      map[string]domain.Fire{},
	}
}

func (s *State) Offset(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offset, nil
}

func (s *State) SaveOffset(_ context.Context, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offset = offset
	return nil
}

func (s *State) Detections(context.Context) ([]domain.Detection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	detections := make([]domain.Detection, 0, len(s.detections))
	for _, detection := range s.detections {
		detections = append(detections, detection)
	}
	return detections, nil
}

func (s *State) SaveDetections(_ context.Context, detections []domain.Detection) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.detections = make(map[string]domain.Detection, len(detections))
	for _, detection := range detections {
		s.detections[detection.Key()] = detection
	}
	return nil
}

func (s *State) Fires(context.Context) ([]domain.Fire, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fires := make([]domain.Fire, 0, len(s.fires))
	for _, fire := range s.fires {
		fires = append(fires, fire)
	}
	sort.Slice(fires, func(i, j int) bool { return fires[i].SourceRefID < fires[j].SourceRefID })
	return fires, nil
}

func (s *State) SaveFires(_ context.Context, fires []domain.Fire) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fires = make(map[string]domain.Fire, len(fires))
	for _, fire := range fires {
		s.fires[fire.SourceRefID] = fire
	}
	return nil
}
