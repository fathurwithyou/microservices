// Package memory provides an in-memory State adapter for unit tests and for
// running without a database.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/ports"
)

// State is a concurrency-safe in-memory implementation of ports.State.
type State struct {
	mu         sync.Mutex
	offset     int64
	session    string
	watermark  time.Time
	detections map[string]domain.Detection
	fires      map[string]domain.Fire
	late       []ports.LateDetection
	counters   map[string]int64
	// FailCommit, when set, makes Commit fail; tests use it to model a
	// crash between the send and the commit.
	FailCommit func(ports.Batch) error
}

// New returns an empty in-memory state.
func New() *State {
	return &State{detections: map[string]domain.Detection{}, fires: map[string]domain.Fire{}, counters: map[string]int64{}}
}

func (s *State) Load(context.Context) (ports.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := ports.Snapshot{Offset: s.offset, Session: s.session, Watermark: s.watermark, Counters: map[string]int64{}}
	for _, detection := range s.detections {
		snapshot.Detections = append(snapshot.Detections, detection)
	}
	sort.Slice(snapshot.Detections, func(i, j int) bool { return snapshot.Detections[i].Key() < snapshot.Detections[j].Key() })
	for _, fire := range s.fires {
		if fire.Status == domain.StatusActive {
			snapshot.Fires = append(snapshot.Fires, fire)
		}
	}
	sort.Slice(snapshot.Fires, func(i, j int) bool { return snapshot.Fires[i].SourceRefID < snapshot.Fires[j].SourceRefID })
	for name, value := range s.counters {
		snapshot.Counters[name] = value
	}
	return snapshot, nil
}

func (s *State) Commit(_ context.Context, batch ports.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.FailCommit != nil {
		if err := s.FailCommit(batch); err != nil {
			return err
		}
	}
	for _, detection := range batch.UpsertDetections {
		s.detections[detection.Key()] = detection
	}
	for _, key := range batch.DeleteDetections {
		delete(s.detections, key)
	}
	for _, fire := range batch.Fires {
		s.fires[fire.SourceRefID] = fire
	}
	s.late = append(s.late, batch.Late...)
	for name, value := range batch.Counters {
		s.counters[name] = value
	}
	if !batch.Watermark.IsZero() {
		s.watermark = batch.Watermark
	}
	s.offset = batch.Offset
	return nil
}

func (s *State) Reset(_ context.Context, session string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offset, s.session, s.watermark = 0, session, time.Time{}
	s.detections = map[string]domain.Detection{}
	s.fires = map[string]domain.Fire{}
	s.late = nil
	s.counters = map[string]int64{}
	return nil
}

// Fires returns every stored fire, whatever its status.
func (s *State) Fires() []domain.Fire {
	s.mu.Lock()
	defer s.mu.Unlock()
	fires := make([]domain.Fire, 0, len(s.fires))
	for _, fire := range s.fires {
		fires = append(fires, fire)
	}
	sort.Slice(fires, func(i, j int) bool { return fires[i].SourceRefID < fires[j].SourceRefID })
	return fires
}

// Late returns the recorded late detections.
func (s *State) Late() []ports.LateDetection {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ports.LateDetection(nil), s.late...)
}
