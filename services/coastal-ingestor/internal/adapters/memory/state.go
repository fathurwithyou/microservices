// Package memory provides an in-memory State adapter for tests and local
// fixture runs.
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
)

// State keeps quarantine records and the last accepted batch in memory.
type State struct {
	mu         sync.Mutex
	quarantine []domain.QuarantineRecord
	lastGood   []domain.Reading
	capturedAt time.Time
}

// New returns an empty in-memory State.
func New() *State {
	return &State{}
}

// SaveQuarantine appends records.
func (s *State) SaveQuarantine(_ context.Context, records []domain.QuarantineRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quarantine = append(s.quarantine, records...)
	return nil
}

// SaveLastGood replaces the cached accepted batch.
func (s *State) SaveLastGood(_ context.Context, readings []domain.Reading, capturedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastGood = append([]domain.Reading(nil), readings...)
	s.capturedAt = capturedAt
	return nil
}

// LastGood returns a copy of the cached accepted batch.
func (s *State) LastGood(_ context.Context) ([]domain.Reading, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.Reading(nil), s.lastGood...), s.capturedAt, nil
}

// Quarantined returns a copy of the stored quarantine records.
func (s *State) Quarantined() []domain.QuarantineRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.QuarantineRecord(nil), s.quarantine...)
}
