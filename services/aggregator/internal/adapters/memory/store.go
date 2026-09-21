package memory

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/hazards"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/google/uuid"
)

type Store struct {
	mu          sync.RWMutex
	events      map[string]domain.HazardEvent
	bySource    map[string]string
	status      map[domain.Source]domain.SourceStatus
	outbox      map[int64]domain.OutboxMessage
	cursors     map[domain.Source]time.Time
	volcanoes   map[string]domain.VolcanoReference
	availableAt map[int64]time.Time
	leaseUntil  map[int64]time.Time
	nextID      int64
}

func New() *Store {
	return &Store{
		events:      make(map[string]domain.HazardEvent),
		bySource:    make(map[string]string),
		status:      make(map[domain.Source]domain.SourceStatus),
		outbox:      make(map[int64]domain.OutboxMessage),
		cursors:     make(map[domain.Source]time.Time),
		volcanoes:   defaultVolcanoes(),
		availableAt: make(map[int64]time.Time),
		leaseUntil:  make(map[int64]time.Time),
	}
}

func (s *Store) PersistHazardAndEnqueue(_ context.Context, event domain.HazardEvent, message domain.OutboxMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	event = clone(event)
	key := string(event.Source) + ":" + event.SourceRefID
	if existingID, ok := s.bySource[key]; ok {
		if unchanged(s.events[existingID], event) {
			return nil
		}
		event.HazardID = existingID
		s.events[existingID] = event
	} else {
		s.bySource[key] = event.HazardID
		s.events[event.HazardID] = event
	}
	s.nextID++
	message.HazardID = event.HazardID
	message.ID = s.nextID
	message.Payload = append([]byte(nil), message.Payload...)
	s.outbox[message.ID] = message
	s.availableAt[message.ID] = time.Now().UTC()
	return nil
}

func unchanged(existing, incoming domain.HazardEvent) bool {
	return existing.Severity == incoming.Severity && reflect.DeepEqual(existing.Attributes, incoming.Attributes)
}

func (s *Store) GetHazard(_ context.Context, id string) (domain.HazardEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	event, ok := s.events[id]
	if !ok {
		return domain.HazardEvent{}, domain.ErrNotFound
	}
	return clone(event), nil
}

func (s *Store) ListHazards(_ context.Context, filter hazards.ListFilter) ([]domain.HazardEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.HazardEvent, 0, len(s.events))
	for _, event := range s.events {
		if filter.Source != "" && event.Source != filter.Source || filter.HazardType != "" && event.HazardType != filter.HazardType || filter.Severity != "" && event.Severity != filter.Severity {
			continue
		}
		items = append(items, clone(event))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].OccurredAt.After(items[j].OccurredAt) })
	start := 0
	if filter.PageToken != "" {
		for i := range items {
			if items[i].HazardID == filter.PageToken {
				start = i + 1
				break
			}
		}
	}
	end := len(items)
	if filter.PageSize > 0 && start+filter.PageSize < end {
		end = start + filter.PageSize
	}
	if start > len(items) {
		start = len(items)
	}
	return items[start:end], nil
}

func (s *Store) SetSourceStatus(_ context.Context, status domain.SourceStatus) error {
	s.mu.Lock()
	s.status[status.Source] = status
	s.mu.Unlock()
	return nil
}

func (s *Store) GetSourceStatus(_ context.Context, source domain.Source) (domain.SourceStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	status, ok := s.status[source]
	if !ok {
		return domain.SourceStatus{Source: source, Status: "unknown"}, nil
	}
	return status, nil
}

func (s *Store) GetPollCursor(_ context.Context, source domain.Source) (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cursors[source], nil
}

func (s *Store) AdvancePollCursor(_ context.Context, source domain.Source, cursor time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cursor.After(s.cursors[source]) {
		s.cursors[source] = cursor
	}
	return nil
}

func (s *Store) LoadVolcanoReferences(_ context.Context) (map[string]domain.VolcanoReference, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneVolcanoes(s.volcanoes), nil
}

func (s *Store) ClaimPendingOutbox(_ context.Context, limit int, leaseDuration time.Duration) ([]domain.OutboxMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	leaseToken := uuid.NewString()
	items := make([]domain.OutboxMessage, 0, limit)
	for id := int64(1); id <= s.nextID && len(items) < limit; id++ {
		message, ok := s.outbox[id]
		if !ok || s.availableAt[id].After(now) || s.leaseUntil[id].After(now) {
			continue
		}
		message.Attempts++
		message.LeaseToken = leaseToken
		message.Payload = append([]byte(nil), message.Payload...)
		s.outbox[id] = message
		s.leaseUntil[id] = now.Add(leaseDuration)
		items = append(items, message)
	}
	return items, nil
}

func (s *Store) MarkOutboxPublished(_ context.Context, id int64, leaseToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	message, ok := s.outbox[id]
	if !ok || message.LeaseToken != leaseToken {
		return fmt.Errorf("outbox message %d lease lost", id)
	}
	delete(s.outbox, id)
	delete(s.availableAt, id)
	delete(s.leaseUntil, id)
	return nil
}

func (s *Store) ReleaseOutbox(_ context.Context, id int64, leaseToken string, nextAttempt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	message, ok := s.outbox[id]
	if !ok || message.LeaseToken != leaseToken {
		return fmt.Errorf("outbox message %d lease lost", id)
	}
	message.LeaseToken = ""
	s.outbox[id] = message
	s.availableAt[id] = nextAttempt
	delete(s.leaseUntil, id)
	return nil
}

func clone(event domain.HazardEvent) domain.HazardEvent {
	event.Attributes = mapsClone(event.Attributes)
	return event
}

func mapsClone(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func defaultVolcanoes() map[string]domain.VolcanoReference {
	return map[string]domain.VolcanoReference{
		"merapi":        {Name: "Gunung Merapi", Latitude: -7.54, Longitude: 110.44},
		"semeru":        {Name: "Gunung Semeru", Latitude: -8.11, Longitude: 112.92},
		"anak_krakatau": {Name: "Anak Krakatau", Latitude: -6.10, Longitude: 105.42},
	}
}

func cloneVolcanoes(input map[string]domain.VolcanoReference) map[string]domain.VolcanoReference {
	output := make(map[string]domain.VolcanoReference, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
