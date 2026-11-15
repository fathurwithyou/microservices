package memory

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/analytics"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/audit"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/hazards"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/google/uuid"
)

type Repository struct {
	mu           sync.RWMutex
	events       map[string]domain.HazardEvent
	bySource     map[string]string
	status       map[domain.Source]domain.SourceStatus
	outbox       map[int64]domain.OutboxMessage
	cursors      map[domain.Source]time.Time
	volcanoes    map[string]domain.VolcanoReference
	fieldReports map[string]domain.FieldReport
	failures     map[string]domain.IngestFailure
	availableAt  map[int64]time.Time
	leaseUntil   map[int64]time.Time
	nextID       int64
}

func NewRepository() *Repository {
	return &Repository{
		events:       make(map[string]domain.HazardEvent),
		bySource:     make(map[string]string),
		status:       make(map[domain.Source]domain.SourceStatus),
		outbox:       make(map[int64]domain.OutboxMessage),
		cursors:      make(map[domain.Source]time.Time),
		volcanoes:    defaultVolcanoes(),
		fieldReports: make(map[string]domain.FieldReport),
		failures:     make(map[string]domain.IngestFailure),
		availableAt:  make(map[int64]time.Time),
		leaseUntil:   make(map[int64]time.Time),
	}
}

func (s *Repository) PersistHazardAndEnqueue(_ context.Context, event domain.HazardEvent, message domain.OutboxMessage) error {
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

func (s *Repository) GetHazard(_ context.Context, id string) (domain.HazardEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	event, ok := s.events[id]
	if !ok {
		return domain.HazardEvent{}, domain.ErrNotFound
	}
	return clone(event), nil
}

// SaveFieldReport stores a field report once by ReportID.
func (s *Repository) SaveFieldReport(_ context.Context, report domain.FieldReport) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.fieldReports[report.ReportID]; exists {
		return false, nil
	}
	s.fieldReports[report.ReportID] = report
	return true, nil
}

// ListAcceptedFieldReports returns accepted reports in arrival order.
func (s *Repository) ListAcceptedFieldReports(_ context.Context, limit int) ([]domain.FieldReport, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	reports := make([]domain.FieldReport, 0)
	for _, report := range s.fieldReports {
		if report.Status == domain.FieldReportAccepted {
			reports = append(reports, report)
		}
	}
	sort.Slice(reports, func(i, j int) bool { return reports[i].ReceivedAt.Before(reports[j].ReceivedAt) })
	if limit > 0 && len(reports) > limit {
		reports = reports[:limit]
	}
	return reports, nil
}

// MarkFieldReport records the terminal state of one accepted report.
func (s *Repository) MarkFieldReport(_ context.Context, reportID string, status domain.FieldReportStatus, processedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	report, ok := s.fieldReports[reportID]
	if !ok || report.Status != domain.FieldReportAccepted {
		return nil
	}
	report.Status = status
	report.Attempts++
	report.ProcessedAt = &processedAt
	s.fieldReports[reportID] = report
	return nil
}

// ListAuditEvents returns events ordered by ingested_at then hazard_id.
func (s *Repository) ListAuditEvents(_ context.Context, filter audit.Filter) ([]domain.HazardEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.HazardEvent, 0, len(s.events))
	for _, event := range s.events {
		if filter.Source != "" && event.Source != filter.Source {
			continue
		}
		if event.IngestedAt.Before(filter.Since) {
			continue
		}
		if filter.Cursor != "" {
			if event.IngestedAt.Before(filter.Since) || (event.IngestedAt.Equal(filter.Since) && event.HazardID <= filter.Cursor) {
				continue
			}
		}
		items = append(items, clone(event))
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].IngestedAt.Equal(items[j].IngestedAt) {
			return items[i].IngestedAt.Before(items[j].IngestedAt)
		}
		return items[i].HazardID < items[j].HazardID
	})
	if filter.Limit > 0 && len(items) > filter.Limit {
		items = items[:filter.Limit]
	}
	return items, nil
}

// SourceRefIDs returns sorted source_ref_id values for checksum.
func (s *Repository) SourceRefIDs(_ context.Context, source domain.Source) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.events))
	for _, event := range s.events {
		if source != "" && event.Source != source {
			continue
		}
		ids = append(ids, event.SourceRefID)
	}
	sort.Strings(ids)
	return ids, nil
}

// SaveIngestFailure stores a raw rejected payload idempotently.
func (s *Repository) SaveIngestFailure(_ context.Context, failure domain.IngestFailure) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.failures[failure.MessageID]; !exists {
		s.failures[failure.MessageID] = failure
	}
	return nil
}

// MarkIngestFailureRecovered removes a replayed payload from the active queue.
func (s *Repository) MarkIngestFailureRecovered(_ context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	failure, ok := s.failures[messageID]
	if !ok {
		return nil
	}
	failure.Status = domain.FailureRecovered
	failure.Attempts++
	s.failures[messageID] = failure
	return nil
}

// RecordIngestFailureAttempt counts one failed automatic retry.
func (s *Repository) RecordIngestFailureAttempt(_ context.Context, messageID, lastError string, retryable, park bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	failure, ok := s.failures[messageID]
	if !ok || failure.Status != domain.FailureRetrying {
		return nil
	}
	failure.Attempts++
	failure.LastError = lastError
	failure.Retryable = retryable
	if park {
		failure.Status = domain.FailureParked
	}
	s.failures[messageID] = failure
	return nil
}

// GetIngestFailure returns one stored failure, including recovered ones.
func (s *Repository) GetIngestFailure(_ context.Context, messageID string) (domain.IngestFailure, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	failure, ok := s.failures[messageID]
	if !ok {
		return domain.IngestFailure{}, domain.ErrNotFound
	}
	failure.Payload = bytes.Clone(failure.Payload)
	failure.PayloadBytes = len(failure.Payload)
	return failure, nil
}

// ListIngestFailures returns failure metadata without payload bytes.
func (s *Repository) ListIngestFailures(_ context.Context, filter domain.FailureFilter) ([]domain.IngestFailure, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.IngestFailure, 0)
	for _, failure := range s.failures {
		if filter.Source != "" && failure.Source != filter.Source {
			continue
		}
		if filter.Status != "" && failure.Status != filter.Status {
			continue
		}
		if filter.Status == "" && failure.Status != domain.FailureParked && failure.Status != domain.FailureRetrying {
			continue
		}
		if filter.AfterID != "" && (failure.ReceivedAt.Before(filter.After) ||
			(failure.ReceivedAt.Equal(filter.After) && failure.MessageID <= filter.AfterID)) {
			continue
		}
		failure.PayloadBytes = len(failure.Payload)
		failure.Payload = nil
		items = append(items, failure)
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].ReceivedAt.Equal(items[j].ReceivedAt) {
			return items[i].ReceivedAt.Before(items[j].ReceivedAt)
		}
		return items[i].MessageID < items[j].MessageID
	})
	if filter.Limit > 0 && len(items) > filter.Limit {
		items = items[:filter.Limit]
	}
	return items, nil
}

// HazardExists reports whether the canonical store contains hazard_id.
func (s *Repository) HazardExists(_ context.Context, hazardID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.events[hazardID]
	return ok, nil
}

func (s *Repository) CountByHazardType(_ context.Context, filter analytics.Filter) ([]analytics.TypeCount, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := map[domain.HazardType]int64{}
	for _, event := range s.events {
		if inWindow(event, filter) {
			counts[analytics.Category(event)]++
		}
	}
	result := make([]analytics.TypeCount, 0, len(counts))
	for kind, count := range counts {
		result = append(result, analytics.TypeCount{HazardType: kind, Count: count})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].HazardType < result[j].HazardType })
	return result, nil
}

func (s *Repository) CountBySeverity(_ context.Context, filter analytics.Filter) ([]analytics.SeverityCount, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := map[domain.Severity]int64{}
	for _, event := range s.events {
		if inWindow(event, filter) {
			counts[event.Severity]++
		}
	}
	result := make([]analytics.SeverityCount, 0, len(counts))
	for severity, count := range counts {
		result = append(result, analytics.SeverityCount{Severity: severity, Count: count})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Severity < result[j].Severity })
	return result, nil
}

func (s *Repository) CountByArea(_ context.Context, filter analytics.Filter) ([]analytics.AreaCount, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := map[string]int64{}
	for _, event := range s.events {
		if inWindow(event, filter) {
			counts[event.AreaName]++
		}
	}
	result := make([]analytics.AreaCount, 0, len(counts))
	for area, count := range counts {
		result = append(result, analytics.AreaCount{AreaName: area, Count: count})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Count != result[j].Count {
			return result[i].Count > result[j].Count
		}
		return result[i].AreaName < result[j].AreaName
	})
	return result, nil
}

func (s *Repository) CountByTimeBucket(_ context.Context, filter analytics.Filter) ([]analytics.TimeBucket, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	type key struct {
		start time.Time
		kind  domain.HazardType
	}
	counts := map[key]int64{}
	for _, event := range s.events {
		if inWindow(event, filter) {
			counts[key{start: bucketStart(event.OccurredAt, filter.Bucket), kind: analytics.Category(event)}]++
		}
	}
	result := make([]analytics.TimeBucket, 0, len(counts))
	for k, count := range counts {
		result = append(result, analytics.TimeBucket{Start: k.start, HazardType: k.kind, Count: count})
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].Start.Equal(result[j].Start) {
			return result[i].Start.Before(result[j].Start)
		}
		return result[i].HazardType < result[j].HazardType
	})
	return result, nil
}

func inWindow(event domain.HazardEvent, filter analytics.Filter) bool {
	return !event.OccurredAt.Before(filter.Since) && event.OccurredAt.Before(filter.Until)
}

func bucketStart(at time.Time, bucket analytics.Bucket) time.Time {
	at = at.UTC()
	switch bucket {
	case analytics.BucketHour:
		return at.Truncate(time.Hour)
	case analytics.BucketWeek:
		day := at.Truncate(24 * time.Hour)
		weekday := int(day.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		return day.AddDate(0, 0, -(weekday - 1))
	default:
		return at.Truncate(24 * time.Hour)
	}
}

func (s *Repository) ListHazards(_ context.Context, filter hazards.ListFilter) ([]domain.HazardEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.HazardEvent, 0, len(s.events))
	for _, event := range s.events {
		if filter.Source != "" && event.Source != filter.Source || filter.HazardType != "" && event.HazardType != filter.HazardType || filter.Severity != "" && event.Severity != filter.Severity {
			continue
		}
		items = append(items, clone(event))
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].OccurredAt.Equal(items[j].OccurredAt) {
			return items[i].OccurredAt.After(items[j].OccurredAt)
		}
		return items[i].HazardID > items[j].HazardID
	})
	cursor, after, err := hazards.DecodePageToken(filter.PageToken)
	if err != nil {
		return nil, err
	}
	start := 0
	if after {
		start = sort.Search(len(items), func(i int) bool { return cursor.Before(items[i]) })
	}
	end := len(items)
	if filter.PageSize > 0 && start+filter.PageSize < end {
		end = start + filter.PageSize
	}
	return items[start:end], nil
}

func (s *Repository) SetSourceStatus(_ context.Context, status domain.SourceStatus) error {
	s.mu.Lock()
	s.status[status.Source] = status
	s.mu.Unlock()
	return nil
}

func (s *Repository) GetSourceStatus(_ context.Context, source domain.Source) (domain.SourceStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	status, ok := s.status[source]
	if !ok {
		return domain.SourceStatus{Source: source, Status: domain.SourceStateUnknown}, nil
	}
	return status, nil
}

func (s *Repository) GetPollCursor(_ context.Context, source domain.Source) (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cursors[source], nil
}

func (s *Repository) AdvancePollCursor(_ context.Context, source domain.Source, cursor time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cursor.After(s.cursors[source]) {
		s.cursors[source] = cursor
	}
	return nil
}

func (s *Repository) LoadVolcanoReferences(_ context.Context) (map[string]domain.VolcanoReference, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneVolcanoes(s.volcanoes), nil
}

func (s *Repository) ClaimPendingOutbox(_ context.Context, limit int, leaseDuration time.Duration) ([]domain.OutboxMessage, error) {
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

func (s *Repository) MarkOutboxPublished(_ context.Context, id int64, leaseToken string) error {
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

func (s *Repository) ReleaseOutbox(_ context.Context, id int64, leaseToken string, nextAttempt time.Time) error {
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
	event.Attributes = maps.Clone(event.Attributes)
	return event
}

func defaultVolcanoes() map[string]domain.VolcanoReference {
	return map[string]domain.VolcanoReference{
		"merapi":        {Name: "Gunung Merapi", Latitude: -7.54, Longitude: 110.44},
		"semeru":        {Name: "Gunung Semeru", Latitude: -8.11, Longitude: 112.92},
		"anak_krakatau": {Name: "Anak Krakatau", Latitude: -6.10, Longitude: 105.42},
	}
}

func cloneVolcanoes(input map[string]domain.VolcanoReference) map[string]domain.VolcanoReference {
	return maps.Clone(input)
}

// HazardLocations returns the single in-memory unit for a hazard.
func (r *Repository) HazardLocations(_ context.Context, _ string) ([]audit.Location, error) {
	return nil, nil
}

// HazardDistribution reports one unit holding everything in memory.
func (r *Repository) HazardDistribution(context.Context) (audit.Distribution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return audit.Distribution{Ranges: 1, Nodes: []audit.NodeShare{{Node: "memory", NodeID: 1, Live: true, Replicas: 1, Leaseholders: 1}}}, nil
}

// UnavailableUnits is always empty for the in-memory store.
func (s *Repository) UnavailableUnits(context.Context) ([]string, error) { return nil, nil }
