package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/adapters/memory"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

func TestWarningsByEventIDCorrelatesUsingRelatedEventID(t *testing.T) {
	warning := map[string]json.RawMessage{
		"warning_id":       json.RawMessage(`"warning-1"`),
		"related_event_id": json.RawMessage(`"event-1"`),
	}

	warnings := warningsByEventID([]map[string]json.RawMessage{warning})
	if warnings["event-1"]["warning_id"] == nil {
		t.Fatalf("warning was not indexed by related event ID: %#v", warnings)
	}
	if _, indexedByWarningID := warnings["warning-1"]; indexedByWarningID {
		t.Fatal("warning was indexed by warning ID instead of related event ID")
	}
}

// fakeSource is a scriptable Source for scheduling tests.
type fakeSource struct {
	name  domain.Source
	fetch func(ctx context.Context, since time.Time, correlationID string) ([]domain.HazardEvent, error)
}

func (f *fakeSource) Name() domain.Source { return f.name }

func (f *fakeSource) Fetch(ctx context.Context, since time.Time, correlationID string) ([]domain.HazardEvent, error) {
	return f.fetch(ctx, since, correlationID)
}

func testEvent(id string, source domain.Source) domain.HazardEvent {
	return domain.HazardEvent{HazardID: id, Source: source, SourceRefID: id, HazardType: domain.HazardSeismic, Severity: domain.SeverityNormal, OccurredAt: time.Now().UTC(), IngestedAt: time.Now().UTC(), Attributes: map[string]any{}}
}

func testPoller(repository Repository) *Poller {
	return &Poller{repository: repository, interval: time.Hour, timeout: time.Second, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

type observingRepository struct {
	*memory.Repository
	advanced chan time.Time
}

func (s *observingRepository) AdvancePollCursor(ctx context.Context, source domain.Source, cursor time.Time) error {
	if err := s.Repository.AdvancePollCursor(ctx, source, cursor); err != nil {
		return err
	}
	s.advanced <- cursor
	return nil
}

func TestRunSourceLoadsAndAdvancesPersistedCursorAfterSuccess(t *testing.T) {
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1)}
	starting := time.Now().UTC().Add(-time.Hour)
	if err := repository.Repository.AdvancePollCursor(context.Background(), domain.SourceBMKG, starting); err != nil {
		t.Fatal(err)
	}

	called := make(chan time.Time, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	source := &fakeSource{name: domain.SourceBMKG, fetch: func(_ context.Context, since time.Time, _ string) ([]domain.HazardEvent, error) {
		called <- since
		return nil, nil
	}}
	go poller.runSource(ctx, source)

	select {
	case since := <-called:
		if since.Before(starting.Add(-3*time.Second)) || since.After(starting) {
			t.Fatalf("poll did not use persisted cursor: since=%s cursor=%s", since, starting)
		}
	case <-time.After(time.Second):
		t.Fatal("poll did not start")
	}

	select {
	case advanced := <-repository.advanced:
		if !advanced.After(starting) {
			t.Fatalf("successful batch did not advance cursor: %s", advanced)
		}
	case <-time.After(time.Second):
		t.Fatal("cursor was not advanced after successful batch")
	}
	cancel()
}

func TestRunSourceDoesNotAdvanceCursorWhenFetchFails(t *testing.T) {
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1)}
	starting := time.Now().UTC().Add(-time.Hour)
	if err := repository.Repository.AdvancePollCursor(context.Background(), domain.SourcePVMBG, starting); err != nil {
		t.Fatal(err)
	}

	called := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	source := &fakeSource{name: domain.SourcePVMBG, fetch: func(_ context.Context, _ time.Time, _ string) ([]domain.HazardEvent, error) {
		called <- struct{}{}
		return nil, errors.New("source unavailable")
	}}
	go poller.runSource(ctx, source)

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("poll did not start")
	}
	select {
	case advanced := <-repository.advanced:
		t.Fatalf("failed batch advanced cursor to %s", advanced)
	case <-time.After(20 * time.Millisecond):
	}
	cursor, err := repository.GetPollCursor(context.Background(), domain.SourcePVMBG)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Equal(starting) {
		t.Fatalf("failed batch changed cursor: got %s, want %s", cursor, starting)
	}
	status, err := repository.GetSourceStatus(context.Background(), domain.SourcePVMBG)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != domain.SourceStateStale {
		t.Fatalf("failed batch status = %q, want %q", status.Status, domain.SourceStateStale)
	}
	cancel()
}

func TestRunSourcePersistsFetchedEvents(t *testing.T) {
	repository := memory.NewRepository()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	source := &fakeSource{name: domain.SourceBMKG, fetch: func(_ context.Context, _ time.Time, _ string) ([]domain.HazardEvent, error) {
		return []domain.HazardEvent{testEvent("h-1", domain.SourceBMKG)}, nil
	}}
	go poller.runSource(ctx, source)

	deadline := time.After(time.Second)
	for {
		event, err := repository.GetHazard(context.Background(), "h-1")
		if err == nil {
			if event.SourceRefID != "h-1" {
				t.Fatalf("persisted event = %+v", event)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatalf("event was not persisted: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestRunSourceAdvancesPastPoisonPayloads(t *testing.T) {
	repository := &observingRepository{Repository: memory.NewRepository(), advanced: make(chan time.Time, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := testPoller(repository)
	source := &fakeSource{name: domain.SourcePVMBG, fetch: func(_ context.Context, _ time.Time, _ string) ([]domain.HazardEvent, error) {
		return []domain.HazardEvent{testEvent("h-2", domain.SourcePVMBG)}, &MappingError{Source: domain.SourcePVMBG, Err: errors.New("bad payload")}
	}}
	go poller.runSource(ctx, source)

	select {
	case <-repository.advanced:
	case <-time.After(time.Second):
		t.Fatal("mapping errors must not block cursor advancement")
	}
	if _, err := repository.GetHazard(context.Background(), "h-2"); err != nil {
		t.Fatalf("successfully mapped event was not persisted: %v", err)
	}
	cancel()
}

func TestNewPollerValidatesConfig(t *testing.T) {
	repository := memory.NewRepository()
	source := &fakeSource{name: domain.SourceBMKG, fetch: func(context.Context, time.Time, string) ([]domain.HazardEvent, error) { return nil, nil }}

	if _, err := NewPoller(PollerConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty config error = %v, want ErrInvalidConfig", err)
	}
	if _, err := NewPoller(PollerConfig{Repository: repository, Sources: []Source{source}, Interval: time.Second, Timeout: 0}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("zero timeout error = %v, want ErrInvalidConfig", err)
	}
	poller, err := NewPoller(PollerConfig{Repository: repository, Sources: []Source{source}, Interval: time.Second, Timeout: time.Second})
	if err != nil {
		t.Fatalf("valid config failed: %v", err)
	}
	if poller == nil || len(poller.sources) != 1 {
		t.Fatalf("poller = %+v", poller)
	}
}
