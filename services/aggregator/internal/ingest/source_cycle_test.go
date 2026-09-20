package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/store/memory"
)

type observingStore struct {
	*memory.Store
	advanced chan time.Time
}

func (s *observingStore) AdvancePollCursor(ctx context.Context, source domain.Source, cursor time.Time) error {
	if err := s.Store.AdvancePollCursor(ctx, source, cursor); err != nil {
		return err
	}
	s.advanced <- cursor
	return nil
}

func TestRunSourceLoadsAndAdvancesPersistedCursorAfterSuccess(t *testing.T) {
	store := &observingStore{Store: memory.New(), advanced: make(chan time.Time, 1)}
	starting := time.Now().UTC().Add(-time.Hour)
	if err := store.Store.AdvancePollCursor(context.Background(), domain.SourceBMKG, starting); err != nil {
		t.Fatal(err)
	}

	called := make(chan time.Time, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := &Poller{store: store, interval: time.Hour, timeout: time.Second, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	go poller.runSource(ctx, domain.SourceBMKG, func(_ context.Context, since time.Time, _ string) error {
		called <- since
		return nil
	})

	select {
	case since := <-called:
		if since.Before(starting.Add(-3*time.Second)) || since.After(starting) {
			t.Fatalf("poll did not use persisted cursor: since=%s cursor=%s", since, starting)
		}
	case <-time.After(time.Second):
		t.Fatal("poll did not start")
	}

	select {
	case advanced := <-store.advanced:
		if !advanced.After(starting) {
			t.Fatalf("successful batch did not advance cursor: %s", advanced)
		}
	case <-time.After(time.Second):
		t.Fatal("cursor was not advanced after successful batch")
	}
	cancel()
}

func TestRunSourceDoesNotAdvanceCursorWhenBatchFails(t *testing.T) {
	store := &observingStore{Store: memory.New(), advanced: make(chan time.Time, 1)}
	starting := time.Now().UTC().Add(-time.Hour)
	if err := store.Store.AdvancePollCursor(context.Background(), domain.SourcePVMBG, starting); err != nil {
		t.Fatal(err)
	}

	called := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := &Poller{store: store, interval: time.Hour, timeout: time.Second, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	go poller.runSource(ctx, domain.SourcePVMBG, func(_ context.Context, _ time.Time, _ string) error {
		called <- struct{}{}
		return errors.New("source unavailable")
	})

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("poll did not start")
	}
	select {
	case advanced := <-store.advanced:
		t.Fatalf("failed batch advanced cursor to %s", advanced)
	case <-time.After(20 * time.Millisecond):
	}
	cursor, err := store.GetPollCursor(context.Background(), domain.SourcePVMBG)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Equal(starting) {
		t.Fatalf("failed batch changed cursor: got %s, want %s", cursor, starting)
	}
	cancel()
}
