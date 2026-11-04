package application

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/adapters/memory"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/ports"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func detection(window time.Time, latitude float64, confidence domain.Confidence) domain.Detection {
	return domain.Detection{
		Satellite:  "NOAA-20",
		AcqDate:    window.Format("2006-01-02"),
		AcqTime:    window.Format("1504"),
		Latitude:   latitude,
		Longitude:  106.8,
		Confidence: confidence,
		FRP:        10,
	}
}

type failingSink struct{ calls int }

func (s *failingSink) Submit(context.Context, []domain.Fire) error {
	s.calls++
	return errors.New("sink unavailable")
}

func TestOffsetNotAdvancedWhenSinkFails(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	state := memory.New()
	if err := state.SaveDetections(ctx, []domain.Detection{detection(now, -6.2, domain.ConfidenceHigh)}); err != nil {
		t.Fatal(err)
	}

	sink := &failingSink{}
	stream := NewStream(StreamConfig{
		State:      state,
		Sink:       sink,
		Logger:     testLogger(),
		LateWindow: 6 * time.Hour,
		MaxAge:     24 * time.Hour,
		MinBackoff: time.Millisecond,
		MaxBackoff: time.Millisecond,
		Now:        func() time.Time { return now },
	})

	// The second detection forms a fire with the stored one, so the sink is
	// called before the offset would be checkpointed.
	event := ports.Event{Offset: 7, Kind: ports.KindDetection, Detection: detection(now, -6.203, domain.ConfidenceHigh)}
	if err := stream.handle(ctx, event); err == nil {
		t.Fatal("handle() error = nil, want sink failure")
	}
	if sink.calls != 1 {
		t.Fatalf("sink calls = %d, want 1", sink.calls)
	}
	offset, err := state.Offset(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0 {
		t.Errorf("offset = %d, want 0 so the event is replayed", offset)
	}
	detections, err := state.Detections(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(detections) != 2 {
		t.Errorf("stored detections = %d, want 2", len(detections))
	}
	fires, err := state.Fires(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(fires) != 1 {
		t.Errorf("stored fires = %d, want 1", len(fires))
	}
}

type recordingFeed struct {
	mu      sync.Mutex
	offsets []int64
	calls   int
	retried chan struct{}
	once    sync.Once
}

func (f *recordingFeed) Stream(ctx context.Context, offset int64, _ ports.EventHandler) error {
	f.mu.Lock()
	f.offsets = append(f.offsets, offset)
	f.calls++
	call := f.calls
	f.mu.Unlock()

	if call == 1 {
		return errors.New("connection refused")
	}
	f.once.Do(func() { close(f.retried) })
	<-ctx.Done()
	return ctx.Err()
}

func TestRunReconnectsFromLastOffset(t *testing.T) {
	ctx := context.Background()
	state := memory.New()
	if err := state.SaveOffset(ctx, 42); err != nil {
		t.Fatal(err)
	}

	feed := &recordingFeed{retried: make(chan struct{})}
	stream := NewStream(StreamConfig{
		Feed:       feed,
		State:      state,
		Sink:       &failingSink{},
		Logger:     testLogger(),
		LateWindow: 6 * time.Hour,
		MaxAge:     24 * time.Hour,
		MinBackoff: time.Millisecond,
		MaxBackoff: time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- stream.Run(runCtx) }()

	select {
	case <-feed.retried:
	case <-time.After(time.Second):
		t.Fatal("feed was not retried after failure")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run() error = %v, want nil on cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop after cancellation")
	}

	feed.mu.Lock()
	defer feed.mu.Unlock()
	if len(feed.offsets) < 2 {
		t.Fatalf("feed calls = %v, want at least 2", feed.offsets)
	}
	if feed.offsets[1] != 42 {
		t.Errorf("reconnect offset = %d, want checkpoint 42", feed.offsets[1])
	}
}
