package application

import (
	"context"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/adapters/memory"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/ports"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func newWatermarkStream(state *memory.State, sink *recordingSink, wallClock time.Time) *Stream {
	return NewStream(StreamConfig{
		State: state, Sink: sink, Logger: testLogger(),
		LateWindow: 6 * time.Hour, MaxAge: 24 * time.Hour,
		Now: func() time.Time { return wallClock },
	})
}

// The feed replays on its own virtual clock; detections far from the BNPB
// clock (here three years earlier) must still be clustered.
func TestVirtualTimeDetectionsAreNotJudgedByWallClock(t *testing.T) {
	ctx := context.Background()
	virtual := time.Date(2023, 8, 14, 5, 30, 0, 0, time.UTC)
	sink := &recordingSink{}
	stream := newWatermarkStream(memory.New(), sink, time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC))
	events := []ports.Event{
		{Offset: 1, Kind: ports.KindDetection, Detection: detection(virtual, -2.10, domain.ConfidenceHigh)},
		{Offset: 2, Kind: ports.KindDetection, Detection: detection(virtual, -2.101, domain.ConfidenceNominal)},
	}
	if err := stream.process(ctx, events); err != nil {
		t.Fatal(err)
	}
	if len(sink.batches) != 1 || len(sink.batches[0]) != 1 || sink.batches[0][0].DetectionCount != 2 {
		t.Fatalf("batches = %+v, want one fire of two detections", sink.batches)
	}
}

// Lateness is relative to the latest acquisition time seen; a detection more
// than LateWindow behind it is counted, not clustered and not lost silently.
func TestDetectionsBeyondAllowedLatenessAreCounted(t *testing.T) {
	ctx := context.Background()
	newest := time.Date(2023, 8, 14, 12, 0, 0, 0, time.UTC)
	sink := &recordingSink{}
	stream := newWatermarkStream(memory.New(), sink, time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC))
	before := testutil.ToFloat64(droppedDetections.WithLabelValues("late"))
	events := []ports.Event{
		{Offset: 1, Kind: ports.KindDetection, Detection: detection(newest, -2.10, domain.ConfidenceHigh)},
		// 5h59 behind: within the allowed lateness, joins state.
		{Offset: 2, Kind: ports.KindDetection, Detection: detection(newest.Add(-359*time.Minute), 1.5, domain.ConfidenceHigh)},
		// 6h01 behind: late.
		{Offset: 3, Kind: ports.KindDetection, Detection: detection(newest.Add(-361*time.Minute), 3.5, domain.ConfidenceHigh)},
	}
	if err := stream.process(ctx, events); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(droppedDetections.WithLabelValues("late")) - before; got != 1 {
		t.Fatalf("late counter grew by %v, want 1", got)
	}
	stored, err := stream.state.Detections(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored) != 2 {
		t.Fatalf("stored %d detections, want 2 (late one excluded)", len(stored))
	}
}

// A replacement replica restores the watermark from durable state, so it
// judges a late detection exactly as the replica it replaced would have.
func TestWatermarkSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	newest := time.Date(2023, 8, 14, 12, 0, 0, 0, time.UTC)
	state := memory.New()
	first := newWatermarkStream(state, &recordingSink{}, time.Now())
	if err := first.process(ctx, []ports.Event{{Offset: 1, Kind: ports.KindDetection, Detection: detection(newest, -2.10, domain.ConfidenceHigh)}}); err != nil {
		t.Fatal(err)
	}
	second := newWatermarkStream(state, &recordingSink{}, time.Now())
	before := testutil.ToFloat64(droppedDetections.WithLabelValues("late"))
	if err := second.process(ctx, []ports.Event{{Offset: 2, Kind: ports.KindDetection, Detection: detection(newest.Add(-7*time.Hour), 4.0, domain.ConfidenceHigh)}}); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(droppedDetections.WithLabelValues("late")) - before; got != 1 {
		t.Fatalf("restarted stream accepted a detection 7h behind the stored watermark")
	}
}
