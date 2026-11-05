package application

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/ports"
)

// StreamConfig holds the dependencies and tuning for the consume loop.
type StreamConfig struct {
	Feed   ports.Feed
	State  ports.State
	Sink   ports.HazardSink
	Logger *slog.Logger
	// LateWindow drops detections older than this at ingest time.
	LateWindow time.Duration
	// MaxAge prunes detections older than this from state.
	MaxAge     time.Duration
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// Now and Sleep are optional test seams; production uses time.Now and a
	// context-aware timer.
	Now   func() time.Time
	Sleep func(ctx context.Context, delay time.Duration) error
}

// Stream consumes the feed, clusters detections, and checkpoints the feed
// offset only after state and the sink have both accepted an event.
type Stream struct {
	feed       ports.Feed
	state      ports.State
	sink       ports.HazardSink
	logger     *slog.Logger
	lateWindow time.Duration
	maxAge     time.Duration
	minBackoff time.Duration
	maxBackoff time.Duration
	now        func() time.Time
	sleep      func(ctx context.Context, delay time.Duration) error
}

// NewStream builds a consume loop.
func NewStream(cfg StreamConfig) *Stream {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	sleep := cfg.Sleep
	if sleep == nil {
		sleep = sleepContext
	}
	return &Stream{
		feed:       cfg.Feed,
		state:      cfg.State,
		sink:       cfg.Sink,
		logger:     cfg.Logger,
		lateWindow: cfg.LateWindow,
		maxAge:     cfg.MaxAge,
		minBackoff: cfg.MinBackoff,
		maxBackoff: cfg.MaxBackoff,
		now:        now,
		sleep:      sleep,
	}
}

// Run reconnects from the last committed offset until ctx is cancelled.
func (s *Stream) Run(ctx context.Context) error {
	attempt := 0
	for {
		offset, err := s.state.Offset(ctx)
		if err != nil {
			return fmt.Errorf("load feed offset: %w", err)
		}
		err = s.feed.Stream(ctx, offset, s.handle)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			attempt = 0
		}
		s.logger.Warn("feed stream ended",
			"service", "satellite-ingestor",
			"component", "stream",
			"offset", offset,
			"error", err,
		)
		delay := s.backoff(attempt)
		attempt++
		if err := s.sleep(ctx, delay); err != nil {
			return nil
		}
	}
}

func (s *Stream) backoff(attempt int) time.Duration {
	delay := s.minBackoff
	for i := 0; i < attempt && delay < s.maxBackoff; i++ {
		delay *= 2
	}
	if delay > s.maxBackoff {
		delay = s.maxBackoff
	}
	return delay
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *Stream) handle(ctx context.Context, event ports.Event) error {
	if event.Kind == ports.KindHeartbeat {
		return s.state.SaveOffset(ctx, event.Offset)
	}

	detections, err := s.state.Detections(ctx)
	if err != nil {
		return fmt.Errorf("load detections: %w", err)
	}
	previous, err := s.state.Fires(ctx)
	if err != nil {
		return fmt.Errorf("load fires: %w", err)
	}

	switch event.Kind {
	case ports.KindDetection, ports.KindUpdate:
		if !s.accept(event.Detection) {
			return s.state.SaveOffset(ctx, event.Offset)
		}
		detections = upsertDetection(detections, event.Detection)
	case ports.KindRetract:
		detections = removeDetection(detections, event.DetectionKey)
	default:
		return nil
	}

	detections = s.prune(detections)
	fires, _ := Cluster(detections)
	current, changes := reconcile(previous, fires)

	if err := s.state.SaveDetections(ctx, detections); err != nil {
		return fmt.Errorf("save detections: %w", err)
	}
	// Submit before saving the fire snapshot so a failed send is retried from
	// the previous snapshot, including retraction tombstones.
	if len(changes) > 0 {
		if err := s.sink.Submit(ctx, changes); err != nil {
			return fmt.Errorf("submit fires: %w", err)
		}
		for _, fire := range changes {
			firesSubmitted.WithLabelValues(fire.Status).Inc()
		}
	}
	if err := s.state.SaveFires(ctx, current); err != nil {
		return fmt.Errorf("save fires: %w", err)
	}
	return s.state.SaveOffset(ctx, event.Offset)
}

func (s *Stream) accept(detection domain.Detection) bool {
	at, err := detection.Time()
	if err != nil {
		droppedDetections.WithLabelValues("invalid_time").Inc()
		return false
	}
	if s.now().Sub(at) > s.lateWindow {
		droppedDetections.WithLabelValues("late").Inc()
		return false
	}
	return true
}

func (s *Stream) prune(detections []domain.Detection) []domain.Detection {
	cutoff := s.now().Add(-s.maxAge)
	kept := detections[:0]
	for _, detection := range detections {
		at, err := detection.Time()
		if err != nil || at.Before(cutoff) {
			continue
		}
		kept = append(kept, detection)
	}
	return kept
}

func upsertDetection(detections []domain.Detection, detection domain.Detection) []domain.Detection {
	key := detection.Key()
	for i := range detections {
		if detections[i].Key() == key {
			detections[i] = detection
			return detections
		}
	}
	return append(detections, detection)
}

func removeDetection(detections []domain.Detection, key string) []domain.Detection {
	kept := detections[:0]
	for _, detection := range detections {
		if detection.Key() != key {
			kept = append(kept, detection)
		}
	}
	return kept
}
