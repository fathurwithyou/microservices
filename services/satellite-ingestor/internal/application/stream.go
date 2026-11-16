package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
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
	// SessionInterval is how often the feed session is polled for lag.
	SessionInterval time.Duration
	// Leadership gates the stream to one active replica.
	Leadership Leadership
	// Now and Sleep are optional test seams; production uses time.Now and a
	// context-aware timer.
	Now   func() time.Time
	Sleep func(ctx context.Context, delay time.Duration) error
}

// Stream consumes the feed, clusters detections, and checkpoints the feed
// offset only after state and the sink have both accepted an event.
type Stream struct {
	feed            ports.Feed
	state           ports.State
	sink            ports.HazardSink
	logger          *slog.Logger
	lateWindow      time.Duration
	maxAge          time.Duration
	minBackoff      time.Duration
	maxBackoff      time.Duration
	now             func() time.Time
	sleep           func(ctx context.Context, delay time.Duration) error
	sessionInterval time.Duration
	leadership      Leadership
	committed       atomic.Int64
	// watermark is the latest satellite acquisition time accepted so far.
	// Lateness is measured against it, never against the BNPB clock: the feed
	// runs on its own (possibly accelerated) virtual time, and P4 groups
	// detections by when the satellite saw them, not when they arrived.
	watermark      time.Time
	watermarkReady bool
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
	sessionInterval := cfg.SessionInterval
	if sessionInterval <= 0 {
		sessionInterval = 5 * time.Second
	}
	return &Stream{
		feed:            cfg.Feed,
		state:           cfg.State,
		sink:            cfg.Sink,
		logger:          cfg.Logger,
		lateWindow:      cfg.LateWindow,
		maxAge:          cfg.MaxAge,
		minBackoff:      cfg.MinBackoff,
		maxBackoff:      cfg.MaxBackoff,
		now:             now,
		sleep:           sleep,
		sessionInterval: sessionInterval,
		leadership:      cfg.Leadership,
	}
}

const (
	// batchBuffer bounds events waiting for the processor.
	batchBuffer = 4096
	// maxBatchSize bounds one state rewrite while catching up.
	maxBatchSize = 512
)

// ErrNotLeader stops the current stream when the lease is lost so the next
// replica can take over.
var ErrNotLeader = errors.New("not leader")

// Run reconnects from the last committed offset until ctx is cancelled.
func (s *Stream) Run(ctx context.Context) error {
	go s.reportLag(ctx)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	attempt := 0
	for {
		if !s.waitLeader(runCtx) {
			return nil
		}
		attemptCtx, attemptCancel := context.WithCancel(runCtx)
		events := make(chan ports.Event, batchBuffer)
		results := make(chan error, 1)
		go func() {
			results <- s.consume(attemptCtx, events)
			attemptCancel()
		}()
		handler := func(streamCtx context.Context, event ports.Event) error {
			select {
			case events <- event:
				return nil
			case <-streamCtx.Done():
				return streamCtx.Err()
			}
		}

		offset, err := s.state.Offset(attemptCtx)
		if err != nil {
			attemptCancel()
			<-results
			return fmt.Errorf("load feed offset: %w", err)
		}
		streamErr := s.feed.Stream(attemptCtx, offset, handler)
		attemptCancel()
		processErr := <-results
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(processErr, ErrNotLeader) {
			attempt = 0
			continue
		}
		if processErr != nil {
			return processErr
		}
		if streamErr == nil {
			attempt = 0
		}
		s.logger.Warn("feed stream ended",
			"service", "satellite-ingestor",
			"component", "stream",
			"offset", offset,
			"error", streamErr,
		)
		delay := s.backoff(attempt)
		attempt++
		if err := s.sleep(ctx, delay); err != nil {
			return nil
		}
	}
}

func (s *Stream) waitLeader(ctx context.Context) bool {
	if s.leadership == nil {
		return true
	}
	for !s.leadership.Allowed() {
		if err := s.sleep(ctx, time.Second); err != nil {
			return false
		}
	}
	return true
}

func (s *Stream) stillLeader(ctx context.Context) bool {
	if s.leadership == nil {
		return true
	}
	if !s.leadership.Allowed() {
		return false
	}
	valid, err := s.leadership.StillLeader(ctx)
	return err == nil && valid
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

// consume drains bursts so one state rewrite covers many detections.
func (s *Stream) consume(ctx context.Context, events <-chan ports.Event) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-events:
			batch := []ports.Event{event}
		drain:
			for len(batch) < maxBatchSize {
				select {
				case next := <-events:
					batch = append(batch, next)
				default:
					break drain
				}
			}
			if err := s.process(ctx, batch); err != nil {
				return err
			}
		}
	}
}

func (s *Stream) handle(ctx context.Context, event ports.Event) error {
	return s.process(ctx, []ports.Event{event})
}

func (s *Stream) process(ctx context.Context, events []ports.Event) error {
	if len(events) == 0 {
		return nil
	}
	if !s.stillLeader(ctx) {
		return ErrNotLeader
	}
	var maxOffset int64
	needsState := false
	for _, event := range events {
		if event.Offset > maxOffset {
			maxOffset = event.Offset
		}
		if event.Kind != ports.KindHeartbeat {
			needsState = true
		}
	}
	if !needsState {
		return s.saveOffset(ctx, maxOffset)
	}
	detections, previous, err := s.load(ctx)
	if err != nil {
		return err
	}
	s.initWatermark(detections)
	changed := false
	for _, event := range events {
		switch event.Kind {
		case ports.KindDetection, ports.KindUpdate:
			if !s.accept(event.Detection) {
				continue
			}
			detections = upsertDetection(detections, event.Detection)
			changed = true
		case ports.KindRetract:
			detections = removeDetection(detections, event.DetectionKey)
			changed = true
		}
	}
	if !changed {
		return s.saveOffset(ctx, maxOffset)
	}
	if !s.stillLeader(ctx) {
		return ErrNotLeader
	}

	detections = s.prune(detections)
	fires, _ := Cluster(detections)
	current, changes := reconcile(previous, fires)

	if err := s.state.SaveDetections(ctx, detections); err != nil {
		return fmt.Errorf("save detections: %w", err)
	}
	// Submit before saving the fire snapshot so a failed send is retried from
	// the previous snapshot, including retraction tombstones.
	if !s.stillLeader(ctx) {
		return ErrNotLeader
	}
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
	return s.saveOffset(ctx, maxOffset)
}

func (s *Stream) load(ctx context.Context) ([]domain.Detection, []domain.Fire, error) {
	detections, err := s.state.Detections(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("load detections: %w", err)
	}
	previous, err := s.state.Fires(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("load fires: %w", err)
	}
	return detections, previous, nil
}

func (s *Stream) saveOffset(ctx context.Context, offset int64) error {
	if err := s.state.SaveOffset(ctx, offset); err != nil {
		return err
	}
	s.committed.Store(offset)
	return nil
}

func (s *Stream) reportLag(ctx context.Context) {
	ticker := time.NewTicker(s.sessionInterval)
	defer ticker.Stop()
	for {
		session, err := s.feed.Session(ctx)
		if err == nil {
			lag := session.LatestOffset - s.committed.Load()
			if lag < 0 {
				lag = 0
			}
			feedLag.Set(float64(lag))
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

// initWatermark restores the watermark from durable state once per process
// lifetime, so a restarted replica judges lateness exactly like the one it
// replaced.
func (s *Stream) initWatermark(detections []domain.Detection) {
	if s.watermarkReady {
		return
	}
	for _, detection := range detections {
		if at, err := detection.Time(); err == nil && at.After(s.watermark) {
			s.watermark = at
		}
	}
	s.watermarkReady = true
	if !s.watermark.IsZero() {
		watermarkSeconds.Set(float64(s.watermark.Unix()))
	}
}

// accept admits a detection unless its acquisition time is more than
// lateWindow behind the watermark. Late detections are not dropped silently:
// they are counted (satellite_detections_dropped_total{reason="late"}) and
// logged with their key.
func (s *Stream) accept(detection domain.Detection) bool {
	at, err := detection.Time()
	if err != nil {
		droppedDetections.WithLabelValues("invalid_time").Inc()
		return false
	}
	if !s.watermark.IsZero() && s.watermark.Sub(at) > s.lateWindow {
		droppedDetections.WithLabelValues("late").Inc()
		s.logger.Warn("late detection not clustered",
			"service", "satellite-ingestor", "component", "stream",
			"detection", detection.Key(), "acquired_at", at, "watermark", s.watermark, "late_by", s.watermark.Sub(at)-s.lateWindow)
		return false
	}
	if at.After(s.watermark) {
		s.watermark = at
		watermarkSeconds.Set(float64(at.Unix()))
	}
	return true
}

// prune forgets detections older than maxAge behind the watermark; they can
// no longer join or change a fire.
func (s *Stream) prune(detections []domain.Detection) []domain.Detection {
	if s.watermark.IsZero() {
		return detections
	}
	cutoff := s.watermark.Add(-s.maxAge)
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
