package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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
	// LateWindow is the accepted lateness in event time.
	LateWindow time.Duration
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// SessionInterval is how often the feed session is polled for lag and
	// session changes.
	SessionInterval time.Duration
	// Leadership gates the stream to one active replica.
	Leadership Leadership
	// Sleep is an optional test seam; production uses a context-aware timer.
	Sleep func(ctx context.Context, delay time.Duration) error
}

// Stream consumes the feed, maintains fires, and commits the feed offset
// together with the state it produced.
//
// Delivery: the offset is committed in the same transaction as the working
// set, after the Aggregator accepted the fires of the batch. A crash before
// the commit replays the batch from the previous offset against the previous
// state, which yields the same fires with the same identifiers, and the
// Aggregator upserts by identifier. That is at-least-once delivery with
// idempotent effects: the stored result equals an uninterrupted run.
type Stream struct {
	feed            ports.Feed
	state           ports.State
	sink            ports.HazardSink
	logger          *slog.Logger
	lateWindow      time.Duration
	minBackoff      time.Duration
	maxBackoff      time.Duration
	sleep           func(ctx context.Context, delay time.Duration) error
	sessionInterval time.Duration
	leadership      Leadership

	mu     sync.Mutex
	status Status
	// cancelAttempt ends the current connection (session change).
	cancelAttempt context.CancelFunc
}

// Status is the reportable state of the processor, served as JSON.
type Status struct {
	Leader          bool             `json:"leader"`
	Connected       bool             `json:"connected"`
	Session         string           `json:"session"`
	Speed           float64          `json:"speed"`
	CommittedOffset int64            `json:"committed_offset"`
	LatestOffset    int64            `json:"latest_offset"`
	LagEvents       int64            `json:"lag_events"`
	Watermark       string           `json:"watermark,omitempty"`
	VirtualNow      string           `json:"virtual_now,omitempty"`
	LateWindow      string           `json:"accepted_lateness"`
	Working         Stats            `json:"working_set"`
	Counters        map[string]int64 `json:"cumulative"`
	LastError       string           `json:"last_error,omitempty"`
	LastCommitAt    string           `json:"last_commit_at,omitempty"`
}

// NewStream builds a consume loop.
func NewStream(cfg StreamConfig) *Stream {
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
		minBackoff:      cfg.MinBackoff,
		maxBackoff:      cfg.MaxBackoff,
		sleep:           sleep,
		sessionInterval: sessionInterval,
		leadership:      cfg.Leadership,
		status:          Status{LateWindow: cfg.LateWindow.String(), Counters: map[string]int64{}},
	}
}

const (
	// batchBuffer bounds events waiting for the processor; a full buffer
	// stops reading the socket, which is the backpressure towards the feed.
	batchBuffer = 4096
	// maxBatchSize bounds one commit while catching up.
	maxBatchSize = 512
	// horizonMargin is how far ahead of the feed's own clock an acquisition
	// time may be before it is treated as corrupt.
	horizonMargin = 24 * time.Hour
)

// ErrNotLeader stops the current stream when the lease is lost so the next
// replica can take over.
var ErrNotLeader = errors.New("not leader")

// Snapshot returns the current status.
func (s *Stream) Snapshot() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := s.status
	status.Counters = make(map[string]int64, len(s.status.Counters))
	for name, value := range s.status.Counters {
		status.Counters[name] = value
	}
	status.Leader = s.leadership == nil || s.leadership.Allowed()
	return status
}

// Run reconnects from the last committed offset until ctx is cancelled.
func (s *Stream) Run(ctx context.Context) error {
	go s.watchSession(ctx)
	attempt := 0
	for {
		if !s.waitLeader(ctx) {
			return nil
		}
		progressed, cause, err := s.attempt(ctx)
		if ctx.Err() != nil {
			return nil
		}
		reconnects.WithLabelValues(cause).Inc()
		if errors.Is(err, ErrNotLeader) {
			attempt = 0
			continue
		}
		s.setError(err)
		s.logger.Warn("feed stream ended",
			"service", "satellite-ingestor", "component", "stream",
			"cause", cause, "committed_offset", s.Snapshot().CommittedOffset, "error", err)
		if progressed {
			attempt = 0
		}
		delay := s.backoff(attempt)
		attempt++
		if err := s.sleep(ctx, delay); err != nil {
			return nil
		}
	}
}

// attempt runs one connection: load state, resume the stream, process until
// it ends. Memory is rebuilt from durable state every time, so nothing a
// failed batch did in memory can leak into the next one.
func (s *Stream) attempt(ctx context.Context) (progressed bool, cause string, err error) {
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.mu.Lock()
	s.cancelAttempt = cancel
	s.mu.Unlock()

	snapshot, err := s.state.Load(attemptCtx)
	if err != nil {
		return false, "error", fmt.Errorf("load state: %w", err)
	}
	session, sessionErr := s.feed.Session(attemptCtx)
	if sessionErr == nil {
		id := sessionID(session)
		if snapshot.Session != id || session.LatestOffset < snapshot.Offset {
			if snapshot.Session != "" || snapshot.Offset > 0 {
				s.logger.Warn("feed session changed, starting over",
					"service", "satellite-ingestor", "component", "stream",
					"previous_session", snapshot.Session, "session", id,
					"committed_offset", snapshot.Offset, "latest_offset", session.LatestOffset)
				sessionResets.Inc()
			}
			if err := s.state.Reset(attemptCtx, id); err != nil {
				return false, "error", fmt.Errorf("reset state for session %s: %w", id, err)
			}
			if snapshot, err = s.state.Load(attemptCtx); err != nil {
				return false, "error", fmt.Errorf("load state: %w", err)
			}
		}
	}

	run := &run{
		stream:    s,
		engine:    NewEngine(s.lateWindow),
		committed: snapshot.Offset,
		counters:  map[string]int64{},
		session:   snapshot.Session,
	}
	for name, value := range snapshot.Counters {
		run.counters[name] = value
	}
	run.engine.SetFinalized(run.counters["closed_fires"], run.counters["closed_singles"])
	run.engine.Restore(snapshot.Detections, snapshot.Fires, snapshot.Watermark)
	if sessionErr == nil && !session.VirtualNow.IsZero() {
		run.engine.SetHorizon(session.VirtualNow.Add(horizonMargin))
	}
	s.publish(run, "")
	if sessionErr == nil {
		s.observeSession(session)
	}

	events := make(chan ports.Event, batchBuffer)
	results := make(chan error, 1)
	go func() {
		err := run.consume(attemptCtx, events)
		// Stop the reader when processing fails.
		cancel()
		results <- err
	}()
	handler := func(streamCtx context.Context, event ports.Event) error {
		select {
		case events <- event:
			return nil
		case <-streamCtx.Done():
			return streamCtx.Err()
		}
	}

	s.setConnected(true)
	streamErr := s.feed.Stream(attemptCtx, snapshot.Offset, handler)
	s.setConnected(false)
	// Let the processor drain what was already read before stopping it.
	close(events)
	processErr := <-results
	progressed = run.committed > snapshot.Offset

	switch {
	case errors.Is(processErr, ErrNotLeader):
		return progressed, "not_leader", ErrNotLeader
	case processErr != nil:
		return progressed, "process", processErr
	case streamErr == nil:
		return progressed, "closed", nil
	}
	return progressed, causeOf(streamErr), streamErr
}

func causeOf(err error) string {
	message := err.Error()
	switch {
	case strings.Contains(message, "feed status "):
		rest := message[strings.Index(message, "feed status ")+len("feed status "):]
		if len(rest) >= 3 {
			return "status_" + rest[:3]
		}
		return "status"
	case strings.Contains(message, "feed idle"):
		return "idle"
	default:
		return "error"
	}
}

func sessionID(session ports.Session) string {
	return session.Group + "/" + session.Profile
}

// run is the in-memory side of one connection attempt.
type run struct {
	stream    *Stream
	engine    *Engine
	committed int64
	counters  map[string]int64
	session   string
}

// consume drains bursts so one commit covers many events.
func (r *run) consume(ctx context.Context, events <-chan ports.Event) error {
	for {
		event, open := <-events
		if !open {
			return nil
		}
		batch := []ports.Event{event}
	drain:
		for len(batch) < maxBatchSize {
			select {
			case next, more := <-events:
				if !more {
					break drain
				}
				batch = append(batch, next)
			default:
				break drain
			}
		}
		if ctx.Err() != nil {
			// The connection is gone; whatever was not committed is read
			// again after the reconnect.
			return nil
		}
		if err := r.process(ctx, batch); err != nil {
			return err
		}
	}
}

// process applies a batch event by event, sends the changed fires, then
// commits state and offset in one transaction.
func (r *run) process(ctx context.Context, events []ports.Event) error {
	s := r.stream
	if !s.stillLeader(ctx) {
		return ErrNotLeader
	}
	var (
		late      []ports.LateDetection
		maxOffset = r.committed
		applied   int
	)
	count := func(name string) { r.counters[name]++ }
	for _, event := range events {
		if event.Offset > 0 {
			if event.Offset <= maxOffset {
				// Replayed by the server (inclusive "from") or by a retry.
				skippedEvents.WithLabelValues("duplicate_offset").Inc()
				continue
			}
			if event.Offset != maxOffset+1 {
				offsetGaps.Inc()
				count("offset_gaps")
				s.logger.Error("feed offset gap",
					"service", "satellite-ingestor", "component", "stream",
					"expected", maxOffset+1, "received", event.Offset)
			}
			maxOffset = event.Offset
		}
		if event.Skipped != "" {
			skippedEvents.WithLabelValues(event.Skipped).Inc()
			count("skipped_" + event.Skipped)
			s.logger.Warn("feed event skipped",
				"service", "satellite-ingestor", "component", "stream",
				"offset", event.Offset, "reason", event.Skipped)
			continue
		}
		eventsReceived.WithLabelValues(string(event.Kind)).Inc()
		var outcome Outcome
		switch event.Kind {
		case ports.KindDetection, ports.KindUpdate:
			outcome = r.engine.Upsert(event.Detection, event.Offset)
		case ports.KindRetract:
			outcome = r.engine.Retract(event.DetectionKey)
		default:
			continue
		}
		applied++
		eventOutcomes.WithLabelValues(string(outcome)).Inc()
		switch outcome {
		case OutcomeLate:
			// Past the accepted lateness: not clustered, but recorded with
			// its key so the count can be reported and inspected.
			at, _ := event.Detection.Time()
			late = append(late, ports.LateDetection{Key: event.Detection.Key(), Offset: event.Offset, AcquiredAt: at, Watermark: r.engine.Watermark()})
			droppedDetections.WithLabelValues("late").Inc()
			count("late")
			s.logger.Warn("late detection not clustered",
				"service", "satellite-ingestor", "component", "stream",
				"detection", event.Detection.Key(), "offset", event.Offset,
				"acquired_at", at, "watermark", r.engine.Watermark(), "late_by", (r.engine.Watermark().Sub(at) - s.lateWindow).String())
		case OutcomeLow:
			droppedDetections.WithLabelValues("low_confidence").Inc()
			count("low_confidence")
		case OutcomeInvalidTime:
			droppedDetections.WithLabelValues("invalid_time").Inc()
			count("invalid_time")
		case OutcomeFuture:
			droppedDetections.WithLabelValues("future").Inc()
			count("future")
		case OutcomeUnknownKey:
			count("retract_unknown")
		case OutcomeDuplicate:
			count("duplicates")
		}
	}
	if maxOffset == r.committed && applied == 0 {
		return nil
	}

	r.engine.Finalize()
	delta := r.engine.Drain()
	stats := r.engine.Stats()
	r.counters["closed_fires"] = stats.FinalizedFires
	r.counters["closed_singles"] = stats.FinalizedSingles

	// Send before committing: a failed send leaves the offset where it was,
	// and the batch is replayed from durable state.
	if len(delta.Fires) > 0 {
		if !s.stillLeader(ctx) {
			return ErrNotLeader
		}
		if err := s.sink.Submit(ctx, delta.Fires); err != nil {
			return fmt.Errorf("submit fires: %w", err)
		}
		for _, fire := range delta.Fires {
			firesSubmitted.WithLabelValues(fire.Status).Inc()
		}
	}
	counters := make(map[string]int64, len(r.counters))
	for name, value := range r.counters {
		counters[name] = value
	}
	if err := s.state.Commit(ctx, ports.Batch{
		Offset:           maxOffset,
		Watermark:        r.engine.Watermark(),
		UpsertDetections: delta.UpsertDetections,
		DeleteDetections: delta.DeleteDetections,
		Fires:            append(append([]domain.Fire{}, delta.Fires...), delta.Closed...),
		Late:             late,
		Counters:         counters,
	}); err != nil {
		return fmt.Errorf("commit batch at offset %d: %w", maxOffset, err)
	}
	r.committed = maxOffset

	now := time.Now()
	for _, event := range events {
		if event.Offset > 0 && !event.ReceivedAt.IsZero() && event.Skipped == "" && event.Kind != ports.KindHeartbeat {
			eventToStore.Observe(now.Sub(event.ReceivedAt).Seconds())
		}
	}
	batchSize.Observe(float64(len(events)))
	s.publish(r, now.UTC().Format(time.RFC3339Nano))
	return nil
}

// publish copies the run's numbers into the status and the gauges.
func (s *Stream) publish(r *run, committedAt string) {
	stats := r.engine.Stats()
	watermark := r.engine.Watermark()
	committedOffset.Set(float64(r.committed))
	activeFires.Set(float64(stats.ActiveFires))
	singleDetections.Set(float64(stats.Singles))
	liveDetections.Set(float64(stats.Detections))
	if !watermark.IsZero() {
		watermarkSeconds.Set(float64(watermark.Unix()))
	}
	for name, value := range r.counters {
		cumulative.WithLabelValues(name).Set(float64(value))
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.status.CommittedOffset = r.committed
	s.status.Session = r.session
	s.status.Working = stats
	if !watermark.IsZero() {
		s.status.Watermark = watermark.UTC().Format(time.RFC3339)
	}
	s.status.Counters = make(map[string]int64, len(r.counters))
	for name, value := range r.counters {
		s.status.Counters[name] = value
	}
	if committedAt != "" {
		s.status.LastCommitAt = committedAt
		s.status.LastError = ""
	}
	lag := s.status.LatestOffset - r.committed
	if lag < 0 {
		lag = 0
	}
	s.status.LagEvents = lag
	feedLag.Set(float64(lag))
}

func (s *Stream) setConnected(value bool) {
	s.mu.Lock()
	s.status.Connected = value
	s.mu.Unlock()
	if value {
		streamConnected.Set(1)
	} else {
		streamConnected.Set(0)
	}
}

func (s *Stream) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		return
	}
	s.status.LastError = err.Error()
}

// observeSession records what the feed says about itself and reports whether
// the session differs from the one being consumed.
func (s *Stream) observeSession(session ports.Session) (changed bool) {
	latestOffset.Set(float64(session.LatestOffset))
	feedSpeed.Set(session.Speed)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status.LatestOffset = session.LatestOffset
	s.status.Speed = session.Speed
	if !session.VirtualNow.IsZero() {
		s.status.VirtualNow = session.VirtualNow.Format(time.RFC3339)
	}
	lag := session.LatestOffset - s.status.CommittedOffset
	if lag < 0 {
		lag = 0
	}
	s.status.LagEvents = lag
	feedLag.Set(float64(lag))
	return s.status.Connected && s.status.Session != "" &&
		(s.status.Session != sessionID(session) || session.LatestOffset < s.status.CommittedOffset)
}

// watchSession polls the feed session for lag and ends the connection when
// the feed started a new session, so the next attempt starts over.
func (s *Stream) watchSession(ctx context.Context) {
	ticker := time.NewTicker(s.sessionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		session, err := s.feed.Session(ctx)
		if err != nil {
			continue
		}
		if s.observeSession(session) {
			s.mu.Lock()
			cancel := s.cancelAttempt
			s.mu.Unlock()
			if cancel != nil {
				cancel()
			}
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
