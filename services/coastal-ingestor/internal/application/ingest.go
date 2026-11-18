package application

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/ports"
)

// IngestConfig tunes the polling loop.
type IngestConfig struct {
	// Interval is the gap between cycles.
	Interval time.Duration
	// SubmitConcurrency is how many readings are submitted to the Aggregator
	// at once. Order between different observations does not matter: the
	// Aggregator upserts by observation ID.
	SubmitConcurrency int
	// SubmitBudget is how long one cycle may spend submitting non-critical
	// readings before it goes back to the sensor. It bounds how long a new
	// critical reading waits behind a backlog of ordinary ones.
	SubmitBudget time.Duration
	// MaxBacklog is the number of fetched, not yet stored non-critical
	// readings above which the cycle stops fetching (memory bound).
	MaxBacklog int
	// StaleAfter is the feed freshness (sensor clock minus newest
	// observed_at, in sensor time) above which the feed is reported stale.
	StaleAfter time.Duration
}

// Defaults for IngestConfig fields left at zero.
const (
	DefaultSubmitConcurrency = 8
	DefaultSubmitBudget      = time.Second
	DefaultMaxBacklog        = 20_000
	DefaultStaleAfter        = 30 * time.Minute
)

// IngestResult is the outcome of one poll cycle.
type IngestResult struct {
	// Readings are the readings accepted from this cycle's fetch, or the
	// last good batch when Stale.
	Readings []domain.Reading
	// Stale reports that the source was skipped or failed and cached
	// readings were returned instead.
	Stale bool
	// Quarantined is the number of readings rejected this cycle.
	Quarantined int
}

// batch is the outcome of one fetch on its way to durable state. A batch is
// committed (last-good and cursor saved) only when nothing in it is left to
// store, so a restart or a new leader resumes from a cursor behind which
// every reading is known to be in the Aggregator or in quarantine.
type batch struct {
	// critical and rest are the accepted readings not yet stored.
	critical []domain.Reading
	rest     []domain.Reading
	// records are the quarantine records not yet saved.
	records []domain.QuarantineRecord
	// accepted is every accepted reading of the fetch, for the last-good cache.
	accepted []domain.Reading
	// cursor is the source position after this fetch.
	cursor    string
	hasCursor bool
}

// Ingestor polls the source, validates readings, and forwards accepted ones.
// It never blocks on a failing source: the breaker and stale cache keep the
// cycle cheap while upstream recovers.
type Ingestor struct {
	cfg       IngestConfig
	source    ports.Source
	state     ports.State
	sink      ports.HazardSink
	validator *Validator
	breaker   *CircuitBreaker
	logger    *slog.Logger
	now       Clock

	leadership Leadership
	// leading is true while this instance holds the lease and has resynced
	// its in-memory cursor from durable state.
	leading bool

	// queue holds fetched batches that are not fully stored yet, oldest
	// first. Only the Run goroutine touches it.
	queue []*batch
	// feedStale and feedWarnedAt throttle the stale-feed warning.
	feedStale    bool
	feedWarnedAt time.Time
}

// Leadership gates polling to one active replica. Allowed is a cheap local
// check; StillLeader revalidates against the database clock before writes.
type Leadership interface {
	Allowed() bool
	StillLeader(ctx context.Context) (bool, error)
}

// WithLeadership enables active/standby mode: a standby replica neither polls
// the sensor nor writes state, so the sensor sees one client however many
// replicas run.
func (i *Ingestor) WithLeadership(leadership Leadership) *Ingestor {
	i.leadership = leadership
	return i
}

// NewIngestor wires the poll loop. Nil collaborators fall back to safe
// defaults so tests only supply what they exercise.
func NewIngestor(
	cfg IngestConfig,
	source ports.Source,
	state ports.State,
	sink ports.HazardSink,
	validator *Validator,
	breaker *CircuitBreaker,
	logger *slog.Logger,
	now Clock,
) *Ingestor {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.SubmitConcurrency <= 0 {
		cfg.SubmitConcurrency = DefaultSubmitConcurrency
	}
	if cfg.SubmitBudget <= 0 {
		cfg.SubmitBudget = DefaultSubmitBudget
	}
	if cfg.MaxBacklog <= 0 {
		cfg.MaxBacklog = DefaultMaxBacklog
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = DefaultStaleAfter
	}
	if now == nil {
		now = time.Now
	}
	if validator == nil {
		validator = NewValidator(DefaultValidatorConfig(), nil)
	}
	if breaker == nil {
		breaker = NewCircuitBreaker(DefaultCircuitConfig(), now)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Ingestor{
		cfg:       cfg,
		source:    source,
		state:     state,
		sink:      sink,
		validator: validator,
		breaker:   breaker,
		logger:    logger,
		now:       now,
	}
}

// Run polls until the context is cancelled. Cycle errors are logged and
// retried on the next tick rather than stopping the service.
func (i *Ingestor) Run(ctx context.Context) {
	ticker := time.NewTicker(i.cfg.Interval)
	defer ticker.Stop()

	for {
		result, err := i.Cycle(ctx)
		switch {
		case err != nil:
			i.logger.Error("ingest cycle failed", "component", "ingest", "error", err)
		case result.Quarantined > 0:
			i.logger.Warn("readings quarantined", "component", "ingest", "count", result.Quarantined)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Cycle performs one fetch, validate, submit, and commit pass.
//
// Critical lane. Within a cycle the readings flagged critical are submitted
// to the Aggregator FIRST, before any other work: before quarantine records
// are saved and before any ordinary reading is submitted. Ordinary readings
// follow with bounded concurrency and a time budget; what does not fit stays
// queued and the cycle returns to the sensor, so while the ingestor works
// through a backlog a new critical reading waits about one budget plus one
// poll interval instead of the whole backlog. Which readings count as
// critical-and-forwardable is the Validator's policy.
//
// Nothing is lost on a failed submit: the reading stays queued and is retried
// next cycle, and the cursor and last-good cache of a fetch are saved only
// after every reading of that fetch is stored.
func (i *Ingestor) Cycle(ctx context.Context) (IngestResult, error) {
	if i.leadership != nil {
		if !i.leadership.Allowed() {
			i.leading = false
			i.dropQueue()
			return IngestResult{}, nil
		}
		if !i.leading {
			// The previous leader advanced the cursor; continue from what it
			// committed instead of this replica's stale in-memory copy.
			i.dropQueue()
			if err := i.resyncCursor(ctx); err != nil {
				return IngestResult{}, err
			}
			i.leading = true
		}
	}

	var result IngestResult
	switch {
	case i.backlog() >= i.cfg.MaxBacklog:
		// Backpressure: the Aggregator is not keeping up. Stop reading ahead
		// until the queue drains rather than grow without bound.
	case !i.breaker.Allow():
		result.Stale = true
	default:
		readings, err := i.source.Fetch(ctx)
		if err != nil {
			i.breaker.RecordFailure()
			i.logger.Warn("source fetch failed", "component", "ingest", "error", err, "circuit", i.breaker.State(), "salvaged", len(readings))
			// A failed fetch may still carry the readings of the pages that
			// preceded the failure; they are processed below.
			result.Stale = len(readings) == 0
		} else {
			i.breaker.RecordSuccess()
			i.observeFeed()
		}
		if err == nil || len(readings) > 0 {
			fetched := i.classify(readings)
			if last := len(i.queue) - 1; last >= 0 && len(fetched.accepted) == 0 && len(fetched.records) == 0 {
				// Nothing new: fold the cursor into the batch already waiting
				// instead of queueing an empty one per poll while it is stuck.
				i.queue[last].cursor, i.queue[last].hasCursor = fetched.cursor, fetched.hasCursor
			} else {
				i.queue = append(i.queue, fetched)
			}
			result.Readings = fetched.accepted
			result.Quarantined = len(fetched.records)
		}
	}

	if len(i.queue) > 0 {
		// Fence every write: a replica frozen past its lease TTL must not
		// commit a batch or a cursor after another replica took over.
		if i.leadership != nil {
			still, err := i.leadership.StillLeader(ctx)
			if err != nil || !still {
				i.leading = false
				i.logger.Warn("leadership lost before write, batch dropped", "component", "ingest", "error", err, "readings", i.backlog())
				i.dropQueue()
				return IngestResult{}, nil
			}
		}
		if err := i.drain(ctx); err != nil {
			return IngestResult{}, err
		}
	}
	if result.Stale {
		return i.stale(ctx)
	}
	return result, nil
}

// classify validates one fetch into a batch and accounts for the rejects.
func (i *Ingestor) classify(readings []domain.Reading) *batch {
	now := i.now()
	fetched := &batch{}
	if source, ok := i.source.(ports.CursorSource); ok {
		fetched.cursor, fetched.hasCursor = source.Cursor(), true
	}
	for _, reading := range readings {
		verdict := i.validator.Assess(reading)
		if !verdict.Rejected {
			if verdict.Note != "" {
				reading.Note = verdict.Note
				i.logger.Warn("critical reading accepted with reservations", "component", "ingest", "observation_id", reading.ID, "note", verdict.Note)
			}
			fetched.accepted = append(fetched.accepted, reading)
			if reading.Critical {
				fetched.critical = append(fetched.critical, reading)
			} else {
				fetched.rest = append(fetched.rest, reading)
			}
			continue
		}
		receivedAt := reading.ReceivedAt
		if receivedAt.IsZero() {
			receivedAt = now
		}
		fetched.records = append(fetched.records, domain.QuarantineRecord{
			MessageID:  reading.ID,
			Source:     domain.SourceCoastalSensor,
			ReceivedAt: receivedAt,
			Reason:     verdict.Reason,
			Retryable:  verdict.Reason.Retryable(),
			Payload:    reading.Payload,
		})
		readingsQuarantined.WithLabelValues(string(verdict.Reason)).Inc()
		if reading.Critical {
			// The one case in which a critical flag does not get through: no
			// usable identity to notify about. It must never pass silently.
			criticalQuarantined.WithLabelValues(string(verdict.Reason)).Inc()
			i.logger.Error("critical reading quarantined: identity broken, nobody is notified",
				"component", "ingest", "reason", verdict.Reason, "observation_id", reading.ID, "raw", logPayload(reading.Payload))
		}
	}
	return fetched
}

// drain stores what the queue holds, most urgent first.
func (i *Ingestor) drain(ctx context.Context) error {
	defer func() { submitBacklog.Set(float64(i.backlog())) }()

	// 1. Critical readings of every queued batch, newest fetch included.
	// Every one is attempted even when another fails.
	var firstErr error
	for _, queued := range i.queue {
		if len(queued.critical) == 0 {
			continue
		}
		remaining, err := i.submit(ctx, queued.critical, time.Time{})
		criticalReadings.Add(float64(len(queued.critical) - len(remaining)))
		queued.critical = remaining
		if err != nil {
			i.logger.Error("critical reading not stored yet, retrying next cycle", "component", "ingest", "error", err, "pending", len(remaining))
			if firstErr == nil {
				firstErr = fmt.Errorf("submit critical readings: %w", err)
			}
		}
	}
	if firstErr != nil {
		return firstErr
	}

	// 2. Everything else, oldest batch first so cursors commit in order.
	deadline := i.now().Add(i.cfg.SubmitBudget)
	for len(i.queue) > 0 {
		head := i.queue[0]
		if len(head.records) > 0 {
			if err := i.state.SaveQuarantine(ctx, head.records); err != nil {
				return fmt.Errorf("save quarantine: %w", err)
			}
			head.records = nil
		}
		if len(head.rest) > 0 {
			remaining, err := i.submit(ctx, head.rest, deadline)
			head.rest = remaining
			if err != nil {
				return fmt.Errorf("submit readings: %w", err)
			}
			if len(remaining) > 0 {
				// Budget spent: look at the sensor again before continuing.
				return nil
			}
		}
		if len(head.accepted) > 0 {
			if err := i.state.SaveLastGood(ctx, head.accepted, i.now()); err != nil {
				return fmt.Errorf("save last good: %w", err)
			}
			head.accepted = nil
		}
		if head.hasCursor {
			if err := i.state.SaveCursor(ctx, head.cursor); err != nil {
				return fmt.Errorf("save sensor cursor: %w", err)
			}
		}
		i.queue = i.queue[1:]
	}
	return nil
}

// submit sends readings to the sink one call each, SubmitConcurrency at a
// time, and returns the ones not stored (failed or not attempted) in their
// original order. A zero deadline means no time budget and no early stop:
// used for the critical lane, where every reading is attempted. With a
// deadline, no new call starts after it passes or after the first failure
// (a failing Aggregator is not worth thousands of further attempts).
func (i *Ingestor) submit(ctx context.Context, readings []domain.Reading, deadline time.Time) ([]domain.Reading, error) {
	budgeted := !deadline.IsZero()
	stored := make([]bool, len(readings))
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
		failed   atomic.Bool
	)
	slots := make(chan struct{}, i.cfg.SubmitConcurrency)
	for index := range readings {
		slots <- struct{}{}
		if ctx.Err() != nil || (budgeted && (failed.Load() || !i.now().Before(deadline))) {
			<-slots
			break
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			defer func() { <-slots }()
			if err := i.sink.Submit(ctx, readings[index:index+1:index+1]); err != nil {
				failed.Store(true)
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			stored[index] = true
		}(index)
	}
	wg.Wait()

	var remaining []domain.Reading
	count := 0
	for index, ok := range stored {
		if ok {
			count++
		} else {
			remaining = append(remaining, readings[index])
		}
	}
	readingsAccepted.Add(float64(count))
	if firstErr == nil && ctx.Err() != nil && len(remaining) > 0 {
		firstErr = ctx.Err()
	}
	return remaining, firstErr
}

// observeFeed publishes what the last fetch learned about the feed itself.
// "Healthy but hours behind" cannot be seen on any single reading; it shows
// as the sensor's clock running away from the newest observation it serves.
func (i *Ingestor) observeFeed() {
	source, ok := i.source.(ports.FeedStatusSource)
	if !ok {
		return
	}
	status := source.FeedStatus()
	if !status.GeneratedAt.IsZero() {
		sensorClock.Set(float64(status.GeneratedAt.Unix()))
	}
	if !status.AtTail || status.GeneratedAt.IsZero() || status.NewestObservedAt.IsZero() {
		return
	}
	freshness := status.GeneratedAt.Sub(status.NewestObservedAt)
	if freshness < 0 {
		freshness = 0
	}
	feedFreshness.Set(freshness.Seconds())
	stale := freshness > i.cfg.StaleAfter
	now := i.now()
	switch {
	case stale && (!i.feedStale || now.Sub(i.feedWarnedAt) >= time.Minute):
		i.feedWarnedAt = now
		i.logger.Warn("sensor answers but its newest data is old", "component", "ingest",
			"freshness_seconds", freshness.Seconds(), "stale_after_seconds", i.cfg.StaleAfter.Seconds(),
			"sensor_clock", status.GeneratedAt.UTC().Format(time.RFC3339), "newest_observed_at", status.NewestObservedAt.UTC().Format(time.RFC3339))
	case !stale && i.feedStale:
		i.logger.Info("sensor feed is fresh again", "component", "ingest", "freshness_seconds", freshness.Seconds())
	}
	i.feedStale = stale
}

// backlog is the number of accepted readings waiting to be stored.
func (i *Ingestor) backlog() int {
	total := 0
	for _, queued := range i.queue {
		total += len(queued.critical) + len(queued.rest)
	}
	return total
}

// dropQueue forgets fetched work. The durable cursor is still behind it, so
// whoever leads next reads it again.
func (i *Ingestor) dropQueue() {
	i.queue = nil
	submitBacklog.Set(0)
}

// logPayload bounds a raw sensor item for a log line.
func logPayload(payload []byte) string {
	const limit = 2048
	if len(payload) > limit {
		payload = payload[:limit]
	}
	return strings.ToValidUTF8(string(payload), "?")
}

func (i *Ingestor) resyncCursor(ctx context.Context) error {
	source, ok := i.source.(ports.CursorSource)
	if !ok {
		return nil
	}
	cursor, err := i.state.Cursor(ctx)
	if err != nil {
		return fmt.Errorf("resync sensor cursor: %w", err)
	}
	source.SetCursor(cursor)
	i.logger.Info("leader resynced sensor cursor", "component", "ingest", "cursor", cursor)
	return nil
}

// stale returns the cached batch so callers keep serving data while the
// source is unavailable.
func (i *Ingestor) stale(ctx context.Context) (IngestResult, error) {
	readings, capturedAt, err := i.state.LastGood(ctx)
	if err != nil {
		return IngestResult{}, fmt.Errorf("load last good readings: %w", err)
	}
	i.logger.Warn("serving stale readings", "component", "ingest", "count", len(readings), "captured_at", capturedAt)
	return IngestResult{Readings: readings, Stale: true}, nil
}
