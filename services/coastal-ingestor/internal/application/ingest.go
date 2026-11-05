package application

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/ports"
)

// IngestConfig tunes the polling loop.
type IngestConfig struct {
	// Interval is the gap between cycles.
	Interval time.Duration
}

// IngestResult is the outcome of one poll cycle.
type IngestResult struct {
	// Readings are the accepted readings, or the last good batch when Stale.
	Readings []domain.Reading
	// Stale reports that the source was skipped or failed and cached
	// readings were returned instead.
	Stale bool
	// Quarantined is the number of readings rejected this cycle.
	Quarantined int
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
	if validator == nil {
		validator = NewValidator(DefaultValidatorConfig(), now, nil)
	}
	if breaker == nil {
		breaker = NewCircuitBreaker(DefaultCircuitConfig(), now)
	}
	if logger == nil {
		logger = slog.Default()
	}
	if now == nil {
		now = time.Now
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

// Cycle performs one fetch, validate, submit, and cache pass. The last good
// cache advances only after the sink accepts the batch.
func (i *Ingestor) Cycle(ctx context.Context) (IngestResult, error) {
	if !i.breaker.Allow() {
		return i.stale(ctx)
	}

	readings, err := i.source.Fetch(ctx)
	if err != nil {
		i.breaker.RecordFailure()
		i.logger.Warn("source fetch failed", "component", "ingest", "error", err, "circuit", i.breaker.State())
		return i.stale(ctx)
	}
	i.breaker.RecordSuccess()

	now := i.now()
	var accepted []domain.Reading
	var records []domain.QuarantineRecord
	for _, reading := range readings {
		reason, rejected := i.validator.Validate(reading)
		if !rejected {
			accepted = append(accepted, reading)
			continue
		}
		receivedAt := reading.ReceivedAt
		if receivedAt.IsZero() {
			receivedAt = now
		}
		records = append(records, domain.QuarantineRecord{
			MessageID:  reading.ID,
			Source:     domain.SourceCoastalSensor,
			ReceivedAt: receivedAt,
			Reason:     reason,
			Retryable:  reason.Retryable(),
			Payload:    reading.Payload,
		})
		readingsQuarantined.WithLabelValues(string(reason)).Inc()
	}

	if len(records) > 0 {
		if err := i.state.SaveQuarantine(ctx, records); err != nil {
			return IngestResult{}, fmt.Errorf("save quarantine: %w", err)
		}
	}

	if len(accepted) > 0 {
		if err := i.sink.Submit(ctx, accepted); err != nil {
			return IngestResult{}, fmt.Errorf("submit readings: %w", err)
		}
		readingsAccepted.Add(float64(len(accepted)))
		for _, reading := range accepted {
			if reading.Critical {
				criticalReadings.Inc()
			}
		}
		if err := i.state.SaveLastGood(ctx, accepted, now); err != nil {
			return IngestResult{}, fmt.Errorf("save last good: %w", err)
		}
	}

	return IngestResult{Readings: accepted, Quarantined: len(records)}, nil
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
