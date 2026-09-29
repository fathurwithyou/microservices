// Package outbox publishes persisted hazard events to the message broker
// using a transactional outbox with lease-based claiming and exponential
// retry backoff.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/ports"
)

// ErrInvalidConfig is returned by NewWorker for missing required
// configuration.
var ErrInvalidConfig = errors.New("outbox: invalid config")

// Default operational knobs; overridable through WorkerConfig.
const (
	DefaultBatchSize     = 100
	DefaultLeaseDuration = 30 * time.Second
	DefaultRetryBase     = 500 * time.Millisecond
	DefaultRetryMaximum  = 30 * time.Second
)

// Worker dispatches pending outbox messages to the publisher.
type Worker struct {
	repository    Repository
	publisher     ports.Publisher
	interval      time.Duration
	batchSize     int
	leaseDuration time.Duration
	retryBase     time.Duration
	retryMaximum  time.Duration
	logger        *slog.Logger
}

// WorkerConfig configures a Worker.
type WorkerConfig struct {
	// Repository claims and acknowledges outbox messages. Required.
	Repository Repository
	// Publisher sends messages to the broker. Required.
	Publisher ports.Publisher
	// Interval between dispatch sweeps. Must be positive.
	Interval time.Duration
	// BatchSize messages claimed per sweep. Defaults to DefaultBatchSize.
	BatchSize int
	// LeaseDuration guards claimed messages from concurrent workers.
	// Defaults to DefaultLeaseDuration.
	LeaseDuration time.Duration
	// RetryBase is the first exponential retry delay. Defaults to DefaultRetryBase.
	RetryBase time.Duration
	// RetryMaximum caps the retry delay. Defaults to DefaultRetryMaximum.
	RetryMaximum time.Duration
	// Logger receives structured logs. Defaults to slog.Default().
	Logger *slog.Logger
}

// NewWorker validates cfg, applies defaults, and returns a Worker.
func NewWorker(cfg WorkerConfig) (*Worker, error) {
	if cfg.Repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidConfig)
	}
	if cfg.Publisher == nil {
		return nil, fmt.Errorf("%w: publisher is required", ErrInvalidConfig)
	}
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("%w: dispatch interval must be positive", ErrInvalidConfig)
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = DefaultLeaseDuration
	}
	if cfg.RetryBase <= 0 {
		cfg.RetryBase = DefaultRetryBase
	}
	if cfg.RetryMaximum <= 0 {
		cfg.RetryMaximum = DefaultRetryMaximum
	}
	if cfg.RetryMaximum < cfg.RetryBase {
		cfg.RetryMaximum = cfg.RetryBase
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Worker{repository: cfg.Repository, publisher: cfg.Publisher, interval: cfg.Interval, batchSize: cfg.BatchSize, leaseDuration: cfg.LeaseDuration, retryBase: cfg.RetryBase, retryMaximum: cfg.RetryMaximum, logger: cfg.Logger}, nil
}

// Run dispatches pending messages until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		w.dispatchPending(ctx)
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) dispatchPending(ctx context.Context) {
	messages, err := w.repository.ClaimPendingOutbox(ctx, w.batchSize, w.leaseDuration)
	if err != nil {
		w.logger.Error("outbox claim failed", "component", "outbox", "error", err)
		return
	}
	for _, message := range messages {
		if err := w.publisher.Publish(ctx, message); err != nil {
			w.logger.Error("outbox publish failed", "component", "outbox", "message_id", message.ID, "event_id", message.HazardID, "error", err)
			nextAttempt := time.Now().UTC().Add(w.retryDelay(message.Attempts))
			if releaseErr := w.repository.ReleaseOutbox(ctx, message.ID, message.LeaseToken, nextAttempt); releaseErr != nil {
				w.logger.Error("outbox lease release failed", "component", "outbox", "message_id", message.ID, "error", releaseErr)
			}
			continue
		}
		if err := w.repository.MarkOutboxPublished(ctx, message.ID, message.LeaseToken); err != nil {
			w.logger.Error("outbox publish acknowledgement failed", "component", "outbox", "message_id", message.ID, "error", err)
		}
	}
}

// retryDelay returns the exponential backoff for the given attempt count,
// capped at the configured maximum.
func (w *Worker) retryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	exponent := min(attempts-1, 6)
	delay := w.retryBase * time.Duration(1<<exponent)
	return min(delay, w.retryMaximum)
}
