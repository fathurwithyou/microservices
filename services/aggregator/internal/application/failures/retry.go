package failures

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

// AttemptRecorder records one failed automatic retry. When park is true the
// message leaves the automatic retry queue and waits for an operator replay.
type AttemptRecorder interface {
	RecordIngestFailureAttempt(ctx context.Context, messageID, reason string, retryable, park bool) error
}

// RetryRepository is what the automatic retry loop needs from storage.
type RetryRepository interface {
	Repository
	AttemptRecorder
}

// RetryResult summarises one automatic retry pass.
type RetryResult struct {
	Recovered int
	Retried   int
	Parked    int
}

// RetryPending retries every message with status retrying, oldest first.
// Parked messages are never touched here: they need a system change and an
// explicit Ops replay. A retrying message whose mapping now fails with a
// schema violation is parked immediately; one that keeps failing for a
// transient reason is parked after maxAttempts.
func (r *Replayer) RetryPending(ctx context.Context, recorder AttemptRecorder, maxAttempts int) (RetryResult, error) {
	return r.retryPending(ctx, recorder, maxAttempts, nil)
}

// retryPending is RetryPending with a fence checked before every message, so
// a replica that lost its lease mid-pass (for example after a freeze) stops
// before its next write instead of finishing the pass.
func (r *Replayer) retryPending(ctx context.Context, recorder AttemptRecorder, maxAttempts int, fence func(context.Context) bool) (RetryResult, error) {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	r.refreshReferences(ctx)
	var result RetryResult
	err := r.walk(ctx, domain.FailureFilter{Status: domain.FailureRetrying}, func(item domain.IngestFailure) (bool, error) {
		if fence != nil && !fence(ctx) {
			r.logger.Warn("retry pass stopped: leadership lost", "component", "failures")
			return false, nil
		}
		return true, r.retryOne(ctx, recorder, item, maxAttempts, &result)
	})
	return result, err
}

func (r *Replayer) retryOne(ctx context.Context, recorder AttemptRecorder, item domain.IngestFailure, maxAttempts int, result *RetryResult) error {
	event, mapErr := r.mapPayload(item)
	if mapErr == nil {
		if _, err := r.submitter.Submit(ctx, event, item.MessageID); err != nil {
			// Storage trouble is transient by definition; count the attempt.
			mapErr = fmt.Errorf("submit: %w", err)
		} else {
			if err := r.repository.MarkIngestFailureRecovered(ctx, item.MessageID); err != nil {
				return fmt.Errorf("retry %s: mark recovered: %w", item.MessageID, err)
			}
			result.Recovered++
			r.logger.Info("retry recovered message", "component", "failures", "message_id", item.MessageID, "source", item.Source, "attempts", item.Attempts+1)
			return nil
		}
	}
	permanent := errors.Is(mapErr, domain.ErrInvalidSourcePayload)
	park := permanent || item.Attempts+1 >= maxAttempts
	if err := recorder.RecordIngestFailureAttempt(ctx, item.MessageID, mapErr.Error(), !permanent, park); err != nil {
		return fmt.Errorf("retry %s: record attempt: %w", item.MessageID, err)
	}
	if park {
		result.Parked++
		r.logger.Warn("retry exhausted, message parked", "component", "failures", "message_id", item.MessageID, "source", item.Source, "attempts", item.Attempts+1, "permanent", permanent, "reason", mapErr.Error())
		return nil
	}
	result.Retried++
	r.logger.Info("retry failed, will retry", "component", "failures", "message_id", item.MessageID, "source", item.Source, "attempts", item.Attempts+1, "reason", mapErr.Error())
	return nil
}

// RetryWorker periodically retries transient ingest failures.
type RetryWorker struct {
	replayer    *Replayer
	recorder    AttemptRecorder
	interval    time.Duration
	maxAttempts int
	allowed     func(context.Context) bool
	logger      *slog.Logger
}

// RetryWorkerConfig configures a RetryWorker.
type RetryWorkerConfig struct {
	Replayer *Replayer
	Recorder AttemptRecorder
	// Interval between passes. Defaults to 30s.
	Interval time.Duration
	// MaxAttempts before a transient failure is parked. Defaults to 5.
	MaxAttempts int
	// Allowed fences the worker, normally on ingestion leadership revalidated
	// against the database clock, so only one aggregator replica retries at a
	// time. It is checked before each pass and before each message. Nil means
	// always allowed.
	Allowed func(context.Context) bool
	Logger  *slog.Logger
}

// NewRetryWorker validates cfg and applies defaults.
func NewRetryWorker(cfg RetryWorkerConfig) (*RetryWorker, error) {
	if cfg.Replayer == nil || cfg.Recorder == nil {
		return nil, errors.New("failures: replayer and recorder are required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.Allowed == nil {
		cfg.Allowed = func(context.Context) bool { return true }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &RetryWorker{replayer: cfg.Replayer, recorder: cfg.Recorder, interval: cfg.Interval, maxAttempts: cfg.MaxAttempts, allowed: cfg.Allowed, logger: cfg.Logger}, nil
}

// Run retries pending failures until ctx is cancelled.
func (w *RetryWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
		if !w.allowed(ctx) {
			continue
		}
		result, err := w.replayer.retryPending(ctx, w.recorder, w.maxAttempts, w.allowed)
		if err != nil {
			w.logger.Error("retry pass failed", "component", "failures", "error", err)
			continue
		}
		if result != (RetryResult{}) {
			w.logger.Info("retry pass finished", "component", "failures", "recovered", result.Recovered, "retried", result.Retried, "parked", result.Parked)
		}
	}
}
