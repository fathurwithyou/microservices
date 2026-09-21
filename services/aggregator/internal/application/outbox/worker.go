package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/ports"
)

type Worker struct {
	store     Store
	publisher ports.Publisher
	interval  time.Duration
	logger    *slog.Logger
}

const (
	outboxBatchSize     = 100
	outboxLeaseDuration = 30 * time.Second
	outboxRetryBase     = 500 * time.Millisecond
	outboxRetryMaximum  = 30 * time.Second
)

func NewWorker(store Store, publisher ports.Publisher, interval time.Duration, logger *slog.Logger) *Worker {
	return &Worker{store: store, publisher: publisher, interval: interval, logger: logger}
}

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
	messages, err := w.store.ClaimPendingOutbox(ctx, outboxBatchSize, outboxLeaseDuration)
	if err != nil {
		w.logger.Error("outbox claim failed", "component", "outbox", "error", err)
		return
	}
	for _, message := range messages {
		if err := w.publisher.Publish(ctx, message); err != nil {
			w.logger.Error("outbox publish failed", "component", "outbox", "message_id", message.ID, "event_id", message.HazardID, "error", err)
			nextAttempt := time.Now().UTC().Add(retryDelay(message.Attempts))
			if releaseErr := w.store.ReleaseOutbox(ctx, message.ID, message.LeaseToken, nextAttempt); releaseErr != nil {
				w.logger.Error("outbox lease release failed", "component", "outbox", "message_id", message.ID, "error", releaseErr)
			}
			continue
		}
		if err := w.store.MarkOutboxPublished(ctx, message.ID, message.LeaseToken); err != nil {
			w.logger.Error("outbox publish acknowledgement failed", "component", "outbox", "message_id", message.ID, "error", err)
		}
	}
}

func retryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	exponent := attempts - 1
	if exponent > 6 {
		exponent = 6
	}
	delay := outboxRetryBase * time.Duration(1<<exponent)
	if delay > outboxRetryMaximum {
		return outboxRetryMaximum
	}
	return delay
}
