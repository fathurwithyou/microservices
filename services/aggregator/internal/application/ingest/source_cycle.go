package ingest

import (
	"context"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/google/uuid"
)

type pollFunc func(context.Context, time.Time, string) error

func (p *Poller) runSource(ctx context.Context, source domain.Source, poll pollFunc) {
	var cursor time.Time
	lastSuccess := time.Time{}
	loaded := false
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		if !loaded {
			loadedCursor, err := p.repository.GetPollCursor(ctx, source)
			if err != nil {
				p.logger.Error("poll cursor load failed", "component", "poller", "source", source, "error", err)
				select {
				case <-ticker.C:
					continue
				case <-ctx.Done():
					return
				}
			}
			cursor = loadedCursor
			if cursor.IsZero() {
				cursor = time.Now().UTC().Add(-24 * time.Hour)
			}
			if status, statusErr := p.repository.GetSourceStatus(ctx, source); statusErr != nil {
				p.logger.Error("source status load failed", "component", "poller", "source", source, "error", statusErr)
			} else if status.LastSuccessAt != nil {
				lastSuccess = *status.LastSuccessAt
			}
			loaded = true
		}

		correlationID := uuid.NewString()
		batchUpperBound := time.Now().UTC()
		pollCtx, cancel := context.WithTimeout(ctx, p.timeout)
		err := poll(pollCtx, cursor.Add(-2*time.Second), correlationID)
		cancel()

		if err != nil {
			p.markStale(ctx, source, lastSuccess, time.Now().UTC(), correlationID, err)
		} else if err := p.repository.AdvancePollCursor(ctx, source, batchUpperBound); err != nil {
			p.markStale(ctx, source, lastSuccess, time.Now().UTC(), correlationID, err)
			p.logger.Error("poll cursor advance failed", "component", "poller", "source", source, "correlation_id", correlationID, "error", err)
		} else {
			cursor = batchUpperBound
			lastSuccess = batchUpperBound
			p.markHealthy(ctx, source, batchUpperBound)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (p *Poller) markStale(ctx context.Context, source domain.Source, lastSuccess, now time.Time, correlationID string, pollErr error) {
	staleSince := lastSuccess
	if staleSince.IsZero() {
		staleSince = now
	}
	status := domain.SourceStatus{Source: source, Status: "stale", StaleSince: &staleSince, LastError: pollErr.Error()}
	if err := p.repository.SetSourceStatus(ctx, status); err != nil {
		p.logger.Error("source status update failed", "component", "poller", "source", source, "error", err)
	}
	p.logger.Error("poll failed", "component", "poller", "source", source, "correlation_id", correlationID, "error", pollErr)
}

func (p *Poller) markHealthy(ctx context.Context, source domain.Source, now time.Time) {
	status := domain.SourceStatus{Source: source, Status: "healthy", LastSuccessAt: &now}
	if err := p.repository.SetSourceStatus(ctx, status); err != nil {
		p.logger.Error("source status update failed", "component", "poller", "source", source, "error", err)
	}
}
