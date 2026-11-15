package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/google/uuid"
)

// cursorOverlap widens each poll window slightly backwards so events
// published right at a batch boundary are not missed. Re-fetched duplicates
// are deduplicated by persistence on (source, source_ref_id).
const cursorOverlap = 2 * time.Second

// defaultInitialLookback is used when no cursor has been persisted yet.
const defaultInitialLookback = 24 * time.Hour

func (p *Poller) runSource(ctx context.Context, source Source) {
	name := source.Name()
	var cursor time.Time
	lastSuccess := time.Time{}
	loaded := false
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		if !loaded {
			loadedCursor, err := p.repository.GetPollCursor(ctx, name)
			if err != nil {
				p.logger.Error("poll cursor load failed", "component", "poller", "source", name, "error", err)
				select {
				case <-ticker.C:
					continue
				case <-ctx.Done():
					return
				}
			}
			cursor = loadedCursor
			if cursor.IsZero() {
				cursor = time.Now().UTC().Add(-defaultInitialLookback)
			}
			if status, statusErr := p.repository.GetSourceStatus(ctx, name); statusErr != nil {
				p.logger.Error("source status load failed", "component", "poller", "source", name, "error", statusErr)
			} else if status.LastSuccessAt != nil {
				lastSuccess = *status.LastSuccessAt
			}
			loaded = true
		}

		correlationID := uuid.NewString()
		batchUpperBound := time.Now().UTC()
		if !p.leaderAllowed() {
			select {
			case <-ticker.C:
				continue
			case <-ctx.Done():
				return
			}
		}
		pollCtx, cancel := context.WithTimeout(ctx, p.timeout)
		events, fetchErr := source.Fetch(pollCtx, cursor.Add(-cursorOverlap), correlationID)
		cancel()

		// Ownership can change while a fetch is in flight. Revalidate against
		// the database clock so a frozen instance cannot persist after its TTL.
		if !p.stillLeader(ctx) {
			select {
			case <-ticker.C:
				continue
			case <-ctx.Done():
				return
			}
		}
		batchErr := p.persistBatch(ctx, events, correlationID)
		if mappingErr, ok := fetchErr.(*MappingError); ok {
			p.recordFailures(ctx, mappingErr, correlationID)
		}
		switch {
		case batchErr != nil:
			// Persistence is on the critical path: never advance past events
			// that were not stored.
			p.markStale(ctx, name, lastSuccess, time.Now().UTC(), correlationID, batchErr)
		case fetchErr != nil && (!isMappingError(fetchErr) || isTransportError(fetchErr)):
			// Undecodable responses are parked above, but the window they
			// covered is re-polled so valid records in it are not lost.
			p.markStale(ctx, name, lastSuccess, time.Now().UTC(), correlationID, fetchErr)
		default:
			if err := p.repository.AdvancePollCursor(ctx, name, batchUpperBound); err != nil {
				p.markStale(ctx, name, lastSuccess, time.Now().UTC(), correlationID, err)
				p.logger.Error("poll cursor advance failed", "component", "poller", "source", name, "correlation_id", correlationID, "error", err)
				select {
				case <-ticker.C:
				case <-ctx.Done():
					return
				}
				continue
			}
			cursor = batchUpperBound
			lastSuccess = batchUpperBound
			p.markHealthy(ctx, name, batchUpperBound)
			if fetchErr != nil {
				// Events were ingested; only some payloads were rejected and
				// the source already logged each rejection. Advancing the
				// cursor prevents poison payloads from blocking the feed.
				p.logger.Warn("batch ingested with rejected payloads", "component", "poller", "source", name, "correlation_id", correlationID, "error", fetchErr)
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

// persistBatch stores every mapped event with its outbox message and returns
// the first persistence failure, if any.
func (p *Poller) persistBatch(ctx context.Context, events []domain.HazardEvent, correlationID string) error {
	for _, event := range events {
		if err := p.persistAndEnqueue(ctx, event, correlationID); err != nil {
			p.logger.Error("event persistence failed", "component", "poller", "source", event.Source, "correlation_id", correlationID, "error", err)
			return err
		}
	}
	return nil
}

func isMappingError(err error) bool {
	var mappingErr *MappingError
	return errors.As(err, &mappingErr)
}

func isTransportError(err error) bool {
	var mappingErr *MappingError
	return errors.As(err, &mappingErr) && mappingErr.Transport
}

func (p *Poller) leaderAllowed() bool {
	return p.leadership == nil || p.leadership.Allowed()
}

// stillLeader revalidates against the lease store when the leadership
// implementation supports it, and falls back to the local flag otherwise.
func (p *Poller) stillLeader(ctx context.Context) bool {
	if p.leadership == nil {
		return true
	}
	if fenced, ok := p.leadership.(interface {
		StillLeader(context.Context) (bool, error)
	}); ok {
		valid, err := fenced.StillLeader(ctx)
		if err != nil {
			p.logger.Error("leader revalidation failed", "component", "poller", "error", err)
		}
		return valid
	}
	return p.leadership.Allowed()
}

func (p *Poller) recordFailures(ctx context.Context, mappingErr *MappingError, correlationID string) {
	for _, rejected := range mappingErr.Rejected {
		payload := rejected.Payload
		if payload == nil {
			var err error
			payload, err = json.Marshal(rejected.Raw)
			if err != nil {
				p.logger.Error("failed payload marshal", "component", "poller", "source", rejected.Source, "error", err)
				continue
			}
		}
		messageID := uuid.NewSHA1(uuid.NameSpaceURL, append([]byte(string(rejected.Source)+":"), payload...)).String()
		// A schema violation never succeeds until the mapping changes; any
		// other mapping failure may be transient and stays in the retry queue.
		retryable := !errors.Is(rejected.Err, domain.ErrInvalidSourcePayload)
		status := domain.FailureParked
		if retryable {
			status = domain.FailureRetrying
		}
		if err := p.repository.SaveIngestFailure(ctx, domain.IngestFailure{
			MessageID:  messageID,
			Source:     rejected.Source,
			ReceivedAt: time.Now().UTC(),
			Reason:     rejected.Err.Error(),
			Retryable:  retryable,
			Status:     status,
			Payload:    payload,
		}); err != nil {
			p.logger.Error("failed payload persist", "component", "poller", "source", rejected.Source, "correlation_id", correlationID, "error", err)
		}
	}
}

func (p *Poller) markStale(ctx context.Context, source domain.Source, lastSuccess, now time.Time, correlationID string, pollErr error) {
	staleSince := lastSuccess
	if staleSince.IsZero() {
		staleSince = now
	}
	status := domain.SourceStatus{Source: source, Status: domain.SourceStateStale, StaleSince: &staleSince, LastError: pollErr.Error()}
	if err := p.repository.SetSourceStatus(ctx, status); err != nil {
		p.logger.Error("source status update failed", "component", "poller", "source", source, "error", err)
	}
	p.logger.Error("poll failed", "component", "poller", "source", source, "correlation_id", correlationID, "error", pollErr)
}

func (p *Poller) markHealthy(ctx context.Context, source domain.Source, now time.Time) {
	status := domain.SourceStatus{Source: source, Status: domain.SourceStateHealthy, LastSuccessAt: &now}
	if err := p.repository.SetSourceStatus(ctx, status); err != nil {
		p.logger.Error("source status update failed", "component", "poller", "source", source, "error", err)
	}
}
