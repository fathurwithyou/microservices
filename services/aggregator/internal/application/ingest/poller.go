// Package ingest polls external hazard feeds, maps their payloads to domain
// events, and persists them transactionally with an outbox message.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/eventmessage"
)

// ErrInvalidConfig is returned by constructors for missing required
// configuration.
var ErrInvalidConfig = errors.New("ingest: invalid config")

// Poller runs one scheduling loop per registered Source. All source-agnostic
// concerns live here: poll cursors, per-poll timeouts, source health status,
// persistence, and outbox enqueueing.
type Poller struct {
	repository Repository
	sources    []Source
	interval   time.Duration
	timeout    time.Duration
	logger     *slog.Logger
}

// PollerConfig configures a Poller.
type PollerConfig struct {
	// Repository persists events, cursors, and source status. Required.
	Repository Repository
	// Sources are the feeds to poll. At least one is required.
	Sources []Source
	// Interval between poll cycles per source. Must be positive.
	Interval time.Duration
	// Timeout bounds a single poll cycle. Must be positive.
	Timeout time.Duration
	// Logger receives structured logs. Defaults to slog.Default().
	Logger *slog.Logger
}

// NewPoller validates cfg and returns a Poller.
func NewPoller(cfg PollerConfig) (*Poller, error) {
	if cfg.Repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidConfig)
	}
	if len(cfg.Sources) == 0 {
		return nil, fmt.Errorf("%w: at least one source is required", ErrInvalidConfig)
	}
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("%w: poll interval must be positive", ErrInvalidConfig)
	}
	if cfg.Timeout <= 0 {
		return nil, fmt.Errorf("%w: poll timeout must be positive", ErrInvalidConfig)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Poller{repository: cfg.Repository, sources: cfg.Sources, interval: cfg.Interval, timeout: cfg.Timeout, logger: cfg.Logger}, nil
}

// Run polls every registered source concurrently until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	var wait sync.WaitGroup
	for _, source := range p.sources {
		wait.Add(1)
		go func() {
			defer wait.Done()
			p.runSource(ctx, source)
		}()
	}
	wait.Wait()
}

// persistAndEnqueue stores one event and its outbox message in a single
// transaction so publishing survives process restarts.
func (p *Poller) persistAndEnqueue(ctx context.Context, event domain.HazardEvent, correlationID string) error {
	payload, err := eventmessage.MarshalHazard(event)
	if err != nil {
		return err
	}
	return p.repository.PersistHazardAndEnqueue(ctx, event, domain.OutboxMessage{HazardID: event.HazardID, CorrelationID: correlationID, RoutingKey: eventmessage.RoutingKey, Payload: payload})
}
