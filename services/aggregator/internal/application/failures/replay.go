// Package failures replays parked source payloads after a mapping fix.
package failures

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/mapping"
)

// Repository supplies parked payloads and records recovery.
type Repository interface {
	ListIngestFailures(context.Context, domain.Source, int, time.Time, string) ([]domain.IngestFailure, error)
	MarkIngestFailureRecovered(context.Context, string) error
}

// Submitter persists replayed events through the canonical ingestion path.
type Submitter interface {
	Submit(context.Context, domain.HazardEvent, string) (string, error)
}

// Replayer maps parked payloads again using the current mapping rules.
type Replayer struct {
	repository Repository
	submitter  Submitter
	volcanoes  map[string]domain.VolcanoReference
	now        func() time.Time
	logger     *slog.Logger
}

// Config configures a Replayer.
type Config struct {
	Repository Repository
	Submitter  Submitter
	Volcanoes  map[string]domain.VolcanoReference
	Now        func() time.Time
	Logger     *slog.Logger
}

// New validates cfg and returns a Replayer.
func New(cfg Config) (*Replayer, error) {
	if cfg.Repository == nil || cfg.Submitter == nil {
		return nil, fmt.Errorf("failures: repository and submitter are required")
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Replayer{repository: cfg.Repository, submitter: cfg.Submitter, volcanoes: cfg.Volcanoes, now: cfg.Now, logger: cfg.Logger}, nil
}

// Replay processes parked payloads for one source in arrival order.
func (r *Replayer) Replay(ctx context.Context, source domain.Source, limit int) (int, error) {
	items, err := r.repository.ListIngestFailures(ctx, source, limit, time.Time{}, "")
	if err != nil {
		return 0, fmt.Errorf("list failures: %w", err)
	}
	recovered := 0
	for _, item := range items {
		// Stop at the first failure so a later message never overtakes an
		// earlier one for the same source reference.
		event, err := r.mapPayload(item)
		if err != nil {
			r.logger.Warn("replay mapping failed", "component", "failures", "message_id", item.MessageID, "source", item.Source, "error", err)
			return recovered, fmt.Errorf("replay %s: %w", item.MessageID, err)
		}
		if _, err := r.submitter.Submit(ctx, event, item.MessageID); err != nil {
			r.logger.Error("replay submit failed", "component", "failures", "message_id", item.MessageID, "error", err)
			return recovered, fmt.Errorf("replay %s: %w", item.MessageID, err)
		}
		if err := r.repository.MarkIngestFailureRecovered(ctx, item.MessageID); err != nil {
			r.logger.Error("replay mark recovered failed", "component", "failures", "message_id", item.MessageID, "error", err)
			return recovered, fmt.Errorf("replay %s: %w", item.MessageID, err)
		}
		recovered++
	}
	return recovered, nil
}

func (r *Replayer) mapPayload(item domain.IngestFailure) (domain.HazardEvent, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(item.Payload, &raw); err != nil {
		return domain.HazardEvent{}, err
	}
	switch item.Source {
	case domain.SourcePVMBG:
		return mapping.Volcanic(raw, r.volcanoes, r.now())
	case domain.SourceBMKG:
		return mapping.Seismic(raw, nil, r.now())
	default:
		return domain.HazardEvent{}, fmt.Errorf("unsupported replay source %q", item.Source)
	}
}
