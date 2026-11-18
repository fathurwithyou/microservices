// Package failures replays parked source payloads after a mapping fix.
package failures

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/mapping"
)

// Repository supplies parked payloads and records recovery. Listings carry
// metadata only; payloads are loaded one message at a time so a backlog of
// large bodies never sits in memory at once.
type Repository interface {
	ListIngestFailures(context.Context, domain.FailureFilter) ([]domain.IngestFailure, error)
	GetIngestFailure(context.Context, string) (domain.IngestFailure, error)
	MarkIngestFailureRecovered(context.Context, string) error
}

// pageSize bounds one metadata read during a replay or retry pass.
const pageSize = 200

// maxPages bounds one pass so a huge backlog cannot keep it busy forever.
const maxPages = 25

// Submitter persists replayed events through the canonical ingestion path.
type Submitter interface {
	Submit(context.Context, domain.HazardEvent, string) (string, error)
}

// errUndecodable marks a stored body that is not one JSON object, such as a
// truncated or oversized response. Replaying it can never succeed.
var errUndecodable = errors.New("undecodable payload")

// Replayer maps parked payloads again using the current mapping rules.
type Replayer struct {
	repository Repository
	submitter  Submitter
	references ReferenceLoader
	mu         sync.RWMutex
	volcanoes  map[string]domain.VolcanoReference
	now        func() time.Time
	logger     *slog.Logger
}

// ReferenceLoader reloads canonical reference data so a replay or retry sees
// rows added after startup.
type ReferenceLoader func(context.Context) (map[string]domain.VolcanoReference, error)

// Config configures a Replayer.
type Config struct {
	Repository Repository
	Submitter  Submitter
	Volcanoes  map[string]domain.VolcanoReference
	// References, when set, refreshes Volcanoes before every replay or retry.
	References ReferenceLoader
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
	return &Replayer{repository: cfg.Repository, submitter: cfg.Submitter, references: cfg.References, volcanoes: cfg.Volcanoes, now: cfg.Now, logger: cfg.Logger}, nil
}

// refreshReferences reloads reference data; on failure the previous copy is
// kept so a store hiccup never empties the map.
func (r *Replayer) refreshReferences(ctx context.Context) {
	if r.references == nil {
		return
	}
	loaded, err := r.references(ctx)
	if err != nil {
		r.logger.Warn("reference reload failed", "component", "failures", "error", err)
		return
	}
	r.mu.Lock()
	r.volcanoes = loaded
	r.mu.Unlock()
}

// Replay processes up to limit replayable messages for one source in
// arrival order. Undecodable bodies are skipped without counting toward
// limit, so a run of them cannot hide the recoverable messages behind it.
func (r *Replayer) Replay(ctx context.Context, source domain.Source, limit int) (int, error) {
	r.refreshReferences(ctx)
	recovered, attempted := 0, 0
	err := r.walk(ctx, domain.FailureFilter{Source: source}, func(item domain.IngestFailure) (bool, error) {
		if attempted >= limit {
			return false, nil
		}
		event, err := r.mapPayload(item)
		if errors.Is(err, errUndecodable) {
			// A whole response that never decoded. Its window was re-polled at
			// ingest time, so nothing is lost; keep it parked for inspection and
			// do not let it block the records behind it.
			r.logger.Warn("replay skipped undecodable message", "component", "failures", "message_id", item.MessageID, "source", item.Source, "error", err)
			return true, nil
		}
		attempted++
		// Stop at the first failure so a later message never overtakes an
		// earlier one for the same source reference.
		if err != nil {
			r.logger.Warn("replay mapping failed", "component", "failures", "message_id", item.MessageID, "source", item.Source, "error", err)
			return false, fmt.Errorf("replay %s: %w", item.MessageID, err)
		}
		if _, err := r.submitter.Submit(ctx, event, item.MessageID); err != nil {
			r.logger.Error("replay submit failed", "component", "failures", "message_id", item.MessageID, "error", err)
			return false, fmt.Errorf("replay %s: %w", item.MessageID, err)
		}
		if err := r.repository.MarkIngestFailureRecovered(ctx, item.MessageID); err != nil {
			r.logger.Error("replay mark recovered failed", "component", "failures", "message_id", item.MessageID, "error", err)
			return false, fmt.Errorf("replay %s: %w", item.MessageID, err)
		}
		recovered++
		return true, nil
	})
	return recovered, err
}

// walk pages through failures matching filter, oldest first, loads each
// message with its payload, and calls visit until it returns false or an
// error. Messages that left the active queue since the listing are skipped.
func (r *Replayer) walk(ctx context.Context, filter domain.FailureFilter, visit func(domain.IngestFailure) (bool, error)) error {
	filter.Limit = pageSize
	for page := 0; page < maxPages; page++ {
		items, err := r.repository.ListIngestFailures(ctx, filter)
		if err != nil {
			return fmt.Errorf("list failures: %w", err)
		}
		for _, meta := range items {
			item, err := r.repository.GetIngestFailure(ctx, meta.MessageID)
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}
			if err != nil {
				return fmt.Errorf("load %s: %w", meta.MessageID, err)
			}
			if item.Status != meta.Status {
				continue
			}
			more, err := visit(item)
			if err != nil || !more {
				return err
			}
		}
		if len(items) < pageSize {
			return nil
		}
		last := items[len(items)-1]
		filter.After, filter.AfterID = last.ReceivedAt, last.MessageID
	}
	return nil
}

func (r *Replayer) mapPayload(item domain.IngestFailure) (domain.HazardEvent, error) {
	var raw map[string]json.RawMessage
	if !utf8.Valid(item.Payload) {
		return domain.HazardEvent{}, fmt.Errorf("%w: invalid UTF-8", errUndecodable)
	}
	if err := json.Unmarshal(item.Payload, &raw); err != nil {
		return domain.HazardEvent{}, fmt.Errorf("%w: %v", errUndecodable, err)
	}
	switch item.Source {
	case domain.SourcePVMBG:
		r.mu.RLock()
		volcanoes := r.volcanoes
		r.mu.RUnlock()
		return mapping.VolcanicReceived(raw, volcanoes, r.now(), item.ReceivedAt)
	case domain.SourceBMKG:
		return mapping.Seismic(raw, nil, r.now())
	default:
		return domain.HazardEvent{}, fmt.Errorf("unsupported replay source %q", item.Source)
	}
}
