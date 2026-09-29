package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/mapping"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/ports"
)

// PVMBGSource fetches volcanic reports from the PVMBG feed and maps them to
// domain events, resolving volcano locations from canonical reference data.
type PVMBGSource struct {
	client    ports.SourceClient
	logger    *slog.Logger
	volcanoes map[string]domain.VolcanoReference
}

// PVMBGSourceConfig configures a PVMBGSource.
type PVMBGSourceConfig struct {
	// Client performs authenticated GET requests against the PVMBG API. Required.
	Client ports.SourceClient
	// Logger receives payload rejection logs. Defaults to slog.Default().
	Logger *slog.Logger
	// Volcanoes is the canonical reference data keyed by volcano ID. Required.
	Volcanoes map[string]domain.VolcanoReference
}

// NewPVMBGSource validates cfg and returns a PVMBGSource. The volcano
// reference map is copied so later mutations by the caller cannot affect
// mapping.
func NewPVMBGSource(cfg PVMBGSourceConfig) (*PVMBGSource, error) {
	if cfg.Client == nil {
		return nil, errors.New("ingest: PVMBG client is required")
	}
	if len(cfg.Volcanoes) == 0 {
		return nil, errors.New("ingest: volcano reference data is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &PVMBGSource{client: cfg.Client, logger: cfg.Logger, volcanoes: maps.Clone(cfg.Volcanoes)}, nil
}

// Name implements Source.
func (s *PVMBGSource) Name() domain.Source { return domain.SourcePVMBG }

// Fetch implements Source.
func (s *PVMBGSource) Fetch(ctx context.Context, since time.Time, correlationID string) ([]domain.HazardEvent, error) {
	reports, err := s.client.Get(ctx, "/volcanic-reports?since="+url.QueryEscape(since.Format(time.RFC3339)), correlationID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	events := make([]domain.HazardEvent, 0, len(reports))
	var mappingErrs []error
	for _, raw := range reports {
		event, err := mapping.Volcanic(raw, s.volcanoes, now)
		if err != nil {
			s.logger.Error("source payload rejected", "component", "mapping", "source", domain.SourcePVMBG, "correlation_id", correlationID, "error", err)
			mappingErrs = append(mappingErrs, fmt.Errorf("map PVMBG report: %w", err))
			continue
		}
		events = append(events, event)
	}
	if len(mappingErrs) > 0 {
		return events, &MappingError{Source: domain.SourcePVMBG, Err: errors.Join(mappingErrs...)}
	}
	return events, nil
}
