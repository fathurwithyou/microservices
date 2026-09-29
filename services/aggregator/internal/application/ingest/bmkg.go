package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/mapping"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/ports"
)

// BMKGSource fetches seismic events and tsunami warnings from the BMKG feed
// and maps them to domain events, correlating warnings to their events
// through related_event_id.
type BMKGSource struct {
	client ports.SourceClient
	logger *slog.Logger
}

// BMKGSourceConfig configures a BMKGSource.
type BMKGSourceConfig struct {
	// Client performs authenticated GET requests against the BMKG API. Required.
	Client ports.SourceClient
	// Logger receives payload rejection logs. Defaults to slog.Default().
	Logger *slog.Logger
}

// NewBMKGSource validates cfg and returns a BMKGSource.
func NewBMKGSource(cfg BMKGSourceConfig) (*BMKGSource, error) {
	if cfg.Client == nil {
		return nil, errors.New("ingest: BMKG client is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &BMKGSource{client: cfg.Client, logger: cfg.Logger}, nil
}

// Name implements Source.
func (s *BMKGSource) Name() domain.Source { return domain.SourceBMKG }

// Fetch implements Source.
func (s *BMKGSource) Fetch(ctx context.Context, since time.Time, correlationID string) ([]domain.HazardEvent, error) {
	query := "?since=" + url.QueryEscape(since.Format(time.RFC3339))
	rawEvents, err := s.client.Get(ctx, "/seismic-events"+query, correlationID)
	if err != nil {
		return nil, err
	}
	rawWarnings, err := s.client.Get(ctx, "/tsunami-warnings"+query, correlationID)
	if err != nil {
		return nil, err
	}

	warningsByEvent := warningsByEventID(rawWarnings)
	now := time.Now().UTC()
	events := make([]domain.HazardEvent, 0, len(rawEvents))
	var mappingErrs []error
	for _, raw := range rawEvents {
		event, err := mapping.Seismic(raw, warningsByEvent[rawString(raw, "event_id")], now)
		if err != nil {
			s.logger.Error("source payload rejected", "component", "mapping", "source", domain.SourceBMKG, "correlation_id", correlationID, "error", err)
			mappingErrs = append(mappingErrs, fmt.Errorf("map BMKG event: %w", err))
			continue
		}
		events = append(events, event)
	}
	if len(mappingErrs) > 0 {
		return events, &MappingError{Source: domain.SourceBMKG, Err: errors.Join(mappingErrs...)}
	}
	return events, nil
}

func warningsByEventID(warnings []map[string]json.RawMessage) map[string]map[string]json.RawMessage {
	result := make(map[string]map[string]json.RawMessage)
	for _, warning := range warnings {
		if relatedEventID := rawString(warning, "related_event_id"); relatedEventID != "" {
			result[relatedEventID] = warning
		}
	}
	return result
}

func rawString(raw map[string]json.RawMessage, key string) string {
	var value string
	data, ok := raw[key]
	if !ok || json.Unmarshal(data, &value) != nil {
		return ""
	}
	return value
}
