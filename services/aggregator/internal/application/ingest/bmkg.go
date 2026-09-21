package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/mapping"
)

func (p *Poller) pollBMKG(ctx context.Context, since time.Time, correlationID string) error {
	query := "?since=" + url.QueryEscape(since.Format(time.RFC3339))
	events, err := p.bmkg.Get(ctx, "/seismic-events"+query, correlationID)
	if err != nil {
		return err
	}
	warnings, err := p.bmkg.Get(ctx, "/tsunami-warnings"+query, correlationID)
	if err != nil {
		return err
	}

	warningsByEvent := warningsByEventID(warnings)
	var mappingErr error
	for _, raw := range events {
		event, err := mapping.Seismic(raw, warningsByEvent[rawString(raw, "event_id")], time.Now().UTC())
		if err != nil {
			p.logger.Error("source payload rejected", "component", "mapping", "source", domain.SourceBMKG, "correlation_id", correlationID, "error", err)
			if mappingErr == nil {
				mappingErr = fmt.Errorf("map BMKG event: %w", err)
			}
			continue
		}
		if err := p.persistAndEnqueue(ctx, event, correlationID); err != nil {
			return err
		}
	}
	return mappingErr
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
