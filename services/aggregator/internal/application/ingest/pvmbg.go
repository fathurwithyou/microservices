package ingest

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/eventmessage"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/mapping"
)

func (p *Poller) pollPVMBG(ctx context.Context, since time.Time, correlationID string) error {
	reports, err := p.pvmbg.Get(ctx, "/volcanic-reports?since="+url.QueryEscape(since.Format(time.RFC3339)), correlationID)
	if err != nil {
		return err
	}
	var mappingErr error
	for _, raw := range reports {
		event, err := mapping.Volcanic(raw, p.volcanoes, time.Now().UTC())
		if err != nil {
			p.logger.Error("source payload rejected", "component", "mapping", "source", domain.SourcePVMBG, "correlation_id", correlationID, "error", err)
			if mappingErr == nil {
				mappingErr = fmt.Errorf("map PVMBG report: %w", err)
			}
			continue
		}
		if err := p.persistAndEnqueue(ctx, event, correlationID); err != nil {
			return err
		}
	}
	return mappingErr
}

func (p *Poller) persistAndEnqueue(ctx context.Context, event domain.HazardEvent, correlationID string) error {
	payload, err := eventmessage.MarshalHazard(event)
	if err != nil {
		return err
	}
	return p.store.PersistHazardAndEnqueue(ctx, event, domain.OutboxMessage{HazardID: event.HazardID, CorrelationID: correlationID, RoutingKey: eventmessage.RoutingKey, Payload: payload})
}
