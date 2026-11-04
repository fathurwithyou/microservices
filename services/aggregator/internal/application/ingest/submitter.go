package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/eventmessage"
)

// Submitter accepts HazardEvents produced by internal ingestor services.
type Submitter struct {
	repository Repository
	logger     *slog.Logger
}

// NewSubmitter returns a submitter backed by the canonical repository.
func NewSubmitter(repository Repository, logger *slog.Logger) (*Submitter, error) {
	if repository == nil {
		return nil, errors.New("ingest: submitter repository is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Submitter{repository: repository, logger: logger}, nil
}

// Submit persists event and its outbox record in one transaction.
func (s *Submitter) Submit(ctx context.Context, event domain.HazardEvent, correlationID string) (string, error) {
	if event.HazardID == "" {
		return "", fmt.Errorf("%w: hazard_id is required", domain.ErrInvalidSourcePayload)
	}
	payload, err := eventmessage.MarshalHazard(event)
	if err != nil {
		return "", err
	}
	if err := s.repository.PersistHazardAndEnqueue(ctx, event, domain.OutboxMessage{
		HazardID:      event.HazardID,
		CorrelationID: correlationID,
		RoutingKey:    routingKey(event),
		Payload:       payload,
	}); err != nil {
		return "", fmt.Errorf("persist submitted hazard: %w", err)
	}
	s.logger.Info("hazard submitted", "component", "ingest", "event_id", event.HazardID, "source", event.Source, "correlation_id", correlationID)
	return event.HazardID, nil
}

func routingKey(event domain.HazardEvent) string {
	if event.Source == domain.SourceCoastalSensor {
		if critical, ok := event.Attributes["critical"].(bool); ok && critical {
			return eventmessage.CriticalRoutingKey
		}
	}
	return eventmessage.RoutingKey
}
