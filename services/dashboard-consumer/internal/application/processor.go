package application

import (
	"errors"
	"log/slog"

	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"google.golang.org/protobuf/proto"
)

var ErrInvalidEvent = errors.New("invalid hazard event")

type Processor struct {
	service string
	logger  *slog.Logger
	seen    map[string]struct{}
}

func NewProcessor(service string, logger *slog.Logger) *Processor {
	return &Processor{service: service, logger: logger, seen: make(map[string]struct{})}
}

func (p *Processor) Process(payload []byte, correlationID string, redelivered bool) error {
	var event eventspb.HazardEvent
	if err := proto.Unmarshal(payload, &event); err != nil {
		return ErrInvalidEvent
	}
	if _, duplicate := p.seen[event.GetHazardId()]; duplicate {
		return nil
	}
	p.seen[event.GetHazardId()] = struct{}{}
	p.logger.Info("event processed", "service", p.service, "event_id", event.GetHazardId(), "correlation_id", correlationID, "redelivered", redelivered)
	return nil
}
