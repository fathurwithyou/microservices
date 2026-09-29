package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/pkg/consumerkit"
	"google.golang.org/protobuf/proto"
)

// ErrInvalidEvent marks a payload that cannot be decoded as a hazard event.
var ErrInvalidEvent = errors.New("invalid hazard event")

// maxSeenIDs bounds the in-memory deduplication set; the oldest IDs are
// evicted first once the cap is reached.
const maxSeenIDs = 10_000

// Processor decodes hazard events and deduplicates them by hazard ID within
// the process lifetime. It is safe for concurrent use and survives broker
// reconnects, so a single instance is created per service run.
type Processor struct {
	service string
	logger  *slog.Logger

	mu      sync.Mutex
	seen    map[string]struct{}
	seenIDs []string // FIFO eviction order for seen
}

// NewProcessor returns a Processor that logs under the given service name.
func NewProcessor(service string, logger *slog.Logger) *Processor {
	return &Processor{
		service: service,
		logger:  logger,
		seen:    make(map[string]struct{}),
	}
}

// Process decodes one delivery and reports the acknowledgement outcome.
// Undecodable payloads are dead-lettered; duplicates are acknowledged
// without reprocessing.
func (p *Processor) Process(_ context.Context, delivery consumerkit.Delivery) consumerkit.Outcome {
	var event eventspb.HazardEvent
	if err := proto.Unmarshal(delivery.Body, &event); err != nil {
		p.logger.Error("event decode failed", "service", p.service, "correlation_id", delivery.CorrelationID, "error", fmt.Errorf("%w: %v", ErrInvalidEvent, err))
		return consumerkit.DeadLetter
	}
	eventID := event.GetHazardId()
	if p.isDuplicate(eventID) {
		p.logger.Debug("duplicate event skipped", "service", p.service, "event_id", eventID, "correlation_id", delivery.CorrelationID)
		return consumerkit.Ack
	}
	p.logger.Info("event processed", "service", p.service, "event_id", eventID, "correlation_id", delivery.CorrelationID, "redelivered", delivery.Redelivered)
	return consumerkit.Ack
}

// isDuplicate records eventID and reports whether it was already seen,
// evicting the oldest entry when the set exceeds maxSeenIDs.
func (p *Processor) isDuplicate(eventID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, seen := p.seen[eventID]; seen {
		return true
	}
	p.seen[eventID] = struct{}{}
	p.seenIDs = append(p.seenIDs, eventID)
	if len(p.seenIDs) > maxSeenIDs {
		oldest := p.seenIDs[0]
		p.seenIDs = p.seenIDs[1:]
		delete(p.seen, oldest)
	}
	return false
}
