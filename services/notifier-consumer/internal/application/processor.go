package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/pkg/consumerkit"
	"github.com/fathurwithyou/microservices/pkg/observability"
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
	now     func() time.Time

	mu      sync.Mutex
	seen    map[string]struct{}
	seenIDs []string // FIFO eviction order for seen
}

// NewProcessor returns a Processor that logs under the given service name.
func NewProcessor(service string, logger *slog.Logger) *Processor {
	return &Processor{
		service: service,
		logger:  logger,
		now:     func() time.Time { return time.Now().UTC() },
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
	if p.isDuplicate(deliveryKey(&event)) {
		p.logger.Debug("duplicate event skipped", "service", p.service, "event_id", eventID, "correlation_id", delivery.CorrelationID)
		return consumerkit.Ack
	}
	if isCritical(&event) {
		p.recordCriticalLatency(&event)
	}
	p.logger.Info("event processed", "service", p.service, "event_id", eventID, "correlation_id", delivery.CorrelationID, "redelivered", delivery.Redelivered)
	return consumerkit.Ack
}

// isCritical reports whether the aggregator flagged the event as field-critical.
func isCritical(event *eventspb.HazardEvent) bool {
	return event.GetAttributes()["critical"] == "true"
}

// receivedAtAttribute is set by an ingestor on events it wants measured from
// the moment the source data reached it.
const receivedAtAttribute = "received_at"

// deliveryKey is what "the same delivery" means for deduplication. A broker
// redelivery repeats the message byte for byte, so the hazard ID alone
// identifies it. An event that carries received_at is identified by the ID and
// that receipt: when the source serves an observation ID again after a restart
// (Sensor Pantai renumbers its stream from zero), it is a new observation that
// happens to reuse an ID, and Tim Lapangan must hear about it. Deduplicating
// on the ID alone would swallow it for the lifetime of this process.
func deliveryKey(event *eventspb.HazardEvent) string {
	if received := attributeString(event, receivedAtAttribute); received != "" {
		return event.GetHazardId() + "|" + received
	}
	return event.GetHazardId()
}

// attributeString returns a string attribute. The Aggregator publishes
// attribute values JSON-encoded, so a string arrives quoted; a bare value is
// accepted as is.
func attributeString(event *eventspb.HazardEvent, key string) string {
	raw, ok := event.GetAttributes()[key]
	if !ok {
		return ""
	}
	var decoded string
	if err := json.Unmarshal([]byte(raw), &decoded); err == nil {
		return decoded
	}
	return raw
}

// recordCriticalLatency measures the first delivery of a critical event
// against the source time, the ingestion time, and, when the ingestor
// supplied it, the time the source data was received. Duplicates and
// redeliveries never reach this point, so one event contributes one sample.
func (p *Processor) recordCriticalLatency(event *eventspb.HazardEvent) {
	now := p.now()
	observability.ObserveCriticalNotification(p.service, observability.LatencyFromOccurred, event.GetOccurredAt(), now)
	observability.ObserveCriticalNotification(p.service, observability.LatencyFromIngested, event.GetIngestedAt(), now)
	// Only sources that set received_at are measured from it; its absence
	// is normal for the others and is not counted as a skipped sample.
	if received := attributeString(event, receivedAtAttribute); received != "" {
		observability.ObserveCriticalNotification(p.service, observability.LatencyFromReceived, received, now)
	}
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
