package outbox

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/adapters/memory"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type recordingPublisher struct {
	calls int
	err   error
}

func (p *recordingPublisher) Publish(context.Context, domain.OutboxMessage) error {
	p.calls++
	return p.err
}

func (p *recordingPublisher) Close() error { return nil }

func TestDispatchPendingPublishesAndAcknowledgesClaimedMessage(t *testing.T) {
	repository := memory.NewRepository()
	event := domain.HazardEvent{HazardID: "h-worker", Source: domain.SourceBMKG, SourceRefID: "source-worker", HazardType: domain.HazardSeismic, Severity: domain.SeverityNormal, OccurredAt: time.Now(), Attributes: map[string]any{}}
	if err := repository.PersistHazardAndEnqueue(context.Background(), event, domain.OutboxMessage{CorrelationID: "test", RoutingKey: "hazard.event", Payload: []byte("event")}); err != nil {
		t.Fatal(err)
	}
	publisher := &recordingPublisher{}
	worker := NewWorker(WorkerConfig{Repository: repository, Publisher: publisher, Interval: time.Second, Logger: slog.Default()})
	worker.dispatchPending(context.Background())

	if publisher.calls != 1 {
		t.Fatalf("publish calls = %d, want 1", publisher.calls)
	}
	remaining, err := repository.ClaimPendingOutbox(context.Background(), 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("acknowledged message remained pending: %#v", remaining)
	}
}

func TestDispatchPendingReleasesFailedClaimForRetry(t *testing.T) {
	repository := memory.NewRepository()
	event := domain.HazardEvent{HazardID: "h-retry", Source: domain.SourcePVMBG, SourceRefID: "source-retry", HazardType: domain.HazardVolcanic, Severity: domain.SeverityWaspada, OccurredAt: time.Now(), Attributes: map[string]any{}}
	if err := repository.PersistHazardAndEnqueue(context.Background(), event, domain.OutboxMessage{CorrelationID: "test", RoutingKey: "hazard.event", Payload: []byte("event")}); err != nil {
		t.Fatal(err)
	}
	publisher := &recordingPublisher{err: errors.New("broker unavailable")}
	worker := NewWorker(WorkerConfig{Repository: repository, Publisher: publisher, Interval: time.Second, Logger: slog.Default()})
	worker.dispatchPending(context.Background())

	immediate, err := repository.ClaimPendingOutbox(context.Background(), 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(immediate) != 0 {
		t.Fatalf("failed message was immediately reclaimed: %#v", immediate)
	}

	time.Sleep(outboxRetryBase + 100*time.Millisecond)
	retry, err := repository.ClaimPendingOutbox(context.Background(), 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(retry) != 1 || retry[0].Attempts != 2 {
		t.Fatalf("failed message was not made available for retry: %#v", retry)
	}
}

func TestRetryDelayIsExponentialAndBounded(t *testing.T) {
	if got := retryDelay(1); got != 500*time.Millisecond {
		t.Fatalf("first retry delay = %s, want 500ms", got)
	}
	if got := retryDelay(3); got != 2*time.Second {
		t.Fatalf("third retry delay = %s, want 2s", got)
	}
	if got := retryDelay(99); got != outboxRetryMaximum {
		t.Fatalf("retry delay = %s, want maximum %s", got, outboxRetryMaximum)
	}
}
