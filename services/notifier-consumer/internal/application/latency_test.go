package application

import (
	"context"
	"io"
	"log/slog"
	"math"
	"testing"
	"time"

	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/pkg/consumerkit"
	"github.com/fathurwithyou/microservices/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

var latencyNow = time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)

func latencyProcessor(service string) *Processor {
	processor := NewProcessor(service, slog.New(slog.NewTextHandler(io.Discard, nil)))
	processor.now = func() time.Time { return latencyNow }
	return processor
}

func deliver(t *testing.T, processor *Processor, event *eventspb.HazardEvent, redelivered bool) {
	t.Helper()
	body, err := proto.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	outcome := processor.Process(context.Background(), consumerkit.Delivery{Body: body, CorrelationID: "c", Redelivered: redelivered})
	if outcome != consumerkit.Ack {
		t.Fatalf("outcome = %v, want Ack", outcome)
	}
}

// histogramSample returns the observation count and sum for a latency series,
// or zeros when the series does not exist yet.
func histogramSample(t *testing.T, service, from string) (uint64, float64) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "critical_notification_latency_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["service"] == service && labels["from"] == from {
				return metric.GetHistogram().GetSampleCount(), metric.GetHistogram().GetSampleSum()
			}
		}
	}
	return 0, 0
}

func skippedCount(t *testing.T, service, from, reason string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "critical_notification_latency_skipped_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["service"] == service && labels["from"] == from && labels["reason"] == reason {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func criticalEvent(id string, occurredAgo, ingestedAgo time.Duration) *eventspb.HazardEvent {
	return &eventspb.HazardEvent{
		HazardId:   id,
		OccurredAt: latencyNow.Add(-occurredAgo).Format(time.RFC3339),
		IngestedAt: latencyNow.Add(-ingestedAgo).Format(time.RFC3339),
		Attributes: map[string]string{"critical": "true"},
	}
}

func TestCriticalEventRecordsLatencyFromBothReferences(t *testing.T) {
	const service = "latency-both"
	processor := latencyProcessor(service)

	deliver(t, processor, criticalEvent("h-1", 4*time.Second, time.Second), false)

	count, sum := histogramSample(t, service, observability.LatencyFromOccurred)
	if count != 1 || math.Abs(sum-4) > 1e-9 {
		t.Fatalf("occurred_at sample = (%d, %v), want (1, 4)", count, sum)
	}
	count, sum = histogramSample(t, service, observability.LatencyFromIngested)
	if count != 1 || math.Abs(sum-1) > 1e-9 {
		t.Fatalf("ingested_at sample = (%d, %v), want (1, 1)", count, sum)
	}
}

func TestNonCriticalEventRecordsNoLatency(t *testing.T) {
	const service = "latency-normal"
	processor := latencyProcessor(service)

	normal := criticalEvent("h-2", time.Second, time.Second)
	normal.Attributes = map[string]string{"critical": "false"}
	deliver(t, processor, normal, false)
	deliver(t, processor, &eventspb.HazardEvent{HazardId: "h-3"}, false)

	if count, _ := histogramSample(t, service, observability.LatencyFromOccurred); count != 0 {
		t.Fatalf("occurred_at count = %d, want 0", count)
	}
	if count, _ := histogramSample(t, service, observability.LatencyFromIngested); count != 0 {
		t.Fatalf("ingested_at count = %d, want 0", count)
	}
}

func TestRedeliveredCriticalEventIsMeasuredOnce(t *testing.T) {
	const service = "latency-dup"
	processor := latencyProcessor(service)

	event := criticalEvent("h-4", 2*time.Second, time.Second)
	deliver(t, processor, event, false)
	deliver(t, processor, event, true)

	if count, _ := histogramSample(t, service, observability.LatencyFromIngested); count != 1 {
		t.Fatalf("count = %d, want 1 (duplicate must not add a sample)", count)
	}
}

func TestUnmeasurableLatencyIsSkippedAndCounted(t *testing.T) {
	const service = "latency-skip"
	processor := latencyProcessor(service)

	// ingested_at in the future (clock skew), occurred_at missing.
	event := &eventspb.HazardEvent{
		HazardId:   "h-5",
		IngestedAt: latencyNow.Add(3 * time.Second).Format(time.RFC3339),
		Attributes: map[string]string{"critical": "true"},
	}
	deliver(t, processor, event, false)

	if count, _ := histogramSample(t, service, observability.LatencyFromIngested); count != 0 {
		t.Fatalf("future reference must not be observed, count = %d", count)
	}
	if got := skippedCount(t, service, observability.LatencyFromIngested, "future"); got != 1 {
		t.Fatalf("future skipped = %v, want 1", got)
	}
	if got := skippedCount(t, service, observability.LatencyFromOccurred, "missing"); got != 1 {
		t.Fatalf("missing skipped = %v, want 1", got)
	}
}
