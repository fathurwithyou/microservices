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

// received_at is the reference the 5 s target is checked against: the wall
// clock at which the ingestor finished reading the sensor response.
func TestCriticalEventRecordsLatencyFromReceivedAt(t *testing.T) {
	const service = "latency-received"
	processor := latencyProcessor(service)

	event := criticalEvent("h-6", 6*24*time.Hour, time.Second)
	// As published by the Aggregator: the value is a JSON-encoded string.
	event.Attributes["received_at"] = `"` + latencyNow.Add(-1250*time.Millisecond).Format(time.RFC3339Nano) + `"`
	deliver(t, processor, event, false)

	count, sum := histogramSample(t, service, observability.LatencyFromReceived)
	if count != 1 || math.Abs(sum-1.25) > 1e-9 {
		t.Fatalf("received_at sample = (%d, %v), want (1, 1.25)", count, sum)
	}
	// The other two references are still recorded.
	if count, _ := histogramSample(t, service, observability.LatencyFromIngested); count != 1 {
		t.Fatalf("ingested_at count = %d, want 1", count)
	}
	if count, _ := histogramSample(t, service, observability.LatencyFromOccurred); count != 1 {
		t.Fatalf("occurred_at count = %d, want 1", count)
	}
}

func TestBareReceivedAtAttributeIsAccepted(t *testing.T) {
	const service = "latency-received-bare"
	processor := latencyProcessor(service)
	event := criticalEvent("h-7", time.Second, time.Second)
	event.Attributes["received_at"] = latencyNow.Add(-2 * time.Second).Format(time.RFC3339Nano)
	deliver(t, processor, event, false)
	if count, sum := histogramSample(t, service, observability.LatencyFromReceived); count != 1 || math.Abs(sum-2) > 1e-9 {
		t.Fatalf("received_at sample = (%d, %v), want (1, 2)", count, sum)
	}
}

// Events from sources that do not set received_at add no sample and no
// "skipped" count for that reference.
func TestMissingReceivedAtIsNotCounted(t *testing.T) {
	const service = "latency-received-absent"
	processor := latencyProcessor(service)
	deliver(t, processor, criticalEvent("h-8", time.Second, time.Second), false)
	if count, _ := histogramSample(t, service, observability.LatencyFromReceived); count != 0 {
		t.Fatalf("received_at count = %d, want 0", count)
	}
	if got := skippedCount(t, service, observability.LatencyFromReceived, "missing"); got != 0 {
		t.Fatalf("missing skipped = %v, want 0", got)
	}
}

func TestUnusableReceivedAtIsSkippedAndCounted(t *testing.T) {
	const service = "latency-received-bad"
	processor := latencyProcessor(service)
	garbled := criticalEvent("h-9", time.Second, time.Second)
	garbled.Attributes["received_at"] = `"soon"`
	deliver(t, processor, garbled, false)
	ahead := criticalEvent("h-10", time.Second, time.Second)
	ahead.Attributes["received_at"] = `"` + latencyNow.Add(time.Minute).Format(time.RFC3339Nano) + `"`
	deliver(t, processor, ahead, false)
	if count, _ := histogramSample(t, service, observability.LatencyFromReceived); count != 0 {
		t.Fatalf("received_at count = %d, want 0", count)
	}
	if got := skippedCount(t, service, observability.LatencyFromReceived, "unparsable"); got != 1 {
		t.Fatalf("unparsable skipped = %v, want 1", got)
	}
	if got := skippedCount(t, service, observability.LatencyFromReceived, "future"); got != 1 {
		t.Fatalf("future skipped = %v, want 1", got)
	}
}

// A redelivery of the same message is measured once; the same observation ID
// received again by the ingestor (sensor restarted and reused the ID) is a new
// notification and a new sample.
func TestReceivedAtDistinguishesReuseFromRedelivery(t *testing.T) {
	const service = "latency-received-reuse"
	processor := latencyProcessor(service)

	first := criticalEvent("h-11", time.Second, time.Second)
	first.Attributes["received_at"] = `"` + latencyNow.Add(-time.Second).Format(time.RFC3339Nano) + `"`
	deliver(t, processor, first, false)
	deliver(t, processor, first, true)
	if count, _ := histogramSample(t, service, observability.LatencyFromReceived); count != 1 {
		t.Fatalf("count after a redelivery = %d, want 1", count)
	}

	reused := criticalEvent("h-11", time.Second, time.Second)
	reused.Attributes["received_at"] = `"` + latencyNow.Add(-500*time.Millisecond).Format(time.RFC3339Nano) + `"`
	deliver(t, processor, reused, false)
	if count, _ := histogramSample(t, service, observability.LatencyFromReceived); count != 2 {
		t.Fatalf("count after the ID was received again = %d, want 2", count)
	}
}
