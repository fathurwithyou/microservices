package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Reference points a critical-notification latency can be measured from.
const (
	// LatencyFromOccurred starts the clock when the hazard happened at the
	// source (sensor observation time). It includes sensor lag and polling.
	LatencyFromOccurred = "occurred_at"
	// LatencyFromIngested starts the clock when the aggregator stored the
	// event. It isolates the outbox, broker, and consumer pipeline.
	LatencyFromIngested = "ingested_at"
	// LatencyFromReceived starts the clock when the ingestor finished
	// reading the source response that carried the event (wall clock,
	// nanosecond resolution, the received_at attribute). It is the only
	// reference taken on our own clock at the moment the data first reached
	// us, so it is the one a "delivered within N seconds" target is checked
	// against; the source's own exposure delay (at most one poll interval
	// while the ingestor is polling) comes on top.
	LatencyFromReceived = "received_at"
)

var (
	// CriticalLatencyBuckets has an exact 5 s edge so the "p95 <= 5 s" target
	// can be checked from bucket counts without interpolation.
	CriticalLatencyBuckets = []float64{0.1, 0.25, 0.5, 1, 2, 3, 4, 5, 7.5, 10, 20, 30, 60}

	criticalNotificationLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "critical_notification_latency_seconds",
		Help:    "Seconds from a critical hazard reference time until a notifier consumed it.",
		Buckets: CriticalLatencyBuckets,
	}, []string{"service", "from"})

	criticalNotificationSkipped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "critical_notification_latency_skipped_total",
		Help: "Critical events whose latency could not be measured, by reason.",
	}, []string{"service", "from", "reason"})
)

func init() {
	prometheus.MustRegister(criticalNotificationLatency, criticalNotificationSkipped)
}

// ObserveCriticalNotification records the delay between reference and now for a
// critical event. An empty or unparsable reference, or one in the future
// (clock skew), is counted as skipped rather than clamped, so skew is visible
// instead of silently flattering the percentile.
func ObserveCriticalNotification(service, from, reference string, now time.Time) {
	if reference == "" {
		criticalNotificationSkipped.WithLabelValues(service, from, "missing").Inc()
		return
	}
	started, err := time.Parse(time.RFC3339, reference)
	if err != nil {
		criticalNotificationSkipped.WithLabelValues(service, from, "unparsable").Inc()
		return
	}
	elapsed := now.Sub(started)
	if elapsed < 0 {
		criticalNotificationSkipped.WithLabelValues(service, from, "future").Inc()
		return
	}
	criticalNotificationLatency.WithLabelValues(service, from).Observe(elapsed.Seconds())
}
