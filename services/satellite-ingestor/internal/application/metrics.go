package application

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	eventsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "satellite_feed_events_total",
		Help: "Feed events read from the stream, by kind.",
	}, []string{"kind"})
	eventOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "satellite_event_outcomes_total",
		Help: "What each detection, update or retract event did to the working set.",
	}, []string{"outcome"})
	droppedDetections = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "satellite_detections_dropped_total",
		Help: "Detections not clustered, by reason (late, low_confidence, invalid_time, future).",
	}, []string{"reason"})
	skippedEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "satellite_events_skipped_total",
		Help: "Feed events passed over without effect, by reason (undecodable, unknown_kind, missing_offset, duplicate_offset).",
	}, []string{"reason"})
	offsetGaps = promauto.NewCounter(prometheus.CounterOpts{
		Name: "satellite_feed_offset_gaps_total",
		Help: "Times the next feed offset was not the committed offset plus one. Stays 0 when nothing is lost.",
	})
	firesSubmitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "satellite_fires_submitted_total",
		Help: "Fire updates sent to the Aggregator, by status.",
	}, []string{"status"})
	watermarkSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_event_time_watermark_seconds",
		Help: "Latest satellite acquisition time seen (event-time watermark), Unix seconds.",
	})
	feedLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_feed_lag_events",
		Help: "Feed offsets between the latest event and the committed offset.",
	})
	committedOffset = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_feed_committed_offset",
		Help: "Last feed offset whose effects are durable.",
	})
	latestOffset = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_feed_latest_offset",
		Help: "Latest offset the feed session reports.",
	})
	feedSpeed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_feed_speed",
		Help: "Replay speed the feed session reports.",
	})
	activeFires = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_active_fires",
		Help: "Fires that can still change.",
	})
	singleDetections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_single_detections",
		Help: "Detections in the working set that belong to no fire.",
	})
	liveDetections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_working_detections",
		Help: "Detections kept in the working set.",
	})
	cumulative = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "satellite_cumulative_count",
		Help: "Durable cumulative counts since the feed session began (late, low_confidence, closed_fires, closed_singles, ...). Survives restarts, unlike the counters.",
	}, []string{"name"})
	eventToStore = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "satellite_event_to_store_seconds",
		Help:    "Time from reading a feed event to its fires being stored by the Aggregator and the offset committed.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 20, 30, 60, 120, 300},
	})
	batchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "satellite_batch_events",
		Help:    "Feed events committed per batch.",
		Buckets: []float64{1, 4, 16, 64, 128, 256, 512},
	})
	reconnects = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "satellite_feed_reconnects_total",
		Help: "Stream connections that ended, by cause (closed, idle, status_401, status_429, status_503, error, process).",
	}, []string{"cause"})
	sessionResets = promauto.NewCounter(prometheus.CounterOpts{
		Name: "satellite_feed_session_resets_total",
		Help: "Times a new feed session (offsets restarted) made the ingestor start over.",
	})
	streamConnected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satellite_feed_connected",
		Help: "1 while this replica holds an open stream.",
	})
)
