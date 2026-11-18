package application

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	readingsAccepted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "coastal_readings_accepted_total",
		Help: "Sensor readings accepted into the canonical store.",
	})
	readingsQuarantined = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "coastal_readings_quarantined_total",
		Help: "Sensor readings rejected before the canonical store, by reason.",
	}, []string{"reason"})
	criticalReadings = promauto.NewCounter(prometheus.CounterOpts{
		Name: "coastal_critical_readings_total",
		Help: "Critical sensor readings forwarded to the Aggregator.",
	})
	criticalQuarantined = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "coastal_critical_quarantined_total",
		Help: "Readings flagged critical that were NOT forwarded because their identity was broken, by reason. Every increment is a warning nobody was notified about.",
	}, []string{"reason"})
	feedFreshness = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "coastal_feed_freshness_seconds",
		Help: "Sensor clock (generated_at) minus the newest observed_at seen, in sensor seconds, sampled when a fetch reaches the end of the stream. Large while the sensor answers healthy means its data is old.",
	})
	sensorClock = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "coastal_sensor_clock_seconds",
		Help: "Newest generated_at reported by the sensor, as Unix seconds of the sensor's own clock.",
	})
	submitBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "coastal_submit_backlog",
		Help: "Accepted non-critical readings fetched but not yet stored by the Aggregator.",
	})
	circuitState = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "coastal_circuit_state",
		Help: "Sensor circuit breaker state: 0 closed, 1 half-open, 2 open.",
	})
)
