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
	circuitState = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "coastal_circuit_state",
		Help: "Sensor circuit breaker state: 0 closed, 1 half-open, 2 open.",
	})
)
