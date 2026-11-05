package application

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	droppedDetections = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "satellite_detections_dropped_total",
		Help: "Detections rejected before clustering, by reason.",
	}, []string{"reason"})
	firesSubmitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "satellite_fires_submitted_total",
		Help: "Fire updates sent to the Aggregator, by status.",
	}, []string{"status"})
)
