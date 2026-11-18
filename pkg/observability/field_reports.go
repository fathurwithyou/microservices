package observability

import "github.com/prometheus/client_golang/prometheus"

var fieldReportsProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "field_reports_processed_total",
	Help: "Field reports drained from the inbox by the aggregator worker.",
}, []string{"hazard_type", "severity"})

func init() {
	prometheus.MustRegister(fieldReportsProcessed)
}

// ObserveFieldReportProcessed counts one drained field report.
func ObserveFieldReportProcessed(hazardType, severity string) {
	fieldReportsProcessed.WithLabelValues(hazardType, severity).Inc()
}
