package degraded

import (
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var answers = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "aggregator_degraded_answers_total",
	Help: "Read requests answered from the last good answer, marked partial, because the store did not answer.",
}, []string{"endpoint"})

func init() {
	prometheus.MustRegister(answers)
}

// logInterval spaces the warnings of one endpoint. During an outage every
// request is degraded; one line per request would bury the cause.
const logInterval = 5 * time.Second

var (
	logMu      sync.Mutex
	lastLogged = map[string]time.Time{}
	unlogged   = map[string]int{}
)

// Record counts one degraded answer of endpoint and warns about it at most
// once per logInterval per endpoint. It does nothing for a live answer.
func Record(endpoint string, info Info) {
	if !info.Degraded() {
		return
	}
	answers.WithLabelValues(endpoint).Inc()

	logMu.Lock()
	now := time.Now()
	unlogged[endpoint]++
	if now.Sub(lastLogged[endpoint]) < logInterval {
		logMu.Unlock()
		return
	}
	count := unlogged[endpoint]
	lastLogged[endpoint], unlogged[endpoint] = now, 0
	logMu.Unlock()

	attrs := []any{"service", "aggregator", "component", "degraded", "endpoint", endpoint, "answers", count, "cause", info.Cause.Error()}
	if !info.Since.IsZero() {
		attrs = append(attrs, "stale_since", info.Since.UTC().Format(time.RFC3339))
	}
	slog.Warn("store did not answer, serving the last good answer marked partial", attrs...)
}
