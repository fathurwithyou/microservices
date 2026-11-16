package httptransport

import (
	"net/http"
	"time"

	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/fathurwithyou/microservices/pkg/observability"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/audit"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/failures"
)

// NewServer returns the aggregator HTTP server with health and Audit API
// routes.
func NewServer(port string, auditQueries *audit.Queries, auditToken, opsToken string, replayer *failures.Replayer) *http.Server {
	router := http.NewServeMux()
	router.HandleFunc("GET /health", health)
	router.Handle("GET /metrics", observability.MetricsHandler())
	if auditQueries != nil {
		auditHandler := newAuditHandler(auditQueries, replayer)
		router.HandleFunc("GET /internal/audit/events", auditAuth(auditToken, auditHandler.events))
		router.HandleFunc("GET /internal/audit/checksum", auditAuth(auditToken, auditHandler.checksum))
		router.HandleFunc("GET /internal/audit/location", auditAuth(auditToken, auditHandler.location))
		router.HandleFunc("GET /internal/audit/distribution", auditAuth(auditToken, auditHandler.distribution))
		router.HandleFunc("GET /internal/audit/unprocessed", auditAuth(auditToken, auditHandler.unprocessed))
		// Replay mutates state, so it is gated by the BNPB Ops credential and
		// never by the read-only audit token.
		router.HandleFunc("POST /internal/ops/replay", opsAuth(opsToken, auditHandler.replay))
		// Raw failed payloads are readable only by BNPB Internal Ops.
		router.HandleFunc("GET /internal/ops/failures", opsAuth(opsToken, auditHandler.opsFailures))
		router.HandleFunc("GET /internal/ops/failures/{message_id}", opsAuth(opsToken, auditHandler.opsFailure))
		router.HandleFunc("POST /internal/ops/field-reports/lookup", opsAuth(opsToken, auditHandler.fieldReportLookup))
	}
	return &http.Server{Addr: ":" + port, Handler: observability.HTTPTracing(observability.HTTPMetrics(router), "aggregator"), ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 10 * time.Second}
}

func health(response http.ResponseWriter, request *http.Request) {
	correlationID := request.Header.Get(correlation.Header)
	if correlationID == "" {
		correlationID = correlation.New()
	}
	response.Header().Set(correlation.Header, correlationID)
	response.Header().Set("Content-Type", "application/json")
	_, _ = response.Write([]byte(`{"status":"ok","service":"aggregator"}`))
}
