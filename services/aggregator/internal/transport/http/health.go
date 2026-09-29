package httptransport

import (
	"net/http"
	"time"

	"github.com/fathurwithyou/microservices/pkg/correlation"
)

// NewHealthServer returns the aggregator liveness HTTP server.
func NewHealthServer(port string) *http.Server {
	router := http.NewServeMux()
	router.HandleFunc("GET /health", health)
	return &http.Server{Addr: ":" + port, Handler: router, ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 3 * time.Second}
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
