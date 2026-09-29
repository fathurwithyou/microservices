// Package healthsrv provides a minimal stdlib HTTP health server for
// background-worker services that do not otherwise expose HTTP.
package healthsrv

import (
	"net/http"
	"strconv"
	"time"
)

// Config configures the health server.
type Config struct {
	// Port is the TCP port to listen on.
	Port string
	// Service is the service name reported in the health payload.
	Service string
}

// New returns an *http.Server serving GET /health with a JSON payload
// identifying the service. The service name is embedded at construction
// time so deployments cannot drift between the binary and its health
// output.
func New(cfg Config) *http.Server {
	body := []byte(`{"status":"ok","service":` + strconv.Quote(cfg.Service) + `}`)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write(body)
	})
	return &http.Server{Addr: ":" + cfg.Port, Handler: mux, ReadHeaderTimeout: 2 * time.Second}
}
