package httptransport

import (
	"net/http"
	"time"

	"github.com/google/uuid"
)

func NewHealthServer(port string) *http.Server {
	router := http.NewServeMux()
	router.HandleFunc("/health", health)
	return &http.Server{Addr: ":" + port, Handler: router, ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 3 * time.Second}
}

func health(response http.ResponseWriter, request *http.Request) {
	correlationID := request.Header.Get("X-Correlation-ID")
	if correlationID == "" {
		correlationID = uuid.NewString()
	}
	response.Header().Set("X-Correlation-ID", correlationID)
	response.Header().Set("Content-Type", "application/json")
	_, _ = response.Write([]byte(`{"status":"ok","service":"aggregator"}`))
}
