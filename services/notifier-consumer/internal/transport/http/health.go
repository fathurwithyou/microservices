package httptransport

import "net/http"

func NewHealthServer(port string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"status":"ok","service":"notifier-consumer"}`))
	})
	return &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 2_000_000_000}
}
