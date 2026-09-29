// Package httptransport wires the BMKG HTTP API.
package httptransport

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/fathurwithyou/microservices/pkg/ginmw"
	"github.com/fathurwithyou/microservices/services/bmkg/internal/state"
	"github.com/gin-gonic/gin"
)

// serviceName identifies this service in middleware logs and the health
// response.
const serviceName = "bmkg"

// apiKeyHeader carries the BMKG credential.
const apiKeyHeader = "X-BMKG-Key"

// RouterConfig configures NewRouter.
type RouterConfig struct {
	State         *state.Memory
	APIKey        string
	ResponseDelay time.Duration
	Logger        *slog.Logger
}

// NewRouter builds the gin engine with shared middleware and BMKG routes.
func NewRouter(cfg RouterConfig) *gin.Engine {
	handler := handler{state: cfg.State, responseDelay: cfg.ResponseDelay, logger: cfg.Logger}
	router := gin.New()
	router.Use(
		ginmw.Correlation(),
		ginmw.Recovery(serviceName, cfg.Logger),
		ginmw.RequestLog(serviceName, cfg.Logger),
	)
	requireAPIKey := ginmw.RequireHeader(ginmw.HeaderAuth{
		Header:   apiKeyHeader,
		Expected: cfg.APIKey,
		Status:   http.StatusUnauthorized,
		Message:  "invalid BMKG credential",
	})
	router.GET("/health", handler.health)
	router.GET("/seismic-events", requireAPIKey, handler.seismicEvents)
	router.GET("/tsunami-warnings", requireAPIKey, handler.tsunamiWarnings)
	return router
}

type handler struct {
	state         *state.Memory
	responseDelay time.Duration
	logger        *slog.Logger
}
