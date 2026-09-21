package httptransport

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/fathurwithyou/microservices/services/bmkg/internal/state"
	"github.com/gin-gonic/gin"
)

type RouterConfig struct {
	State         *state.Memory
	APIKey        string
	ResponseDelay time.Duration
	Logger        *slog.Logger
}

func NewRouter(cfg RouterConfig) *gin.Engine {
	handler := handler{state: cfg.State, responseDelay: cfg.ResponseDelay, logger: cfg.Logger}
	router := gin.New()
	router.Use(correlationMiddleware(), recoveryMiddleware(cfg.Logger), requestLogMiddleware(cfg.Logger))
	router.GET("/health", handler.health)
	router.GET("/seismic-events", requireAPIKey(cfg.APIKey), handler.seismicEvents)
	router.GET("/tsunami-warnings", requireAPIKey(cfg.APIKey), handler.tsunamiWarnings)
	return router
}

type handler struct {
	state         *state.Memory
	responseDelay time.Duration
	logger        *slog.Logger
}

func (h handler) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "bmkg"})
}
