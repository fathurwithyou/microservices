package httptransport

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/fathurwithyou/microservices/services/bmkg/internal/store"
	"github.com/gin-gonic/gin"
)

func NewRouter(store *store.Memory, apiKey string, responseDelay time.Duration, logger *slog.Logger) *gin.Engine {
	handler := handler{store: store, responseDelay: responseDelay, logger: logger}
	router := gin.New()
	router.Use(correlationMiddleware(), recoveryMiddleware(logger), requestLogMiddleware(logger))
	router.GET("/health", handler.health)
	router.GET("/seismic-events", requireAPIKey(apiKey), handler.seismicEvents)
	router.GET("/tsunami-warnings", requireAPIKey(apiKey), handler.tsunamiWarnings)
	return router
}

type handler struct {
	store         *store.Memory
	responseDelay time.Duration
	logger        *slog.Logger
}

func (h handler) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "bmkg"})
}
