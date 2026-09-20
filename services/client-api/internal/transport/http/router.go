package httptransport

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/fathurwithyou/microservices/services/client-api/internal/application"
	"github.com/gin-gonic/gin"
)

func NewRouter(api *application.API, maxInflight int, timeout time.Duration, logger *slog.Logger) *gin.Engine {
	if maxInflight < 1 {
		maxInflight = 1
	}
	handler := handler{api: api, timeout: timeout}
	router := gin.New()
	router.Use(correlationMiddleware(), recoveryMiddleware(logger), requestLimitMiddleware(make(chan struct{}, maxInflight)), requestLogMiddleware(logger))
	router.GET("/health", health)
	router.POST("/auth/token", handler.issueToken)
	router.POST("/auth/refresh", handler.refreshToken)
	protected := router.Group("/")
	protected.Use(handler.authenticate)
	protected.GET("/hazards", handler.listHazards)
	protected.GET("/hazards/:hazard_id", handler.getHazard)
	protected.GET("/sources/status", handler.sourceStatus)
	return router
}

type handler struct {
	api     *application.API
	timeout time.Duration
}

func health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "client-api"})
}
