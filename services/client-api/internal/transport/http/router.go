package httptransport

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/fathurwithyou/microservices/pkg/observability"
	"github.com/fathurwithyou/microservices/services/client-api/internal/application"
	"github.com/gin-gonic/gin"
)

type RouterConfig struct {
	API         *application.API
	MaxInflight int
	Timeout     time.Duration
	Logger      *slog.Logger
}

func NewRouter(cfg RouterConfig) *gin.Engine {
	if cfg.MaxInflight < 1 {
		cfg.MaxInflight = 1
	}
	handler := handler{api: cfg.API, timeout: cfg.Timeout, limiter: newClassLimiter(cfg.MaxInflight)}
	router := gin.New()
	router.Use(observability.GinTracing("client-api"), correlationMiddleware(), recoveryMiddleware(cfg.Logger), requestLogMiddleware(cfg.Logger), observability.GinMetrics())
	router.GET("/health", health)
	router.GET("/metrics", gin.WrapH(observability.MetricsHandler()))
	// Token endpoints carry no client class yet, so they get a plain cap. A
	// global cap in front of every route would let a Media flood fill it and
	// reject Field Team requests before their class is known.
	tokenLimit := requestLimitMiddleware(make(chan struct{}, cfg.MaxInflight))
	router.POST("/auth/token", tokenLimit, handler.issueToken)
	router.POST("/auth/refresh", tokenLimit, handler.refreshToken)
	protected := router.Group("/")
	protected.Use(handler.authenticate, handler.priorityLimit)
	protected.GET("/hazards", handler.listHazards)
	protected.GET("/hazards/:hazard_id", handler.getHazard)
	protected.GET("/sources/status", handler.sourceStatus)
	protected.GET("/coastal/latest", handler.latestCoastal)
	protected.GET("/analytics", handler.analytics)
	protected.POST("/field-reports", handler.submitFieldReport)
	return router
}

type handler struct {
	api     *application.API
	timeout time.Duration
	limiter *classLimiter
}

func health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "client-api"})
}
