package httptransport

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/fathurwithyou/microservices/pkg/ginmw"
	"github.com/fathurwithyou/microservices/services/demo-api/internal/config"
	"github.com/gin-gonic/gin"
)

const serviceName = "demo-api"

type RouterConfig struct {
	Config config.Config
	Client *http.Client
	Logger *slog.Logger
	Now    func() time.Time
}

func NewRouter(cfg RouterConfig) *gin.Engine {
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: cfg.Config.RequestTimeout}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	handler := handler{cfg: cfg.Config, client: cfg.Client, logger: cfg.Logger, now: cfg.Now}
	router := gin.New()
	router.Use(ginmw.Correlation(), ginmw.Recovery(serviceName, cfg.Logger), ginmw.RequestLog(serviceName, cfg.Logger))
	router.GET("/health", handler.health)
	router.POST("/admin/schema-version", handler.schemaVersion)
	router.POST("/admin/outage", handler.outage)
	router.GET("/observability/health", handler.serviceHealth)
	router.GET("/observability/queues", handler.queues)
	router.POST("/trust/cross-credentials", handler.crossCredentials)
	router.GET("/artifacts", handler.listArtifacts)
	router.GET("/artifacts/:name", handler.getArtifact)
	router.GET("/ownership", handler.ownership)
	return router
}

type handler struct {
	cfg    config.Config
	client *http.Client
	logger *slog.Logger
	now    func() time.Time
}

func (h handler) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "demo-api"})
}
