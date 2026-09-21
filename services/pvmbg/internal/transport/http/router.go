package httptransport

import (
	"log/slog"

	"github.com/fathurwithyou/microservices/services/pvmbg/internal/state"
	"github.com/gin-gonic/gin"
)

type RouterConfig struct {
	State    *state.Runtime
	Token    string
	AdminKey string
	Logger   *slog.Logger
}

func NewRouter(cfg RouterConfig) *gin.Engine {
	handler := handler{state: cfg.State, logger: cfg.Logger}
	router := gin.New()
	router.Use(correlationMiddleware(), recoveryMiddleware(cfg.Logger), requestLogMiddleware(cfg.Logger))
	router.GET("/health", handler.health)
	router.GET("/volcanic-reports", requireBearer(cfg.Token), handler.volcanicReports)
	router.POST("/admin/schema-version", requireAdminKey(cfg.AdminKey), handler.schemaVersion)
	router.POST("/admin/outage", requireAdminKey(cfg.AdminKey), handler.outage)
	return router
}

type handler struct {
	state  *state.Runtime
	logger *slog.Logger
}
