package httptransport

import (
	"log/slog"

	"github.com/fathurwithyou/microservices/services/pvmbg/internal/store"
	"github.com/gin-gonic/gin"
)

func NewRouter(state *store.Runtime, token, adminKey string, logger *slog.Logger) *gin.Engine {
	handler := handler{state: state, logger: logger}
	router := gin.New()
	router.Use(correlationMiddleware(), recoveryMiddleware(logger), requestLogMiddleware(logger))
	router.GET("/health", handler.health)
	router.GET("/volcanic-reports", requireBearer(token), handler.volcanicReports)
	router.POST("/admin/schema-version", requireAdminKey(adminKey), handler.schemaVersion)
	router.POST("/admin/outage", requireAdminKey(adminKey), handler.outage)
	return router
}

type handler struct {
	state  *store.Runtime
	logger *slog.Logger
}
