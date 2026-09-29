// Package httptransport wires the PVMBG HTTP API.
package httptransport

import (
	"log/slog"
	"math/rand/v2"
	"net/http"

	"github.com/fathurwithyou/microservices/pkg/ginmw"
	"github.com/fathurwithyou/microservices/services/pvmbg/internal/state"
	"github.com/gin-gonic/gin"
)

// serviceName identifies this service in middleware logs and the health
// response.
const serviceName = "pvmbg"

// adminKeyHeader carries the simulation-control credential.
const adminKeyHeader = "X-PVMBG-Admin-Key"

// bearerScheme is the Authorization scheme for data routes.
const bearerScheme = "Bearer "

// RouterConfig configures NewRouter.
type RouterConfig struct {
	State    *state.Runtime
	Token    string
	AdminKey string
	Rand     *rand.Rand
	Logger   *slog.Logger
}

// NewRouter builds the gin engine with shared middleware and PVMBG routes.
func NewRouter(cfg RouterConfig) *gin.Engine {
	handler := handler{state: cfg.State, rand: cfg.Rand, logger: cfg.Logger}
	if handler.rand == nil {
		handler.rand = rand.New(rand.NewPCG(uint64(1), uint64(2)))
	}
	router := gin.New()
	router.Use(
		ginmw.Correlation(),
		ginmw.Recovery(serviceName, cfg.Logger),
		ginmw.RequestLog(serviceName, cfg.Logger),
	)
	router.GET("/health", handler.health)
	router.GET("/volcanic-reports", ginmw.RequireHeader(ginmw.HeaderAuth{
		Header:   "Authorization",
		Expected: bearerScheme + cfg.Token,
		Status:   http.StatusUnauthorized,
		Message:  "invalid PVMBG credential",
	}), handler.volcanicReports)
	admin := ginmw.RequireHeader(ginmw.HeaderAuth{
		Header:   adminKeyHeader,
		Expected: cfg.AdminKey,
		Status:   http.StatusForbidden,
		Message:  "invalid admin credential",
	})
	router.POST("/admin/schema-version", admin, handler.schemaVersion)
	router.POST("/admin/outage", admin, handler.outage)
	return router
}

type handler struct {
	state  *state.Runtime
	rand   *rand.Rand
	logger *slog.Logger
}
