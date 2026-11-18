// Package httptransport wires the PVMBG HTTP API.
package httptransport

import (
	"log/slog"
	"math/rand/v2"
	"net/http"

	"github.com/fathurwithyou/microservices/pkg/ginmw"
	"github.com/fathurwithyou/microservices/pkg/observability"
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
	// AdminOpen lets a caller without the admin key header through. The
	// course Tester Agent calls the admin endpoints from inside the cluster
	// and carries no credential of ours.
	AdminOpen bool
	Rand      *rand.Rand
	Logger    *slog.Logger
}

// NewRouter builds the gin engine with shared middleware and PVMBG routes.
func NewRouter(cfg RouterConfig) *gin.Engine {
	handler := handler{state: cfg.State, rand: cfg.Rand, logger: cfg.Logger}
	if handler.rand == nil {
		handler.rand = rand.New(rand.NewPCG(uint64(1), uint64(2)))
	}
	router := gin.New()
	router.Use(
		observability.GinTracing("pvmbg"),
		ginmw.Correlation(),
		ginmw.Recovery(serviceName, cfg.Logger),
		ginmw.RequestLog(serviceName, cfg.Logger),
		observability.GinMetrics(),
	)
	router.GET("/health", handler.health)
	router.GET("/metrics", gin.WrapH(observability.MetricsHandler()))
	router.GET("/volcanic-reports", ginmw.RequireHeader(ginmw.HeaderAuth{
		Header:   "Authorization",
		Expected: bearerScheme + cfg.Token,
		Status:   http.StatusUnauthorized,
		Message:  "invalid PVMBG credential",
	}), handler.volcanicReports)
	requireKey := ginmw.RequireHeader(ginmw.HeaderAuth{
		Header:   adminKeyHeader,
		Expected: cfg.AdminKey,
		Status:   http.StatusForbidden,
		Message:  "invalid admin credential",
	})
	admin := func(c *gin.Context) {
		// A wrong key is always refused; only a missing key is let through,
		// and only when the deployment opted in.
		if cfg.AdminOpen && c.GetHeader(adminKeyHeader) == "" {
			c.Next()
			return
		}
		requireKey(c)
	}
	router.POST("/admin/schema-version", admin, handler.schemaVersion)
	router.POST("/admin/outage", admin, handler.outage)
	return router
}

type handler struct {
	state  *state.Runtime
	rand   *rand.Rand
	logger *slog.Logger
}
