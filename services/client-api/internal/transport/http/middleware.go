package httptransport

import (
	"log/slog"
	"net/http"

	"github.com/fathurwithyou/microservices/pkg/ginmw"
	"github.com/gin-gonic/gin"
)

func correlationMiddleware() gin.HandlerFunc {
	return ginmw.Correlation()
}

func recoveryMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return ginmw.Recovery("client-api", logger)
}

func requestLogMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return ginmw.RequestLog("client-api", logger)
}

func requestLimitMiddleware(semaphore chan struct{}) gin.HandlerFunc {
	return func(c *gin.Context) {
		select {
		case semaphore <- struct{}{}:
			defer func() { <-semaphore }()
			c.Next()
		default:
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "request capacity exceeded"})
		}
	}
}
