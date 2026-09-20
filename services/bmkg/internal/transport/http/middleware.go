package httptransport

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func requireAPIKey(expected string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.GetHeader("X-BMKG-Key") != expected {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid BMKG credential"})
			return
		}
		c.Next()
	}
}

func correlationMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		correlationID := c.GetHeader("X-Correlation-ID")
		if correlationID == "" {
			correlationID = uuid.NewString()
		}
		c.Set("correlation_id", correlationID)
		c.Header("X-Correlation-ID", correlationID)
		c.Next()
	}
}

func recoveryMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Error("panic recovered", "service", "bmkg", "component", "http", "correlation_id", c.GetString("correlation_id"), "error", recovered)
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			}
		}()
		c.Next()
	}
}

func requestLogMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()
		logger.Info("request completed", "service", "bmkg", "component", "http", "operation", c.Request.Method+" "+c.Request.URL.Path, "correlation_id", c.GetString("correlation_id"), "latency_ms", time.Since(started).Milliseconds(), "status", c.Writer.Status())
	}
}
