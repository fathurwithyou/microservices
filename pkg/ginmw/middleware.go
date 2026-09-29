// Package ginmw provides shared gin middleware: correlation ID
// propagation, panic recovery, request logging, and header-based
// credential checks. All middleware is parameterized by service name so no
// business logic is shared between services.
package ginmw

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"time"

	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/gin-gonic/gin"
)

// Correlation returns middleware that reads the correlation ID from the
// request header, generates one when absent, stores it in the gin context
// under correlation.GinKey, and echoes it in the response header.
func Correlation() gin.HandlerFunc {
	return func(c *gin.Context) {
		correlationID := c.GetHeader(correlation.Header)
		if correlationID == "" {
			correlationID = correlation.New()
		}
		c.Set(correlation.GinKey, correlationID)
		c.Header(correlation.Header, correlationID)
		c.Next()
	}
}

// Recovery returns middleware that recovers panics, logs them against the
// service, and responds with a generic 500.
func Recovery(service string, logger *slog.Logger) gin.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}
	return func(c *gin.Context) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Error("panic recovered", "service", service, "component", "http", "correlation_id", c.GetString(correlation.GinKey), "error", recovered)
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			}
		}()
		c.Next()
	}
}

// RequestLog returns middleware that logs one structured record per
// request with method, path, correlation ID, latency, and status code.
func RequestLog(service string, logger *slog.Logger) gin.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()
		logger.Info("request completed", "service", service, "component", "http", "operation", c.Request.Method+" "+c.Request.URL.Path, "correlation_id", c.GetString(correlation.GinKey), "latency_ms", time.Since(started).Milliseconds(), "status", c.Writer.Status())
	}
}

// HeaderAuth configures RequireHeader.
type HeaderAuth struct {
	// Header is the request header carrying the credential.
	Header string
	// Expected is the exact credential value, compared in constant time.
	Expected string
	// Status is the HTTP status returned on failure. Defaults to 401.
	Status int
	// Message is the JSON error body on failure. Defaults to
	// "invalid credential".
	Message string
}

// RequireHeader returns middleware that aborts with a JSON error unless the
// configured header exactly matches the expected credential.
func RequireHeader(cfg HeaderAuth) gin.HandlerFunc {
	status := cfg.Status
	if status == 0 {
		status = http.StatusUnauthorized
	}
	message := cfg.Message
	if message == "" {
		message = "invalid credential"
	}
	return func(c *gin.Context) {
		value := c.GetHeader(cfg.Header)
		if subtle.ConstantTimeCompare([]byte(value), []byte(cfg.Expected)) != 1 {
			c.AbortWithStatusJSON(status, gin.H{"error": message})
			return
		}
		c.Next()
	}
}
