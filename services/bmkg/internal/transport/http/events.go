package httptransport

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func (h handler) seismicEvents(c *gin.Context) {
	if !waitForResponseDelay(c, h.responseDelay) {
		return
	}
	started := time.Now()
	c.JSON(http.StatusOK, h.state.EventsSince(parseSince(c.Query("since"))))
	h.logResponse(c, "seismic-events", started)
}

func (h handler) tsunamiWarnings(c *gin.Context) {
	started := time.Now()
	c.JSON(http.StatusOK, h.state.WarningsSince(parseSince(c.Query("since"))))
	h.logResponse(c, "tsunami-warnings", started)
}

func (h handler) logResponse(c *gin.Context, operation string, started time.Time) {
	h.logger.Info("outbound response", "service", "bmkg", "component", "http", "operation", operation, "latency_ms", time.Since(started).Milliseconds(), "status", http.StatusOK, "correlation_id", c.GetString("correlation_id"))
}

func waitForResponseDelay(c *gin.Context, delay time.Duration) bool {
	select {
	case <-time.After(delay):
		return true
	case <-c.Request.Context().Done():
		return false
	}
}

func parseSince(value string) time.Time {
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed
	}
	return time.Time{}
}
