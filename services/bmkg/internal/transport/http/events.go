package httptransport

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func (h handler) seismicEvents(c *gin.Context) {
	if !waitDelay(c.Request.Context(), h.responseDelay) {
		return
	}
	since, ok := parseSince(c)
	if !ok {
		return
	}
	started := time.Now()
	c.JSON(http.StatusOK, h.state.EventsSince(since))
	h.logResponse(c, "seismic-events", started)
}

func (h handler) tsunamiWarnings(c *gin.Context) {
	since, ok := parseSince(c)
	if !ok {
		return
	}
	started := time.Now()
	c.JSON(http.StatusOK, h.state.WarningsSince(since))
	h.logResponse(c, "tsunami-warnings", started)
}

func (h handler) logResponse(c *gin.Context, operation string, started time.Time) {
	h.logger.Info("outbound response", "service", serviceName, "component", "http", "operation", operation, "latency_ms", time.Since(started).Milliseconds(), "status", http.StatusOK, "correlation_id", c.GetString("correlation_id"))
}

// waitDelay blocks for d, returning false when the client disconnected
// before the delay elapsed.
func waitDelay(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// parseSince reads the optional `since` query parameter as RFC3339. An
// empty value means "everything" (the zero time). A malformed non-empty
// value responds 400 and returns ok=false.
func parseSince(c *gin.Context) (time.Time, bool) {
	value := c.Query("since")
	if value == "" {
		return time.Time{}, true
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid since parameter"})
		return time.Time{}, false
	}
	return parsed, true
}
