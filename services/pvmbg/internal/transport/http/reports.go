package httptransport

import (
	"context"
	"net/http"
	"time"

	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/gin-gonic/gin"
)

// volcanicReports serves reports observed since the ?since= cursor. A
// malformed non-empty cursor is a 400: silently returning the full history
// would surprise pollers and hide client bugs.
func (h handler) volcanicReports(c *gin.Context) {
	if h.state.IsOutage() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "pvmbg outage"})
		return
	}
	since, ok := parseSince(c.Query("since"))
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid since parameter"})
		return
	}
	if !waitDelay(c.Request.Context(), h.randomDelay()) {
		return
	}

	started := time.Now()
	c.JSON(http.StatusOK, h.state.ReportsSince(since))
	h.logger.Info("outbound response", "service", serviceName, "component", "http", "operation", "volcanic-reports", "latency_ms", time.Since(started).Milliseconds(), "status", http.StatusOK, "correlation_id", c.GetString(correlation.GinKey))
}

// parseSince parses an RFC3339 cursor. An empty value means "from the
// beginning". The second return value is false for malformed input.
func parseSince(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, true
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

// waitDelay sleeps for d unless ctx is cancelled first, reporting whether
// the full delay elapsed.
func waitDelay(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}
