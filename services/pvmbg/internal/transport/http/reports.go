package httptransport

import (
	"crypto/rand"
	"math/big"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func (h handler) health(c *gin.Context) {
	if h.state.IsOutage() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "outage", "service": "pvmbg"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "pvmbg"})
}

func (h handler) volcanicReports(c *gin.Context) {
	if h.state.IsOutage() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "pvmbg outage"})
		return
	}
	if !waitForDelay(c, randomDelay(h.state.DelayRange())) {
		return
	}

	started := time.Now()
	c.JSON(http.StatusOK, h.state.ReportsSince(parseSince(c.Query("since"))))
	h.logger.Info("outbound response", "service", "pvmbg", "component", "http", "operation", "volcanic-reports", "latency_ms", time.Since(started).Milliseconds(), "status", http.StatusOK, "correlation_id", c.GetString("correlation_id"))
}

func waitForDelay(c *gin.Context, delay time.Duration) bool {
	select {
	case <-time.After(delay):
		return true
	case <-c.Request.Context().Done():
		return false
	}
}

func randomDelay(minDelay, maxDelay time.Duration) time.Duration {
	if maxDelay <= minDelay {
		return minDelay
	}
	span := big.NewInt(int64(maxDelay - minDelay))
	value, err := rand.Int(rand.Reader, span)
	if err != nil {
		return minDelay
	}
	return minDelay + time.Duration(value.Int64())
}

func parseSince(value string) time.Time {
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed
	}
	return time.Time{}
}
