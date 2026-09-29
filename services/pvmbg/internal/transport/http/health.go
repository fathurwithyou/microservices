package httptransport

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// health reports liveness, or outage status when outage simulation is on.
// The admin endpoints intentionally stay available during an outage so the
// simulation can be switched off.
func (h handler) health(c *gin.Context) {
	if h.state.IsOutage() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "outage", "service": serviceName})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": serviceName})
}
