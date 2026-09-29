package httptransport

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// health reports service liveness. BMKG has no outage simulation, so it
// always returns 200.
func (h handler) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": serviceName})
}
