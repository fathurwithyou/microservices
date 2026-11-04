package httptransport

import (
	"net/http"

	"github.com/fathurwithyou/microservices/services/client-api/internal/ports"
	"github.com/gin-gonic/gin"
)

func (h handler) analytics(c *gin.Context) {
	principal, ok := requestPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}
	bucket := c.DefaultQuery("bucket", "day")
	switch bucket {
	case "hour", "day", "week":
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "bucket must be hour, day, or week"})
		return
	}
	ctx, cancel := h.contextWithCorrelation(c)
	defer cancel()
	result, err := h.api.Analytics(ctx, principal, ports.AnalyticsFilter{
		Since:  c.Query("since"),
		Until:  c.Query("until"),
		Bucket: bucket,
	})
	if err != nil {
		writeAPIError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}
