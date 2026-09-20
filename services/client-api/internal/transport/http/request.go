package httptransport

import (
	"context"
	"strconv"

	"github.com/gin-gonic/gin"
)

func (h handler) requestContext(c *gin.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.Request.Context(), h.timeout)
}

func requestScopes(c *gin.Context) []string {
	value, _ := c.Get("scopes")
	scopes, _ := value.([]string)
	return scopes
}

func requestsRawData(c *gin.Context) bool {
	return c.Query("raw") == "true" || c.Query("raw") == "1"
}

func pageSize(value string) int {
	if value == "" {
		return 50
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 50
	}
	if parsed > 100 {
		return 100
	}
	return parsed
}
