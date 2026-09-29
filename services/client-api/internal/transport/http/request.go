package httptransport

import (
	"context"
	"strconv"

	"github.com/gin-gonic/gin"
)

// principalKey is the gin context key under which the authenticate
// middleware stores the application Principal.
const principalKey = "principal"

// Page size bounds for hazard listing.
const (
	defaultPageSize = 50
	maxPageSize     = 100
)

// rawQueryValues are the accepted truthy spellings of the ?raw= parameter.
var rawQueryValues = map[string]bool{"true": true, "1": true}

func (h handler) requestContext(c *gin.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.Request.Context(), h.timeout)
}

func requestsRawData(c *gin.Context) bool {
	return rawQueryValues[c.Query("raw")]
}

func pageSize(value string) int {
	if value == "" {
		return defaultPageSize
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return defaultPageSize
	}
	return min(parsed, maxPageSize)
}
