package httptransport

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type enabledRequest struct {
	Enabled *bool `json:"enabled"`
}

func (h handler) schemaVersion(c *gin.Context) {
	enabled := bindEnabled(c)
	h.state.SetSchemaV2(enabled)
	c.JSON(http.StatusOK, gin.H{"schema_version": 2, "confidence_level": enabled})
}

func (h handler) outage(c *gin.Context) {
	enabled := bindEnabled(c)
	h.state.SetOutage(enabled)
	c.JSON(http.StatusOK, gin.H{"outage": enabled})
}

func bindEnabled(c *gin.Context) bool {
	request := enabledRequest{}
	if c.Request.Body != nil && c.Request.ContentLength != 0 {
		if err := c.ShouldBindJSON(&request); err == nil && request.Enabled != nil {
			return *request.Enabled
		}
	}
	return true
}
