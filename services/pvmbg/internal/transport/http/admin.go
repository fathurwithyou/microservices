package httptransport

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

type enabledRequest struct {
	Enabled *bool `json:"enabled"`
}

func (h handler) schemaVersion(c *gin.Context) {
	enabled, ok := bindEnabled(c)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	h.state.SetSchemaV2(enabled)
	c.JSON(http.StatusOK, gin.H{"schema_version": 2, "confidence_level": enabled})
}

func (h handler) outage(c *gin.Context) {
	enabled, ok := bindEnabled(c)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	h.state.SetOutage(enabled)
	c.JSON(http.StatusOK, gin.H{"outage": enabled})
}

// bindEnabled decodes the {"enabled": bool} body. An empty body preserves
// the historical default of true; malformed JSON is a client error instead
// of silently enabling the simulation. Chunked bodies are supported because
// the body is read directly instead of relying on ContentLength.
func bindEnabled(c *gin.Context) (bool, bool) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return false, false
	}
	if len(body) == 0 {
		return true, true
	}
	var request enabledRequest
	if err := json.Unmarshal(body, &request); err != nil || request.Enabled == nil {
		return false, false
	}
	return *request.Enabled, true
}
