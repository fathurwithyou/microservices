package httptransport

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/fathurwithyou/microservices/services/pvmbg/internal/state"
	"github.com/gin-gonic/gin"
)

type enabledRequest struct {
	Enabled *bool  `json:"enabled"`
	Mode    string `json:"mode"`
	// Count is how many responses carry a malformed mode (default 1).
	Count int `json:"count"`
	// Version selects the schema by number, the form the course Tester Agent
	// sends ({"version":2}): 1 is the original schema, 2 adds
	// confidence_level, 3 and above is the incompatible schema. A number or
	// a numeric string is accepted.
	Version json.RawMessage `json:"version"`
}

// versionOf reads the version field; ok is false when it is absent.
func versionOf(raw json.RawMessage) (version int, ok bool, err error) {
	text := strings.Trim(strings.TrimSpace(string(raw)), `"`)
	if text == "" || text == "null" {
		return 0, false, nil
	}
	text = strings.TrimPrefix(strings.ToLower(text), "v")
	number, err := strconv.Atoi(text)
	if err != nil || number < 1 {
		return 0, true, fmt.Errorf("version must be 1, 2 or 3")
	}
	return number, true, nil
}

func (h handler) schemaVersion(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	// The body shape an external caller sends is part of what an operator
	// needs to see when a schema change was triggered.
	if h.logger != nil {
		preview := body
		if len(preview) > 512 {
			preview = preview[:512]
		}
		h.logger.Info("admin schema-version requested", "service", serviceName, "component", "admin", "body", string(preview), "content_type", c.GetHeader("Content-Type"))
	}
	request := enabledRequest{}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
			return
		}
	} else {
		enabled := true
		request.Enabled = &enabled
	}
	if version, given, err := versionOf(request.Version); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	} else if given && request.Mode == "" && request.Enabled == nil {
		switch {
		case version == 1:
			h.state.Reset()
			c.JSON(http.StatusOK, gin.H{"schema_version": 1, "mode": "normal"})
		case version == 2:
			h.state.Reset()
			h.state.SetSchemaV2(true)
			c.JSON(http.StatusOK, gin.H{"schema_version": 2, "confidence_level": true})
		default:
			h.state.SetBreaking(true)
			c.JSON(http.StatusOK, gin.H{"schema_version": 3, "mode": "breaking"})
		}
		return
	}
	switch request.Mode {
	case "breaking":
		// Incompatible schema: renamed field, changed type, missing required
		// field. Persists until another mode is selected.
		h.state.SetBreaking(true)
		c.JSON(http.StatusOK, gin.H{"schema_version": 3, "mode": "breaking"})
		return
	case "normal", "v1":
		h.state.Reset()
		c.JSON(http.StatusOK, gin.H{"schema_version": 1, "mode": "normal"})
		return
	case "":
	default:
		kind, ok := state.ParseMalformedKind(request.Mode)
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "mode must be one of breaking, normal, truncated, deep_nesting, invalid_utf8, oversized"})
			return
		}
		count := request.Count
		if count < 1 {
			count = 1
		}
		h.state.SetMalformed(kind, count)
		c.JSON(http.StatusOK, gin.H{"mode": string(kind), "count": count})
		return
	}
	if request.Enabled == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	h.state.SetSchemaV2(*request.Enabled)
	c.JSON(http.StatusOK, gin.H{"schema_version": 2, "confidence_level": *request.Enabled})
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
