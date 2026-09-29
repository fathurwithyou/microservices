package httptransport

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/gin-gonic/gin"
)

func (h handler) listArtifacts(c *gin.Context) {
	entries, err := os.ReadDir(h.cfg.ArtifactsDir)
	if err != nil {
		h.logger.Error("artifacts directory unreadable", "service", serviceName, "component", "http", "correlation_id", c.GetString(correlation.GinKey), "dir", h.cfg.ArtifactsDir, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "artifacts unavailable"})
		return
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
			names = append(names, entry.Name())
		}
	}
	c.JSON(http.StatusOK, gin.H{"artifacts": names})
}

func (h handler) getArtifact(c *gin.Context) {
	name := filepath.Base(c.Param("name"))
	if name == "." || name == string(filepath.Separator) || !strings.HasSuffix(strings.ToLower(name), ".json") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "artifact name must be a json file"})
		return
	}
	payload, err := os.ReadFile(filepath.Join(h.cfg.ArtifactsDir, name))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "artifact not found"})
		return
	}
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "artifact is not valid JSON"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"name": name, "content": decoded})
}
