package httptransport

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/gin-gonic/gin"
)

func (h handler) schemaVersion(c *gin.Context) {
	h.proxyAdmin(c, "/admin/schema-version")
}

func (h handler) outage(c *gin.Context) {
	h.proxyAdmin(c, "/admin/outage")
}

func (h handler) proxyAdmin(c *gin.Context, path string) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	if len(body) == 0 {
		body = []byte(`{"enabled":true}`)
	}
	request, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, strings.TrimRight(h.cfg.PVMBGURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "unable to reach PVMBG admin"})
		return
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-PVMBG-Admin-Key", h.cfg.PVMBGAdminKey)
	request.Header.Set(correlation.Header, c.GetString(correlation.GinKey))
	response, err := h.client.Do(request)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "PVMBG admin unavailable"})
		return
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		h.logger.Error("failed to read PVMBG admin response", "service", serviceName, "component", "http", "correlation_id", c.GetString(correlation.GinKey), "error", err)
		c.JSON(http.StatusBadGateway, gin.H{"error": "PVMBG admin unavailable"})
		return
	}
	c.Data(response.StatusCode, "application/json", payload)
}

func (h handler) ownership(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"canonical_store_owner":      "aggregator",
		"demo_api_database_access":   false,
		"demo_api_docker_access":     false,
		"client_api_database_access": false,
		"statement":                  "Canonical Store is owned by Aggregator. Client-Facing API and demo-api obtain hazard data through Aggregator APIs only. demo-api does not hold a database URL or Docker socket.",
	})
}
