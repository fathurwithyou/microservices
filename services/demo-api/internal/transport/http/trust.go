package httptransport

import (
	"context"
	"net/http"
	"strings"

	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/gin-gonic/gin"
)

type trustCase struct {
	Name           string `json:"name"`
	Target         string `json:"target"`
	CredentialUsed string `json:"credential_used"`
	StatusCode     int    `json:"status_code"`
	Passed         bool   `json:"passed"`
}

func (h handler) crossCredentials(c *gin.Context) {
	ctx := c.Request.Context()
	correlationID := c.GetString(correlation.GinKey)
	cases := []trustCase{
		h.probeCredential(ctx, correlationID, "BMKG key rejected by PVMBG", strings.TrimRight(h.cfg.PVMBGURL, "/")+"/volcanic-reports", map[string]string{"Authorization": "Bearer " + h.cfg.BMKGAPIKey}, "BMKG API key as PVMBG bearer"),
		h.probeCredential(ctx, correlationID, "PVMBG token rejected by BMKG", strings.TrimRight(h.cfg.BMKGURL, "/")+"/seismic-events", map[string]string{"X-BMKG-Key": h.cfg.PVMBGToken}, "PVMBG token as BMKG key"),
	}
	passed := true
	for index := range cases {
		cases[index].Passed = cases[index].StatusCode == http.StatusUnauthorized || cases[index].StatusCode == http.StatusForbidden
		if !cases[index].Passed {
			passed = false
		}
	}
	c.JSON(http.StatusOK, gin.H{"passed": passed, "cases": cases})
}

func (h handler) probeCredential(ctx context.Context, correlationID, name, url string, headers map[string]string, credentialUsed string) trustCase {
	result := trustCase{Name: name, Target: url, CredentialUsed: credentialUsed}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		h.logger.Error("trust probe request build failed", "service", serviceName, "component", "http", "correlation_id", correlationID, "case", name, "error", err)
		return result
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	request.Header.Set(correlation.Header, correlationID)
	response, err := h.client.Do(request)
	if err != nil {
		h.logger.Error("trust probe request failed", "service", serviceName, "component", "http", "correlation_id", correlationID, "case", name, "error", err)
		return result
	}
	defer response.Body.Close()
	result.StatusCode = response.StatusCode
	return result
}
