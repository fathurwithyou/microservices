package httptransport

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/gin-gonic/gin"
)

type healthProbe struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	Detail   string `json:"detail,omitempty"`
	Optional bool   `json:"optional,omitempty"`
}

// probeTarget describes one dependency checked by serviceHealth.
type probeTarget struct {
	name     string
	kind     string
	target   string
	optional bool
}

func (h handler) serviceHealth(c *gin.Context) {
	probes := []probeTarget{
		{"client-api", "http", h.cfg.ClientAPIHealthURL, false},
		{"aggregator", "http", h.cfg.AggregatorHealthURL, false},
		{"bmkg", "http", h.cfg.BMKGHealthURL, false},
		{"pvmbg", "http", h.cfg.PVMBGHealthURL, false},
		{"auth", "tcp", h.cfg.AuthAddr, false},
		{"dashboard-consumer", "http", h.cfg.DashboardConsumerURL, false},
		{"notifier-consumer", "http", h.cfg.NotifierConsumerURL, false},
		{"portal-consumer", "http", h.cfg.PortalConsumerURL, true},
	}
	ctx := c.Request.Context()
	correlationID := c.GetString(correlation.GinKey)
	results := make([]healthProbe, len(probes))
	var waitGroup sync.WaitGroup
	for index, probe := range probes {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			results[index] = h.probe(ctx, correlationID, probe)
		}()
	}
	waitGroup.Wait()
	c.JSON(http.StatusOK, gin.H{"checked_at": h.now().UTC().Format(time.RFC3339), "services": results})
}

func (h handler) probe(ctx context.Context, correlationID string, probe probeTarget) healthProbe {
	result := healthProbe{Name: probe.name, Optional: probe.optional, Status: "down"}
	if probe.kind == "tcp" {
		connection, err := net.DialTimeout("tcp", probe.target, h.cfg.RequestTimeout)
		if err != nil {
			result.Detail = "tcp unreachable"
			return result
		}
		_ = connection.Close()
		result.Status = "healthy"
		return result
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, probe.target, nil)
	if err != nil {
		result.Detail = "invalid health URL"
		return result
	}
	request.Header.Set(correlation.Header, correlationID)
	response, err := h.client.Do(request)
	if err != nil {
		result.Detail = "unreachable"
		return result
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		result.Status = "healthy"
		return result
	}
	result.Detail = http.StatusText(response.StatusCode)
	if probe.name == "pvmbg" && response.StatusCode == http.StatusServiceUnavailable {
		result.Status = "outage"
		result.Detail = "PVMBG outage switch is enabled"
	}
	return result
}
