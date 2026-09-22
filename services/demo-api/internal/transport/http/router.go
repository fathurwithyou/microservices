package httptransport

import (
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/demo-api/internal/config"
	"github.com/gin-gonic/gin"
)

type RouterConfig struct {
	Config config.Config
	Client *http.Client
	Logger *slog.Logger
	Now    func() time.Time
}

func NewRouter(cfg RouterConfig) *gin.Engine {
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: cfg.Config.RequestTimeout}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	handler := handler{cfg: cfg.Config, client: cfg.Client, logger: cfg.Logger, now: cfg.Now}
	router := gin.New()
	router.Use(correlationMiddleware(), recoveryMiddleware(cfg.Logger), requestLogMiddleware(cfg.Logger))
	router.GET("/health", handler.health)
	router.POST("/admin/schema-version", handler.schemaVersion)
	router.POST("/admin/outage", handler.outage)
	router.GET("/observability/health", handler.serviceHealth)
	router.GET("/observability/queues", handler.queues)
	router.POST("/trust/cross-credentials", handler.crossCredentials)
	router.GET("/artifacts", handler.listArtifacts)
	router.GET("/artifacts/:name", handler.getArtifact)
	router.GET("/ownership", handler.ownership)
	return router
}

type handler struct {
	cfg    config.Config
	client *http.Client
	logger *slog.Logger
	now    func() time.Time
}

type healthProbe struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	Detail   string `json:"detail,omitempty"`
	Optional bool   `json:"optional,omitempty"`
}

type queueSnapshot struct {
	Name      string `json:"name"`
	Messages  int    `json:"messages"`
	Consumers int    `json:"consumers"`
	Ready     int    `json:"ready"`
}

type trustCase struct {
	Name           string `json:"name"`
	Target         string `json:"target"`
	CredentialUsed string `json:"credential_used"`
	StatusCode     int    `json:"status_code"`
	Passed         bool   `json:"passed"`
}

func (h handler) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "demo-api"})
}

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
	request, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, strings.TrimRight(h.cfg.PVMBGURL, "/")+path, strings.NewReader(string(body)))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "unable to reach PVMBG admin"})
		return
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-PVMBG-Admin-Key", h.cfg.PVMBGAdminKey)
	request.Header.Set("X-Correlation-ID", c.GetString("correlation_id"))
	response, err := h.client.Do(request)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "PVMBG admin unavailable"})
		return
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(response.Body)
	c.Data(response.StatusCode, "application/json", payload)
}

func (h handler) serviceHealth(c *gin.Context) {
	probes := []struct {
		name     string
		kind     string
		target   string
		optional bool
	}{
		{"client-api", "http", h.cfg.ClientAPIHealthURL, false},
		{"aggregator", "http", h.cfg.AggregatorHealthURL, false},
		{"bmkg", "http", h.cfg.BMKGHealthURL, false},
		{"pvmbg", "http", h.cfg.PVMBGHealthURL, false},
		{"auth", "tcp", h.cfg.AuthAddr, false},
		{"dashboard-consumer", "http", h.cfg.DashboardConsumerURL, false},
		{"notifier-consumer", "http", h.cfg.NotifierConsumerURL, false},
		{"portal-consumer", "http", h.cfg.PortalConsumerURL, true},
	}
	results := make([]healthProbe, len(probes))
	var waitGroup sync.WaitGroup
	for index, probe := range probes {
		waitGroup.Add(1)
		go func(index int, probe struct {
			name     string
			kind     string
			target   string
			optional bool
		}) {
			defer waitGroup.Done()
			results[index] = h.probe(c, probe.name, probe.kind, probe.target, probe.optional)
		}(index, probe)
	}
	waitGroup.Wait()
	c.JSON(http.StatusOK, gin.H{"checked_at": h.now().UTC().Format(time.RFC3339), "services": results})
}

func (h handler) probe(c *gin.Context, name, kind, target string, optional bool) healthProbe {
	result := healthProbe{Name: name, Optional: optional, Status: "down"}
	if kind == "tcp" {
		connection, err := net.DialTimeout("tcp", target, h.cfg.RequestTimeout)
		if err != nil {
			result.Detail = "tcp unreachable"
			return result
		}
		_ = connection.Close()
		result.Status = "healthy"
		return result
	}
	request, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, target, nil)
	if err != nil {
		result.Detail = "invalid health URL"
		return result
	}
	request.Header.Set("X-Correlation-ID", c.GetString("correlation_id"))
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
	if name == "pvmbg" && response.StatusCode == http.StatusServiceUnavailable {
		result.Status = "outage"
		result.Detail = "PVMBG outage switch is enabled"
	}
	return result
}

func (h handler) queues(c *gin.Context) {
	names := []string{"dashboard.hazard-events", "notifier.hazard-events", "portal.hazard-events"}
	snapshots := make([]queueSnapshot, 0, len(names))
	for _, name := range names {
		snapshot, err := h.readQueue(c, name)
		if err != nil {
			snapshots = append(snapshots, queueSnapshot{Name: name})
			continue
		}
		snapshots = append(snapshots, snapshot)
	}
	c.JSON(http.StatusOK, gin.H{"checked_at": h.now().UTC().Format(time.RFC3339), "queues": snapshots})
}

func (h handler) readQueue(c *gin.Context, name string) (queueSnapshot, error) {
	url := strings.TrimRight(h.cfg.RabbitManagementURL, "/") + "/api/queues/%2F/" + name
	request, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, url, nil)
	if err != nil {
		return queueSnapshot{}, err
	}
	request.SetBasicAuth(h.cfg.RabbitUser, h.cfg.RabbitPassword)
	request.Header.Set("X-Correlation-ID", c.GetString("correlation_id"))
	response, err := h.client.Do(request)
	if err != nil {
		return queueSnapshot{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return queueSnapshot{}, errStatus(response.StatusCode)
	}
	var payload struct {
		Name                   string `json:"name"`
		Messages               int    `json:"messages"`
		Consumers              int    `json:"consumers"`
		MessagesReady          int    `json:"messages_ready"`
		MessagesUnacknowledged int    `json:"messages_unacknowledged"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return queueSnapshot{}, err
	}
	return queueSnapshot{Name: payload.Name, Messages: payload.Messages, Consumers: payload.Consumers, Ready: payload.MessagesReady}, nil
}

type statusError int

func errStatus(code int) error { return statusError(code) }
func (s statusError) Error() string {
	return http.StatusText(int(s))
}

func (h handler) crossCredentials(c *gin.Context) {
	cases := []trustCase{
		h.probeCredential(c, "BMKG key rejected by PVMBG", strings.TrimRight(h.cfg.PVMBGURL, "/")+"/volcanic-reports", map[string]string{"Authorization": "Bearer " + h.cfg.BMKGAPIKey}, "BMKG API key as PVMBG bearer"),
		h.probeCredential(c, "PVMBG token rejected by BMKG", strings.TrimRight(h.cfg.BMKGURL, "/")+"/seismic-events", map[string]string{"X-BMKG-Key": h.cfg.PVMBGToken}, "PVMBG token as BMKG key"),
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

func (h handler) probeCredential(c *gin.Context, name, url string, headers map[string]string, credentialUsed string) trustCase {
	result := trustCase{Name: name, Target: url, CredentialUsed: credentialUsed}
	request, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, url, nil)
	if err != nil {
		return result
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	request.Header.Set("X-Correlation-ID", c.GetString("correlation_id"))
	response, err := h.client.Do(request)
	if err != nil {
		return result
	}
	defer response.Body.Close()
	result.StatusCode = response.StatusCode
	return result
}

func (h handler) listArtifacts(c *gin.Context) {
	entries, err := os.ReadDir(h.cfg.ArtifactsDir)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"artifacts": []any{}})
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

func (h handler) ownership(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"canonical_store_owner":      "aggregator",
		"demo_api_database_access":   false,
		"demo_api_docker_access":     false,
		"client_api_database_access": false,
		"statement":                  "Canonical Store is owned by Aggregator. Client-Facing API and demo-api obtain hazard data through Aggregator APIs only. demo-api does not hold a database URL or Docker socket.",
	})
}
