package httptransport

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/demo-api/internal/config"
)

func TestHealth(t *testing.T) {
	router := newTestRouter(t, config.Config{RequestTimeout: time.Second}, nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestSchemaVersionProxiesAdminKey(t *testing.T) {
	var seenKey string
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		seenKey = request.Header.Get("X-PVMBG-Admin-Key")
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"schema_version":2,"confidence_level":true}`))
	}))
	t.Cleanup(upstream.Close)
	router := newTestRouter(t, config.Config{PVMBGURL: upstream.URL, PVMBGAdminKey: "admin-secret", RequestTimeout: time.Second}, nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/admin/schema-version", strings.NewReader(`{"enabled":true}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	if seenKey != "admin-secret" {
		t.Fatalf("admin key = %q", seenKey)
	}
}

func TestCrossCredentialsRejectsForeignKeys(t *testing.T) {
	bmkg := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.WriteHeader(http.StatusUnauthorized)
		_, _ = response.Write([]byte(`{"error":"invalid BMKG credential"}`))
	}))
	t.Cleanup(bmkg.Close)
	pvmbg := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.WriteHeader(http.StatusUnauthorized)
		_, _ = response.Write([]byte(`{"error":"invalid PVMBG credential"}`))
	}))
	t.Cleanup(pvmbg.Close)
	router := newTestRouter(t, config.Config{
		BMKGURL:        bmkg.URL,
		PVMBGURL:       pvmbg.URL,
		BMKGAPIKey:     "bmkg-key",
		PVMBGToken:     "pvmbg-token",
		RequestTimeout: time.Second,
	}, nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/trust/cross-credentials", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	body := decode(t, response.Body)
	if body["passed"] != true {
		t.Fatalf("passed = %#v body=%s", body["passed"], response.Body.String())
	}
	encoded, _ := json.Marshal(body)
	if strings.Contains(string(encoded), "bmkg-key") || strings.Contains(string(encoded), "pvmbg-token") {
		t.Fatal("response leaked source credentials")
	}
}

func TestOwnershipDeclaresNoStoreAccess(t *testing.T) {
	router := newTestRouter(t, config.Config{RequestTimeout: time.Second}, nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/ownership", nil))
	body := decode(t, response.Body)
	if body["canonical_store_owner"] != "aggregator" {
		t.Fatalf("owner = %#v", body["canonical_store_owner"])
	}
	if body["demo_api_database_access"] != false {
		t.Fatal("demo-api must not claim database access")
	}
}

func TestArtifactPathTraversalRejected(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "p1.json"), []byte(`{"problem":"P1"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	router := newTestRouter(t, config.Config{ArtifactsDir: dir, RequestTimeout: time.Second}, nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/artifacts/../secret.json", nil))
	if response.Code == http.StatusOK {
		t.Fatal("path traversal must not succeed")
	}
}

func TestQueuesMapsManagementAPI(t *testing.T) {
	management := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		user, password, ok := request.BasicAuth()
		if !ok || user != "guest" || password != "secret" {
			response.WriteHeader(http.StatusUnauthorized)
			return
		}
		name := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"name":"` + name + `","messages":4,"consumers":1,"messages_ready":4}`))
	}))
	t.Cleanup(management.Close)
	router := newTestRouter(t, config.Config{
		RabbitManagementURL: management.URL,
		RabbitUser:          "guest",
		RabbitPassword:      "secret",
		RequestTimeout:      time.Second,
		QueueNames:          []string{"dashboard.hazard-events", "notifier.hazard-events", "portal.hazard-events"},
	}, nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/observability/queues", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	body := decode(t, response.Body)
	queues, ok := body["queues"].([]any)
	if !ok || len(queues) != 3 {
		t.Fatalf("queues = %#v", body["queues"])
	}
	for index, expected := range []string{"dashboard.hazard-events", "notifier.hazard-events", "portal.hazard-events"} {
		snapshot := queues[index].(map[string]any)
		if snapshot["name"] != expected {
			t.Fatalf("queues[%d].name = %v want %s", index, snapshot["name"], expected)
		}
	}
}

func TestQueuesPreservesOrderUnderParallelReads(t *testing.T) {
	delays := map[string]time.Duration{
		"dashboard.hazard-events": 200 * time.Millisecond,
		"notifier.hazard-events":  100 * time.Millisecond,
		"portal.hazard-events":    0,
	}
	management := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		name := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		time.Sleep(delays[name])
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"name":"` + name + `","messages":1,"consumers":1,"messages_ready":1}`))
	}))
	t.Cleanup(management.Close)
	router := newTestRouter(t, config.Config{
		RabbitManagementURL: management.URL,
		RabbitUser:          "guest",
		RabbitPassword:      "secret",
		RequestTimeout:      time.Second,
		QueueNames:          []string{"dashboard.hazard-events", "notifier.hazard-events", "portal.hazard-events"},
	}, nil)
	started := time.Now()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/observability/queues", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	if elapsed := time.Since(started); elapsed > 280*time.Millisecond {
		t.Fatalf("queue reads look sequential: elapsed = %s", elapsed)
	}
	body := decode(t, response.Body)
	queues := body["queues"].([]any)
	for index, expected := range []string{"dashboard.hazard-events", "notifier.hazard-events", "portal.hazard-events"} {
		snapshot := queues[index].(map[string]any)
		if snapshot["name"] != expected {
			t.Fatalf("queues[%d].name = %v want %s", index, snapshot["name"], expected)
		}
	}
}

func TestServiceHealthAggregatesProbeTargets(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if strings.Contains(request.URL.Path, "pvmbg") {
			response.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	authAddr := strings.TrimPrefix(upstream.URL, "http://")
	router := newTestRouter(t, config.Config{
		ClientAPIHealthURL:   upstream.URL + "/client",
		AggregatorHealthURL:  upstream.URL + "/aggregator",
		BMKGHealthURL:        upstream.URL + "/bmkg",
		PVMBGHealthURL:       upstream.URL + "/pvmbg",
		AuthAddr:             authAddr,
		DashboardConsumerURL: upstream.URL + "/dashboard",
		NotifierConsumerURL:  upstream.URL + "/notifier",
		PortalConsumerURL:    upstream.URL + "/portal",
		RequestTimeout:       time.Second,
	}, nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/observability/health", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	body := decode(t, response.Body)
	services, ok := body["services"].([]any)
	if !ok || len(services) != 8 {
		t.Fatalf("services = %#v", body["services"])
	}
	statuses := map[string]map[string]any{}
	for _, entry := range services {
		probe := entry.(map[string]any)
		statuses[probe["name"].(string)] = probe
	}
	if statuses["client-api"]["status"] != "healthy" {
		t.Fatalf("client-api = %#v", statuses["client-api"])
	}
	if statuses["auth"]["status"] != "healthy" {
		t.Fatalf("auth = %#v", statuses["auth"])
	}
	if statuses["pvmbg"]["status"] != "outage" || statuses["pvmbg"]["detail"] != "PVMBG outage switch is enabled" {
		t.Fatalf("pvmbg = %#v", statuses["pvmbg"])
	}
	if statuses["portal-consumer"]["optional"] != true {
		t.Fatalf("portal-consumer = %#v", statuses["portal-consumer"])
	}
}

func TestServiceHealthMarksUnreachableTargetsDown(t *testing.T) {
	router := newTestRouter(t, config.Config{
		ClientAPIHealthURL:   "http://127.0.0.1:1/health",
		AggregatorHealthURL:  "http://127.0.0.1:1/health",
		BMKGHealthURL:        "http://127.0.0.1:1/health",
		PVMBGHealthURL:       "http://127.0.0.1:1/health",
		AuthAddr:             "127.0.0.1:1",
		DashboardConsumerURL: "http://127.0.0.1:1/health",
		NotifierConsumerURL:  "http://127.0.0.1:1/health",
		PortalConsumerURL:    "http://127.0.0.1:1/health",
		RequestTimeout:       time.Second,
	}, nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/observability/health", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	body := decode(t, response.Body)
	for _, entry := range body["services"].([]any) {
		probe := entry.(map[string]any)
		if probe["status"] != "down" {
			t.Fatalf("probe %v status = %v want down", probe["name"], probe["status"])
		}
	}
}

func TestListArtifactsUnavailableReturns500(t *testing.T) {
	router := newTestRouter(t, config.Config{ArtifactsDir: filepath.Join(t.TempDir(), "missing"), RequestTimeout: time.Second}, nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/artifacts", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	body := decode(t, response.Body)
	if body["error"] != "artifacts unavailable" {
		t.Fatalf("error = %#v", body["error"])
	}
}

func newTestRouter(t *testing.T, cfg config.Config, client *http.Client) http.Handler {
	t.Helper()
	return NewRouter(RouterConfig{Config: cfg, Client: client, Logger: slog.Default(), Now: func() time.Time { return time.Date(2026, 9, 22, 8, 0, 0, 0, time.UTC) }})
}

func decode(t *testing.T, body io.Reader) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.NewDecoder(body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}
