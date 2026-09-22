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
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"name":"dashboard.hazard-events","messages":4,"consumers":1,"messages_ready":4}`))
	}))
	t.Cleanup(management.Close)
	router := newTestRouter(t, config.Config{
		RabbitManagementURL: management.URL,
		RabbitUser:          "guest",
		RabbitPassword:      "secret",
		RequestTimeout:      time.Second,
	}, nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/observability/queues", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
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
