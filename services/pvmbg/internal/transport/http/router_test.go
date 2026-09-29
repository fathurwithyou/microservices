package httptransport

import (
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fathurwithyou/microservices/services/pvmbg/internal/state"
	"github.com/gin-gonic/gin"
)

func testRuntime(t *testing.T) *state.Runtime {
	t.Helper()
	runtime, err := state.New(state.Config{SeedCount: 2})
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	return runtime
}

func testRouter(runtime *state.Runtime) *gin.Engine {
	return NewRouter(RouterConfig{State: runtime, Token: "pvmbg-token", AdminKey: "admin-key", Rand: rand.New(rand.NewPCG(1, 2)), Logger: slog.Default()})
}

func TestVolcanicReportsRequireBearerToken(t *testing.T) {
	router := testRouter(testRuntime(t))
	request := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestVolcanicReportsRejectMalformedSince(t *testing.T) {
	router := testRouter(testRuntime(t))
	request := httptest.NewRequest(http.MethodGet, "/volcanic-reports?since=not-a-time", nil)
	request.Header.Set("Authorization", "Bearer pvmbg-token")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestSchemaVersionAddsConfidenceLevel(t *testing.T) {
	runtime := testRuntime(t)
	router := testRouter(runtime)
	adminRequest := httptest.NewRequest(http.MethodPost, "/admin/schema-version", strings.NewReader(`{"enabled":true}`))
	adminRequest.Header.Set("X-PVMBG-Admin-Key", "admin-key")
	adminRequest.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(httptest.NewRecorder(), adminRequest)

	reportRequest := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	reportRequest.Header.Set("Authorization", "Bearer pvmbg-token")
	reportResponse := httptest.NewRecorder()
	router.ServeHTTP(reportResponse, reportRequest)

	if reportResponse.Code != http.StatusOK || !strings.Contains(reportResponse.Body.String(), "confidence_level") {
		t.Fatalf("schema response = %d %s", reportResponse.Code, reportResponse.Body.String())
	}
}

func TestAdminRejectsMalformedBody(t *testing.T) {
	runtime := testRuntime(t)
	router := testRouter(runtime)
	for _, path := range []string{"/admin/schema-version", "/admin/outage"} {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{invalid`))
		request.Header.Set("X-PVMBG-Admin-Key", "admin-key")
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d", path, response.Code, http.StatusBadRequest)
		}
	}
	// Simulation flags must be unchanged by the malformed bodies.
	if runtime.IsOutage() {
		t.Fatal("malformed outage body must not enable outage")
	}
}
