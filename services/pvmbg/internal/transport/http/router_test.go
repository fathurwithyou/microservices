package httptransport

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fathurwithyou/microservices/services/pvmbg/internal/store"
)

func TestVolcanicReportsRequireBearerToken(t *testing.T) {
	router := NewRouter(store.New(0, 0, 2), "pvmbg-token", "admin-key", slog.Default())
	request := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestSchemaVersionAddsConfidenceLevel(t *testing.T) {
	router := NewRouter(store.New(0, 0, 2), "pvmbg-token", "admin-key", slog.Default())
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
