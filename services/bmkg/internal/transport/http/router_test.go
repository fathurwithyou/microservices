package httptransport

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fathurwithyou/microservices/services/bmkg/internal/store"
)

func TestSeismicEventsRequireBMKGCredential(t *testing.T) {
	router := NewRouter(store.New(2), "bmkg-key", 0, slog.Default())
	request := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestSeismicEventsReturnSeededData(t *testing.T) {
	router := NewRouter(store.New(2), "bmkg-key", 0, slog.Default())
	request := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	request.Header.Set("X-BMKG-Key", "bmkg-key")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
}
