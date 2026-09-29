package httptransport

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/bmkg/internal/state"
)

func newTestRouter(t *testing.T) *RouterConfig {
	t.Helper()
	return &RouterConfig{
		State:  state.New(state.Config{SeedCount: 2, Now: func() time.Time { return time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC) }}),
		APIKey: "bmkg-key",
		Logger: slog.Default(),
	}
}

func TestSeismicEventsRequireBMKGCredential(t *testing.T) {
	router := NewRouter(*newTestRouter(t))
	request := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	if !strings.Contains(response.Body.String(), "invalid BMKG credential") {
		t.Fatalf("body = %s, want invalid BMKG credential", response.Body.String())
	}
}

func TestSeismicEventsReturnSeededData(t *testing.T) {
	router := NewRouter(*newTestRouter(t))
	request := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	request.Header.Set("X-BMKG-Key", "bmkg-key")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	if !strings.Contains(response.Body.String(), "bmkg-seismic-1") {
		t.Fatalf("body = %s, want seeded events", response.Body.String())
	}
}

func TestSeismicEventsMalformedSinceReturns400(t *testing.T) {
	router := NewRouter(*newTestRouter(t))
	request := httptest.NewRequest(http.MethodGet, "/seismic-events?since=yesterday", nil)
	request.Header.Set("X-BMKG-Key", "bmkg-key")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if !strings.Contains(response.Body.String(), "invalid since parameter") {
		t.Fatalf("body = %s, want invalid since parameter", response.Body.String())
	}
}

func TestSeismicEventsValidSinceFilters(t *testing.T) {
	router := NewRouter(*newTestRouter(t))
	request := httptest.NewRequest(http.MethodGet, "/seismic-events?since=2026-09-19T23:59:00Z", nil)
	request.Header.Set("X-BMKG-Key", "bmkg-key")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
}

func TestTsunamiWarningsMalformedSinceReturns400(t *testing.T) {
	router := NewRouter(*newTestRouter(t))
	request := httptest.NewRequest(http.MethodGet, "/tsunami-warnings?since=nope", nil)
	request.Header.Set("X-BMKG-Key", "bmkg-key")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestHealthIsPublic(t *testing.T) {
	router := NewRouter(*newTestRouter(t))
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if !strings.Contains(response.Body.String(), `"service":"bmkg"`) {
		t.Fatalf("body = %s, want service bmkg", response.Body.String())
	}
}

func TestWaitDelayClientDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitDelay(ctx, time.Second) {
		t.Fatal("waitDelay must return false when the context is already canceled")
	}
	if !waitDelay(context.Background(), time.Millisecond) {
		t.Fatal("waitDelay must return true after the delay elapses")
	}
}
