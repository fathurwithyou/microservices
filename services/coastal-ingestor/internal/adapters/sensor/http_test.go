package sensor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/ports"
)

func TestFetchMapsReadingsAndSendsToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("Authorization = %q, want Bearer secret", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{
			"id":"r1","sensor_id":"s1",
			"occurred_at":"2026-03-01T11:59:00Z",
			"received_at":"2026-03-01T12:00:00Z",
			"latitude":-6.2,"longitude":106.8,
			"severity":"SIAGA","critical":true,
			"payload":{"raw":true}
		}]`))
	}))
	defer server.Close()

	readings, err := New(server.URL, "secret", time.Second).Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}
	if len(readings) != 1 {
		t.Fatalf("len(readings) = %d, want 1", len(readings))
	}
	got := readings[0]
	if got.ID != "r1" || got.SensorID != "s1" || got.Severity != domain.SeveritySiaga || !got.Critical {
		t.Fatalf("unexpected reading: %+v", got)
	}
	if string(got.Payload) != `{"raw":true}` {
		t.Errorf("Payload = %s, want raw JSON", got.Payload)
	}
	if !got.OccurredAt.Equal(time.Date(2026, 3, 1, 11, 59, 0, 0, time.UTC)) {
		t.Errorf("OccurredAt = %s", got.OccurredAt)
	}
}

func TestFetchStatusAndPayloadErrors(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr error
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, body: `{}`, wantErr: ports.ErrSourceUnauthorized},
		{name: "forbidden", status: http.StatusForbidden, body: `{}`, wantErr: ports.ErrSourceUnauthorized},
		{name: "server error", status: http.StatusInternalServerError, body: `{}`, wantErr: ports.ErrSourceUnavailable},
		{name: "malformed body", status: http.StatusOK, body: `not-json`, wantErr: ports.ErrSourcePayload},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			_, err := New(server.URL, "secret", time.Second).Fetch(context.Background())
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestFetchEmptyURLIsFixtureMode(t *testing.T) {
	readings, err := New("", "", time.Second).Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}
	if len(readings) != 0 {
		t.Fatalf("len(readings) = %d, want 0", len(readings))
	}
}
