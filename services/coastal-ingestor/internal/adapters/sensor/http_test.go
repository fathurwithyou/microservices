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

const stationsBody = `{"items":[
 {"station_id":"stn-padang-01","name":"Padang","province":"Sumatera Barat","latitude":-0.9492,"longitude":100.3543}
]}`

func TestFetchWalksPagesAndMapsContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("Authorization = %q, want Bearer secret", got)
		}
		switch r.URL.Path {
		case "/v1/stations":
			_, _ = w.Write([]byte(stationsBody))
		case "/v1/observations":
			switch r.URL.Query().Get("cursor") {
			case "":
				_, _ = w.Write([]byte(`{"generated_at":"2026-03-01T12:00:00Z","items":[
				 {"observation_id":"obs-1","station_id":"stn-padang-01","observed_at":"2026-03-01T11:59:00Z","quality":"ok","wave_height_m":0.8,"sea_level_anomaly_m":0.1,"critical":false}
				],"next_cursor":"page-2"}`))
			case "page-2":
				_, _ = w.Write([]byte(`{"generated_at":"2026-03-01T12:00:00Z","items":[
				 {"observation_id":"obs-1","station_id":"stn-padang-01","observed_at":"2026-03-01T11:59:00Z","quality":"ok"},
				 {"observation_id":"obs-2","station_id":"stn-padang-01","observed_at":"2026-03-01T12:00:00Z","quality":"suspect","critical":true}
				],"next_cursor":null}`))
			default:
				t.Errorf("unexpected cursor %q", r.URL.Query().Get("cursor"))
			}
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	readings, err := New(server.URL, "secret", time.Second).Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}
	if len(readings) != 2 {
		t.Fatalf("len(readings) = %d, want 2", len(readings))
	}
	first := readings[0]
	if first.ID != "obs-1" || first.SensorID != "stn-padang-01" || first.AreaName != "Sumatera Barat" {
		t.Fatalf("unexpected first reading: %+v", first)
	}
	if first.Latitude != -0.9492 || first.Longitude != 100.3543 {
		t.Errorf("coordinates not taken from the station registry: %+v", first)
	}
	if first.Quality != "ok" || first.Severity != domain.SeverityNormal || first.Critical {
		t.Errorf("unexpected first classification: %+v", first)
	}
	second := readings[1]
	if second.Quality != "suspect" || !second.Critical || second.Severity != domain.SeverityAwas {
		t.Errorf("unexpected second classification: %+v", second)
	}
}

func TestFetchDeduplicatesRepeatedTailPage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/stations" {
			_, _ = w.Write([]byte(stationsBody))
			return
		}
		_, _ = w.Write([]byte(`{"items":[
		 {"observation_id":"obs-1","station_id":"stn-padang-01","observed_at":"2026-03-01T11:59:00Z","quality":"ok"}
		],"next_cursor":null}`))
	}))
	defer server.Close()

	client := New(server.URL, "secret", time.Second)
	if _, err := client.Fetch(context.Background()); err != nil {
		t.Fatalf("first Fetch: %v", err)
	}
	readings, err := client.Fetch(context.Background())
	if err != nil {
		t.Fatalf("second Fetch: %v", err)
	}
	if len(readings) != 0 {
		t.Fatalf("second Fetch returned %d readings, want 0", len(readings))
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
