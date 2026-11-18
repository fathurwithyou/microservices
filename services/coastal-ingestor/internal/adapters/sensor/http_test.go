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

// A sensor that restarts forgets positions but keeps reporting healthy: the
// old cursor gets an empty final page. The client must re-read from the start
// instead of waiting forever at a position that no longer exists.
func TestFetchRecoversWhenSensorRestartsItsStream(t *testing.T) {
	restarted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/stations" {
			_, _ = w.Write([]byte(stationsBody))
			return
		}
		cursor := r.URL.Query().Get("cursor")
		switch {
		case !restarted && cursor == "":
			_, _ = w.Write([]byte(`{"generated_at":"2026-03-03T10:00:00Z","items":[{"observation_id":"a-1","station_id":"stn-padang-01","observed_at":"2026-03-01T11:59:00Z","quality":"ok"}],"next_cursor":"o-1"}`))
		case !restarted && cursor == "o-1":
			_, _ = w.Write([]byte(`{"generated_at":"2026-03-03T10:00:01Z","items":[{"observation_id":"a-2","station_id":"stn-padang-01","observed_at":"2026-03-01T12:00:00Z","quality":"ok"}],"next_cursor":"o-2"}`))
		case !restarted:
			_, _ = w.Write([]byte(`{"generated_at":"2026-03-03T10:00:02Z","items":[],"next_cursor":null}`))
		case cursor != "":
			// Restarted: the old cursor means nothing, and the clock started over.
			_, _ = w.Write([]byte(`{"generated_at":"2026-03-01T00:00:10Z","items":[],"next_cursor":null}`))
		default:
			_, _ = w.Write([]byte(`{"generated_at":"2026-03-01T00:00:11Z","items":[{"observation_id":"b-1","station_id":"stn-padang-01","observed_at":"2026-03-01T12:05:00Z","quality":"ok","critical":true}],"next_cursor":"o-1"}`))
		}
	}))
	defer server.Close()

	client := New(server.URL, "secret", time.Second)
	if readings, err := client.Fetch(context.Background()); err != nil || len(readings) != 2 {
		t.Fatalf("before restart: %d readings, err %v", len(readings), err)
	}
	restarted = true
	readings, err := client.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(readings) != 1 || !readings[0].Critical {
		t.Fatalf("after restart readings = %+v, want the new critical observation", readings)
	}
}

func TestFetchCaughtUpCursorIsNotAReset(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/stations" {
			_, _ = w.Write([]byte(stationsBody))
			return
		}
		cursor := r.URL.Query().Get("cursor")
		requests = append(requests, cursor)
		if cursor == "" {
			_, _ = w.Write([]byte(`{"generated_at":"2026-03-03T10:00:00Z","items":[{"observation_id":"a-1","station_id":"stn-padang-01","observed_at":"2026-03-01T11:59:00Z","quality":"ok"}],"next_cursor":"o-1"}`))
			return
		}
		// Caught up: nothing new yet, the sensor clock keeps moving forward.
		_, _ = w.Write([]byte(`{"generated_at":"2026-03-03T10:00:05Z","items":[],"next_cursor":null}`))
	}))
	defer server.Close()

	client := New(server.URL, "secret", time.Second)
	if readings, err := client.Fetch(context.Background()); err != nil || len(readings) != 1 {
		t.Fatalf("first fetch: %d readings, err %v", len(readings), err)
	}
	for i := 0; i < 3; i++ {
		readings, err := client.Fetch(context.Background())
		if err != nil || len(readings) != 0 {
			t.Fatalf("caught-up fetch %d: %d readings, err %v", i, len(readings), err)
		}
	}
	if client.Cursor() != "o-1" {
		t.Fatalf("cursor = %q, want it kept at o-1", client.Cursor())
	}
	for _, cursor := range requests[1:] {
		if cursor == "" {
			t.Fatalf("stream re-read from the start while merely caught up: requests %v", requests)
		}
	}
}

func TestFetchTreatsLongEmptyCursorAsForgotten(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/stations" {
			_, _ = w.Write([]byte(stationsBody))
			return
		}
		if r.URL.Query().Get("cursor") == "" {
			_, _ = w.Write([]byte(`{"generated_at":"2026-03-01T00:01:00Z","items":[{"observation_id":"b-1","station_id":"stn-padang-01","observed_at":"2026-03-01T12:05:00Z","quality":"ok"}],"next_cursor":"o-1"}`))
			return
		}
		_, _ = w.Write([]byte(`{"generated_at":"2026-03-01T00:01:00Z","items":[],"next_cursor":null}`))
	}))
	defer server.Close()

	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	client := New(server.URL, "secret", time.Second)
	client.now = func() time.Time { return now }
	// Seeded after an ingestor restart with a cursor the sensor no longer has.
	client.SetCursor("o-999")
	if readings, err := client.Fetch(context.Background()); err != nil || len(readings) != 0 {
		t.Fatalf("first empty answer: %d readings, err %v", len(readings), err)
	}
	now = now.Add(forgetAfter)
	readings, err := client.Fetch(context.Background())
	if err != nil || len(readings) != 1 || readings[0].ID != "b-1" {
		t.Fatalf("after %s of empty answers: readings %+v, err %v; want the stream re-read", forgetAfter, readings, err)
	}
}
