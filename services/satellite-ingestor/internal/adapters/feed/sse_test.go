package feed

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/ports"
)

const retractKey = "NOAA-20|2026-11-04|1230|-6.2|106.8"

func TestStreamParsesEvents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/stream" {
			t.Errorf("path = %q, want /v1/stream", r.URL.Path)
		}
		if got := r.URL.Query().Get("from"); got != "0" {
			t.Errorf("from = %q, want 0", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		if got := r.Header.Get("Last-Event-ID"); got != "" {
			t.Errorf("Last-Event-ID = %q, want empty at offset 0", got)
		}

		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, ": keepalive comment\n\n")
		_, _ = io.WriteString(w, "event: detection\nid: 1\n"+
			"data: {\"satellite\":\"NOAA-20\",\"acq_date\":\"2026-11-04\",\"acq_time\":\"1230\","+
			"\"latitude\":-6.20,\"longitude\":106.80,\"confidence\":\"h\",\"frp\":12.5}\n\n")
		_, _ = io.WriteString(w, "event: heartbeat\nid: 2\n\n")
		_, _ = io.WriteString(w, "event: update\nid: 3\n"+
			"data: {\"satellite\":\"NOAA-20\",\"acq_date\":\"2026-11-04\",\"acq_time\":\"1230\","+
			"\"latitude\":-6.2,\"longitude\":106.8,\"confidence\":\"n\",\"frp\":10}\n\n")
		_, _ = io.WriteString(w, fmt.Sprintf("event: retract\nid: 4\ndata: {\"key\":%q}\n\n", retractKey))
		_, _ = io.WriteString(w, "event: future-kind\nid: 5\ndata: ignored\n\n")
	}))
	defer server.Close()

	client := New(Config{URL: server.URL, Token: "secret", Timeout: 5 * time.Second})
	var events []ports.Event
	err := client.Stream(context.Background(), 0, func(_ context.Context, event ports.Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("Stream() error = %v, want nil", err)
	}

	if len(events) != 4 {
		t.Fatalf("got %d events, want 4: %+v", len(events), events)
	}
	if events[0].Kind != ports.KindDetection || events[0].Offset != 1 {
		t.Errorf("event 0 = %+v, want detection at offset 1", events[0])
	}
	wantDetection := domain.Detection{
		Satellite:    "NOAA-20",
		AcqDate:      "2026-11-04",
		AcqTime:      "1230",
		Latitude:     -6.2,
		Longitude:    106.8,
		RawLatitude:  "-6.20",
		RawLongitude: "106.80",
		Confidence:   domain.ConfidenceHigh,
		FRP:          12.5,
	}
	if events[0].Detection != wantDetection {
		t.Errorf("detection = %+v, want %+v", events[0].Detection, wantDetection)
	}
	if events[1].Kind != ports.KindHeartbeat || events[1].Offset != 2 {
		t.Errorf("event 1 = %+v, want heartbeat at offset 2", events[1])
	}
	if events[2].Kind != ports.KindUpdate || events[2].Detection.FRP != 10 {
		t.Errorf("event 2 = %+v, want update with FRP 10", events[2])
	}
	if events[3].Kind != ports.KindRetract || events[3].DetectionKey != retractKey {
		t.Errorf("event 3 = %+v, want retract of %q", events[3], retractKey)
	}
}

func TestStreamResumesWithLastEventID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("from"); got != "9" {
			t.Errorf("from = %q, want 9", got)
		}
		if got := r.Header.Get("Last-Event-ID"); got != "9" {
			t.Errorf("Last-Event-ID = %q, want 9", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: heartbeat\nid: 10\n\n")
	}))
	defer server.Close()

	client := New(Config{URL: server.URL, Token: "secret", Timeout: 5 * time.Second})
	var offsets []int64
	err := client.Stream(context.Background(), 9, func(_ context.Context, event ports.Event) error {
		offsets = append(offsets, event.Offset)
		return nil
	})
	if err != nil {
		t.Fatalf("Stream() error = %v, want nil", err)
	}
	if len(offsets) != 1 || offsets[0] != 10 {
		t.Fatalf("offsets = %v, want [10]", offsets)
	}
}

func TestSessionReportsLatestOffset(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/session" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("Authorization = %q", got)
		}
		_, _ = io.WriteString(w, `{"group":"sidequestproxy","profile":"practice","speed":60,"virtual_now":"2026-11-12T03:00:00Z","latest_offset":4096}`)
	}))
	defer server.Close()

	session, err := New(Config{URL: server.URL, Token: "secret", Timeout: 5 * time.Second}).Session(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if session.LatestOffset != 4096 || session.Speed != 60 || session.Profile != "practice" {
		t.Fatalf("session = %+v", session)
	}
}
