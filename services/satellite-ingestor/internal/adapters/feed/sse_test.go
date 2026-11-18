package feed

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/ports"
)

func collect(t *testing.T, body string) []ports.Event {
	t.Helper()
	var events []ports.Event
	err := readEvents(context.Background(), strings.NewReader(body), func(_ context.Context, event ports.Event) error {
		events = append(events, event)
		return nil
	}, func() {})
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func TestFramesKeepTheFeedsOwnTextInDetectionKeys(t *testing.T) {
	events := collect(t, strings.Join([]string{
		`id: 1`, `event: detection`,
		`data: {"satellite":"N20","acq_date":"2026-11-12","acq_time":"243","latitude":-2.51230,"longitude":113.80440,"confidence":"n","frp":12.4}`, ``,
		`id: 2`, `event: update`,
		`data: {"satellite":"N20","acq_date":"2026-11-12","acq_time":243,"latitude":"-2.51230","longitude":"113.80440","confidence":"high","frp":"20.5"}`, ``,
		`event: heartbeat`, `data: {"virtual_now":"2026-11-12T02:50:00Z","latest_offset":2}`, ``,
		`: keep-alive comment`, ``,
		`id: 3`, `event: retract`, `data: {"key":"N20|2026-11-12|243|-2.51230|113.80440"}`, ``,
		`id: 4`, `event: retract`, `data: {"satellite":"N21","acq_date":"2026-11-12","acq_time":"5","latitude":-2.50000,"longitude":113.80000}`, ``,
		`id: 5`, `event: detection`, `data: {"detection":{"satellite":"N","acq_date":"2026-11-12","acq_time":"1312","latitude":1.5,"longitude":101.25,"confidence":"l","frp":1}}`, ``, ``,
	}, "\n"))
	if len(events) != 6 {
		t.Fatalf("got %d events: %+v", len(events), events)
	}
	const key = "N20|2026-11-12|243|-2.51230|113.80440"
	if got := events[0].Detection.Key(); got != key || events[0].Detection.Confidence != "n" || events[0].Detection.FRP != 12.4 {
		t.Errorf("detection = %+v key %s", events[0].Detection, got)
	}
	if at, err := events[0].Detection.Time(); err != nil || at.Format(time.RFC3339) != "2026-11-12T02:43:00Z" {
		t.Errorf("detection time = %v, %v", at, err)
	}
	if got := events[1].Detection.Key(); got != key || events[1].Detection.Confidence != "h" || events[1].Detection.FRP != 20.5 {
		t.Errorf("update with number time and string numbers = %+v key %s", events[1].Detection, got)
	}
	if events[2].Kind != ports.KindHeartbeat || events[2].Offset != 0 {
		t.Errorf("heartbeat without id = %+v", events[2])
	}
	if events[3].Kind != ports.KindRetract || events[3].DetectionKey != key {
		t.Errorf("retract by key = %+v", events[3])
	}
	if events[4].DetectionKey != "N21|2026-11-12|5|-2.50000|113.80000" {
		t.Errorf("retract by fields = %+v", events[4])
	}
	if events[5].Detection.Satellite != "N" || !events[5].Detection.Ignored() {
		t.Errorf("wrapped detection = %+v", events[5])
	}
}

func TestBrokenFramesAdvanceTheOffsetInsteadOfWedgingTheStream(t *testing.T) {
	events := collect(t, strings.Join([]string{
		`id: 7`, `event: detection`, `data: {"satellite":"N20","acq_date":`, ``,
		`id: 8`, `event: detection`, `data: {"satellite":"N20","acq_date":"2026-11-12","acq_time":"243","latitude":"north","longitude":1}`, ``,
		`id: 9`, `event: reprocess`, `data: {}`, ``,
		`id: 10`, `event: retract`, `data: {}`, ``,
		`event: detection`, `data: {"satellite":"N20","acq_date":"2026-11-12","acq_time":"243","latitude":1,"longitude":1}`, ``,
		`id: 11`, `event: detection`, `data: ` + strings.Repeat("[", 5000), ``, ``,
	}, "\n"))
	want := []struct {
		offset int64
		reason string
	}{{7, "undecodable"}, {8, "undecodable"}, {9, "unknown_kind"}, {10, "undecodable"}, {0, "missing_offset"}, {11, "undecodable"}}
	if len(events) != len(want) {
		t.Fatalf("got %d events: %+v", len(events), events)
	}
	for index, expect := range want {
		if events[index].Offset != expect.offset || events[index].Skipped != expect.reason {
			t.Errorf("event %d = offset %d %q, want %d %q", index, events[index].Offset, events[index].Skipped, expect.offset, expect.reason)
		}
	}
}

func TestStreamSendsBothResumeConventionsAndReportsStatus(t *testing.T) {
	var from, lastID, auth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/session":
			fmt.Fprint(w, `{"group":"g7","profile":"practice","speed":"60x","virtual_now":"2026-11-12T02:43:05Z","latest_offset":42,"extra":true}`)
		case "/v1/stream":
			if r.Header.Get("Authorization") != "Bearer good" {
				w.WriteHeader(http.StatusUnauthorized)
				fmt.Fprint(w, `{"error":{"code":"unauthorized","message":"token tidak valid"}}`)
				return
			}
			from, lastID, auth = r.URL.Query().Get("from"), r.Header.Get("Last-Event-ID"), r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "id: 18\nevent: heartbeat\ndata: {}\n\n")
		}
	}))
	defer server.Close()

	client := New(Config{URL: server.URL, Token: "good"})
	var got []ports.Event
	if err := client.Stream(context.Background(), 17, func(_ context.Context, event ports.Event) error {
		got = append(got, event)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if from != "17" || lastID != "17" || auth != "Bearer good" || len(got) != 1 || got[0].ReceivedAt.IsZero() {
		t.Fatalf("from=%q last=%q events=%+v", from, lastID, got)
	}
	session, err := client.Session(context.Background())
	if err != nil || session.Speed != 60 || session.LatestOffset != 42 || session.Profile != "practice" || session.VirtualNow.IsZero() {
		t.Fatalf("session = %+v, %v", session, err)
	}
	err = New(Config{URL: server.URL, Token: "bad"}).Stream(context.Background(), 0, func(context.Context, ports.Event) error { return nil })
	if status, ok := err.(*StatusError); !ok || status.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized stream = %v", err)
	}
}

func TestSilentConnectionIsDroppedByTheIdleWatchdog(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	started := time.Now()
	err := New(Config{URL: server.URL, Token: "t", IdleTimeout: 150 * time.Millisecond}).
		Stream(context.Background(), 0, func(context.Context, ports.Event) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "idle") || time.Since(started) > 3*time.Second {
		t.Fatalf("idle stream returned %v after %s", err, time.Since(started))
	}
}
