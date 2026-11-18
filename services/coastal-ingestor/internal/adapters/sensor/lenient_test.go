package sensor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/adapters/memory"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/application"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/ports"
)

// item renders a contract-conforming observation.
func item(id, observedAt string, critical bool) string {
	return fmt.Sprintf(`{"observation_id":%q,"station_id":"stn-padang-01","observed_at":%q,"sea_level_anomaly_m":0.42,"wave_height_m":1.8,"quality":"ok","critical":%t}`, id, observedAt, critical)
}

// sensorStub serves /v1/stations and a scripted /v1/observations.
func sensorStub(t *testing.T, observations func(cursor string, w http.ResponseWriter)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/stations" {
			_, _ = w.Write([]byte(stationsBody))
			return
		}
		observations(r.URL.Query().Get("cursor"), w)
	}))
	t.Cleanup(server.Close)
	return server
}

func ids(readings []domain.Reading) string {
	var out []string
	for _, reading := range readings {
		out = append(out, reading.ID)
	}
	return strings.Join(out, ",")
}

type recordingSink struct {
	mu  sync.Mutex
	ids []string
}

func (s *recordingSink) Submit(_ context.Context, readings []domain.Reading) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, reading := range readings {
		s.ids = append(s.ids, reading.ID)
	}
	return nil
}

// One poison item must cost exactly one quarantine record: the good readings
// around it are stored, the critical reading behind it reaches the sink, and
// the cursor moves on. Before, the typed page decode failed on the first wrong
// type, the cursor never advanced, and everything behind the item was lost.
func TestPoisonItemDoesNotCostThePage(t *testing.T) {
	poisons := map[string]struct {
		raw    string
		reason domain.QuarantineReason
	}{
		"measurement is a string":    {`{"observation_id":"bad","station_id":"stn-padang-01","observed_at":"2026-11-12T00:30:00Z","sea_level_anomaly_m":0.1,"wave_height_m":"abc","quality":"ok","critical":false}`, domain.ReasonInvalidFieldType},
		"item is a string":           {`"obs-000119"`, domain.ReasonMalformedItem},
		"item is null":               {`null`, domain.ReasonMalformedItem},
		"item is an array":           {`[1,2,3]`, domain.ReasonMalformedItem},
		"item is not JSON at all":    {`{"observation_id":"bad","wave_height_m":NaN}`, domain.ReasonMalformedItem},
		"bare word":                  {`garbage`, domain.ReasonMalformedItem},
		"numeric observation_id":     {`{"observation_id":119,"station_id":"stn-padang-01","observed_at":"2026-11-12T00:30:00Z","sea_level_anomaly_m":0.1,"wave_height_m":1,"quality":"ok"}`, domain.ReasonInvalidFieldType},
		"no observed_at":             {`{"observation_id":"bad","station_id":"stn-padang-01","sea_level_anomaly_m":0.1,"wave_height_m":1,"quality":"ok"}`, domain.ReasonMissingObservedAt},
		"unparseable observed_at":    {`{"observation_id":"bad","station_id":"stn-padang-01","observed_at":"yesterday","sea_level_anomaly_m":0.1,"wave_height_m":1,"quality":"ok"}`, domain.ReasonInvalidObservedAt},
		"number float64 cannot hold": {`{"observation_id":"bad","station_id":"stn-padang-01","observed_at":"2026-11-12T00:30:00Z","sea_level_anomaly_m":1e999,"wave_height_m":1,"quality":"ok"}`, domain.ReasonInvalidMeasurement},
		"absurd wave height":         {`{"observation_id":"bad","station_id":"stn-padang-01","observed_at":"2026-11-12T00:30:00Z","sea_level_anomaly_m":0.1,"wave_height_m":12345,"quality":"ok"}`, domain.ReasonInvalidMeasurement},
		"no measurements":            {`{"observation_id":"bad","station_id":"stn-padang-01","observed_at":"2026-11-12T00:30:00Z","quality":"ok"}`, domain.ReasonMissingMeasurement},
		"empty object":               {`{}`, domain.ReasonMissingID},
		"unknown station":            {`{"observation_id":"bad","station_id":"stn-atlantis-99","observed_at":"2026-11-12T00:30:00Z","sea_level_anomaly_m":0.1,"wave_height_m":1,"quality":"ok"}`, domain.ReasonUnknownStation},
		"from the sensor's future":   {`{"observation_id":"bad","station_id":"stn-padang-01","observed_at":"2026-11-12T09:00:00Z","sea_level_anomaly_m":0.1,"wave_height_m":1,"quality":"ok"}`, domain.ReasonOccurredAtFuture},
		"oversized":                  {`{"observation_id":"bad","pad":"` + strings.Repeat("x", maxItemBytes) + `"}`, domain.ReasonOversizedItem},
	}
	for name, poison := range poisons {
		t.Run(name, func(t *testing.T) {
			server := sensorStub(t, func(cursor string, w http.ResponseWriter) {
				switch cursor {
				case "":
					fmt.Fprintf(w, `{"generated_at":"2026-11-12T00:31:00Z","items":[%s,%s,%s,%s],"next_cursor":"c-2"}`,
						item("obs-1", "2026-11-12T00:29:00Z", false), poison.raw,
						item("obs-3", "2026-11-12T00:30:00Z", true), item("obs-4", "2026-11-12T00:30:30Z", false))
				default:
					_, _ = w.Write([]byte(`{"generated_at":"2026-11-12T00:31:01Z","items":[],"next_cursor":null}`))
				}
			})
			client := New(server.URL, "secret", time.Second)
			state := memory.New()
			sink := &recordingSink{}
			ingestor := application.NewIngestor(application.IngestConfig{Interval: time.Second, SubmitConcurrency: 1}, client, state, sink,
				nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

			result, err := ingestor.Cycle(context.Background())
			if err != nil {
				t.Fatalf("Cycle: %v", err)
			}
			if got := strings.Join(sink.ids, ","); got != "obs-3,obs-1,obs-4" {
				t.Fatalf("stored %q, want the critical obs-3 first, then obs-1 and obs-4", got)
			}
			records := state.Quarantined()
			if result.Quarantined != 1 || len(records) != 1 {
				t.Fatalf("quarantined %d (records %d), want exactly the poison item", result.Quarantined, len(records))
			}
			if records[0].Reason != poison.reason {
				t.Errorf("reason = %q, want %q", records[0].Reason, poison.reason)
			}
			wantPayload := poison.raw
			if poison.reason == domain.ReasonOversizedItem {
				wantPayload = poison.raw[:oversizedPayloadBytes]
			}
			if string(records[0].Payload) != wantPayload {
				t.Errorf("quarantine payload is not the raw item: %.80q", records[0].Payload)
			}
			if cursor, _ := state.Cursor(context.Background()); cursor != "c-2" {
				t.Errorf("cursor = %q, want c-2: a poison item must not pin the stream", cursor)
			}
		})
	}
}

// A page cut in the middle: the complete items before the cut are processed,
// the fetch is a failure for the breaker, and the cursor stays in front of the
// page so nothing behind the cut is skipped.
func TestTruncatedPageIsSalvagedWithoutAdvancing(t *testing.T) {
	full := fmt.Sprintf(`{"generated_at":"2026-11-12T00:31:00Z","items":[%s,%s,%s],"next_cursor":"c-2"}`,
		item("obs-1", "2026-11-12T00:29:00Z", false), item("obs-2", "2026-11-12T00:30:00Z", true), item("obs-3", "2026-11-12T00:30:30Z", true))
	cut := full[:strings.Index(full, `"obs-3"`)+20]
	healed := false
	var cursors []string
	server := sensorStub(t, func(cursor string, w http.ResponseWriter) {
		cursors = append(cursors, cursor)
		switch {
		case cursor == "" && !healed:
			_, _ = w.Write([]byte(cut))
		case cursor == "":
			_, _ = w.Write([]byte(full))
		default:
			_, _ = w.Write([]byte(`{"generated_at":"2026-11-12T00:31:05Z","items":[],"next_cursor":null}`))
		}
	})
	client := New(server.URL, "secret", time.Second)

	readings, err := client.Fetch(context.Background())
	if !errors.Is(err, ports.ErrSourcePayload) {
		t.Fatalf("err = %v, want ErrSourcePayload so the breaker counts the damaged page", err)
	}
	if got := ids(readings); got != "obs-1,obs-2" {
		t.Fatalf("salvaged %q, want the two complete items obs-1,obs-2", got)
	}
	if !readings[1].Critical {
		t.Fatal("the salvaged critical reading lost its flag")
	}
	if client.Cursor() != "" {
		t.Fatalf("cursor = %q, want it left in front of the damaged page", client.Cursor())
	}

	// Still damaged: nothing is handed out twice.
	readings, err = client.Fetch(context.Background())
	if !errors.Is(err, ports.ErrSourcePayload) || len(readings) != 0 {
		t.Fatalf("second damaged fetch: %q, err %v; want no repeats and the error", ids(readings), err)
	}

	healed = true
	readings, err = client.Fetch(context.Background())
	if err != nil {
		t.Fatalf("healed fetch: %v", err)
	}
	if got := ids(readings); got != "obs-3" {
		t.Fatalf("healed fetch returned %q, want only obs-3 (the item behind the cut)", got)
	}
	if client.Cursor() != "c-2" {
		t.Fatalf("cursor = %q, want c-2 after the complete page", client.Cursor())
	}
}

// Readings of pages that were read completely are returned even when a later
// page fails, and the cursor stays behind the failed page.
func TestEarlierPagesSurviveALaterFailure(t *testing.T) {
	server := sensorStub(t, func(cursor string, w http.ResponseWriter) {
		switch cursor {
		case "":
			fmt.Fprintf(w, `{"generated_at":"2026-11-12T00:31:00Z","items":[%s,%s],"next_cursor":"c-2"}`,
				item("obs-1", "2026-11-12T00:29:00Z", false), item("obs-2", "2026-11-12T00:30:00Z", true))
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	client := New(server.URL, "secret", time.Second)
	readings, err := client.Fetch(context.Background())
	if !errors.Is(err, ports.ErrSourceUnavailable) {
		t.Fatalf("err = %v, want ErrSourceUnavailable", err)
	}
	if got := ids(readings); got != "obs-1,obs-2" {
		t.Fatalf("readings = %q, want page one despite page two failing", got)
	}
	if client.Cursor() != "c-2" {
		t.Fatalf("cursor = %q, want c-2 (the failed page is retried)", client.Cursor())
	}
}

func TestPageEnvelopeViolations(t *testing.T) {
	good := item("obs-1", "2026-11-12T00:29:00Z", true)
	tests := []struct {
		name        string
		body        string
		wantIDs     string
		wantErr     bool
		wantCursor  string
		wantReasons []domain.QuarantineReason
	}{
		{name: "items null", body: `{"generated_at":"2026-11-12T00:31:00Z","items":null,"next_cursor":"c-2"}`, wantCursor: "c-2"},
		{name: "items missing", body: `{"generated_at":"2026-11-12T00:31:00Z","next_cursor":"c-2"}`, wantCursor: "c-2"},
		{name: "items is an object", body: `{"generated_at":"2026-11-12T00:31:00Z","items":{"a":1},"next_cursor":"c-2"}`, wantIDs: "", wantCursor: "c-2", wantReasons: []domain.QuarantineReason{domain.ReasonMalformedItem}},
		{name: "items is a string", body: `{"items":"none","next_cursor":"c-2"}`, wantCursor: "c-2", wantReasons: []domain.QuarantineReason{domain.ReasonMalformedItem}},
		{name: "generated_at missing", body: `{"items":[` + good + `],"next_cursor":"c-2"}`, wantIDs: "obs-1", wantCursor: "c-2", wantReasons: []domain.QuarantineReason{""}},
		{name: "generated_at wrong type", body: `{"generated_at":12,"items":[` + good + `],"next_cursor":"c-2"}`, wantIDs: "obs-1", wantCursor: "c-2", wantReasons: []domain.QuarantineReason{""}},
		{name: "duplicated keys, last wins", body: `{"items":[],"items":[` + good + `],"next_cursor":"old","next_cursor":"c-2"}`, wantIDs: "obs-1", wantCursor: "c-2", wantReasons: []domain.QuarantineReason{""}},
		{name: "next_cursor wrong type", body: `{"items":[` + good + `],"next_cursor":7}`, wantIDs: "obs-1", wantErr: true, wantReasons: []domain.QuarantineReason{""}},
		{name: "unknown fields ignored", body: `{"v":2,"meta":{"a":[1,{"b":"]"}]},"items":[` + good + `],"next_cursor":"c-2","zzz":null}`, wantIDs: "obs-1", wantCursor: "c-2", wantReasons: []domain.QuarantineReason{""}},
		{name: "body is an array", body: `[` + good + `]`, wantErr: true},
		{name: "body is null", body: `null`, wantErr: true},
		{name: "empty body", body: ``, wantErr: true},
		{name: "cut before items", body: `{"generated_at":"2026-11-12T00:3`, wantErr: true},
		{name: "cut after items", body: `{"items":[` + good + `],"next_cur`, wantIDs: "obs-1", wantErr: true, wantReasons: []domain.QuarantineReason{""}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := sensorStub(t, func(cursor string, w http.ResponseWriter) {
				if cursor == "" {
					_, _ = w.Write([]byte(tc.body))
					return
				}
				_, _ = w.Write([]byte(`{"items":[],"next_cursor":null}`))
			})
			client := New(server.URL, "secret", time.Second)
			readings, err := client.Fetch(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil && !errors.Is(err, ports.ErrSourcePayload) {
				t.Fatalf("err = %v, want ErrSourcePayload", err)
			}
			if got := ids(readings); got != tc.wantIDs {
				t.Fatalf("ids = %q, want %q", got, tc.wantIDs)
			}
			if len(readings) != len(tc.wantReasons) {
				t.Fatalf("%d readings, want %d", len(readings), len(tc.wantReasons))
			}
			for i, want := range tc.wantReasons {
				if readings[i].IdentityViolation != want {
					t.Errorf("reading %d identity violation = %q, want %q", i, readings[i].IdentityViolation, want)
				}
			}
			if client.Cursor() != tc.wantCursor {
				t.Fatalf("cursor = %q, want %q", client.Cursor(), tc.wantCursor)
			}
		})
	}
}

func TestCriticalFlagIsReadFailSafe(t *testing.T) {
	tests := []struct {
		raw       string
		critical  bool
		violation domain.QuarantineReason
	}{
		{`true`, true, ""},
		{`false`, false, ""},
		{`null`, false, ""},
		{`"yes"`, true, domain.ReasonInvalidFieldType},
		{`"TRUE"`, true, domain.ReasonInvalidFieldType},
		{`"tsunami"`, true, domain.ReasonInvalidFieldType},
		{`1`, true, domain.ReasonInvalidFieldType},
		{`"no"`, false, domain.ReasonInvalidFieldType},
		{`"false"`, false, domain.ReasonInvalidFieldType},
		{`0`, false, domain.ReasonInvalidFieldType},
		{`{"level":3}`, false, domain.ReasonInvalidFieldType},
	}
	client := New("http://unused", "", time.Second)
	client.stations = map[string]station{"stn-padang-01": {Province: "Sumatera Barat"}}
	for _, tc := range tests {
		raw := `{"observation_id":"o","station_id":"stn-padang-01","observed_at":"2026-11-12T00:30:00Z","sea_level_anomaly_m":0.1,"wave_height_m":1,"quality":"ok","critical":` + tc.raw + `}`
		reading := client.decodeItem(rawItem{data: []byte(raw)}, page{})
		if reading.Critical != tc.critical || reading.ContentViolation != tc.violation || reading.IdentityViolation != "" {
			t.Errorf("critical=%s: got critical %v, content %q, identity %q; want %v, %q", tc.raw, reading.Critical, reading.ContentViolation, reading.IdentityViolation, tc.critical, tc.violation)
		}
		if tc.critical && reading.Severity != domain.SeverityAwas {
			t.Errorf("critical=%s: severity %q, want AWAS", tc.raw, reading.Severity)
		}
	}

	// An item that cannot be decoded but spells out a critical flag is marked,
	// so its quarantine is counted and logged as a critical one.
	for _, raw := range []string{`"{\"observation_id\":\"o\",\"critical\":true}"`, `{"observation_id":"o","critical":true,"wave_height_m":NaN}`} {
		reading := client.decodeItem(rawItem{data: []byte(raw)}, page{})
		if reading.IdentityViolation != domain.ReasonMalformedItem || !reading.Critical {
			t.Errorf("%s: identity %q critical %v; want malformed_item flagged critical", raw, reading.IdentityViolation, reading.Critical)
		}
	}
}

// nastyPayloads are responses a hostile or broken sensor could send. None may
// panic, hang, or produce a reading the validator would let through by
// accident.
var nastyPayloads = map[string]string{
	"deep nesting in item":     `{"items":[` + strings.Repeat("[", 200_000) + strings.Repeat("]", 200_000) + `],"next_cursor":null}`,
	"deep nesting unclosed":    `{"items":[` + strings.Repeat(`{"a":`, 100_000),
	"deep nesting in field":    `{"items":[{"observation_id":` + strings.Repeat("[", 50_000) + strings.Repeat("]", 50_000) + `}]}`,
	"invalid utf-8 in id":      "{\"items\":[{\"observation_id\":\"obs-\xff\xfe\",\"station_id\":\"stn-padang-01\",\"observed_at\":\"2026-11-12T00:30:00Z\",\"quality\":\"ok\",\"wave_height_m\":1,\"sea_level_anomaly_m\":0}]}",
	"invalid utf-8 everywhere": "{\"items\":[\xff\xfe\xfd,{\"\xff\":\"\xfe\"}],\"next_cursor\":\"\xff\"}",
	"huge numbers":             `{"items":[{"observation_id":"o","station_id":"stn-padang-01","observed_at":"2026-11-12T00:30:00Z","quality":"ok","wave_height_m":1e99999,"sea_level_anomaly_m":-` + strings.Repeat("9", 5000) + `}]}`,
	"huge integer id":          `{"items":[{"observation_id":` + strings.Repeat("9", 100_000) + `}]}`,
	"duplicated keys":          `{"items":[{"observation_id":"a","observation_id":"b","critical":false,"critical":true,"observed_at":1,"observed_at":"2026-11-12T00:30:00Z"}]}`,
	"items null":               `{"items":null}`,
	"items object":             `{"items":{}}`,
	"items number":             `{"items":3}`,
	"items of scalars":         `{"items":[null,true,false,0,-1.5e3,"",[],{}]}`,
	"generated_at missing":     `{"items":[{}]}`,
	"generated_at absurd":      `{"generated_at":"99999-99-99T99:99:99Z","items":[{"observed_at":"0000-00-00T00:00:00Z"}]}`,
	"observed_at year 0":       `{"generated_at":"2026-11-12T00:31:00Z","items":[{"observation_id":"o","station_id":"stn-padang-01","observed_at":"0000-01-01T00:00:00Z","quality":"ok","wave_height_m":1,"sea_level_anomaly_m":0}]}`,
	"only open brace":          `{`,
	"only open bracket":        `{"items":[`,
	"unterminated string":      `{"items":["abc`,
	"trailing backslash":       `{"items":["abc\`,
	"mismatched brackets":      `{"items":[{"a":[}]],"next_cursor":"x"}`,
	"stray closers":            `{"items":[}}}]]]`,
	"commas only":              `{"items":[,,,,]}`,
	"colon soup":               `{:::,,,"items"::[[::]]}`,
	"empty":                    ``,
	"whitespace":               " \n\t ",
	"nul bytes":                "{\"items\":[\x00\x00,\x00]}",
	"html error page":          `<html><body>502 Bad Gateway</body></html>`,
	"escaped quotes":           `{"items":["\"","\\\"","\\"],"next_cursor":"\\"}`,
	"next_cursor object":       `{"items":[],"next_cursor":{"o":1}}`,
	"very long key":            `{"` + strings.Repeat("k", 1<<20) + `":1,"items":[]}`,
	"many tiny items":          `{"items":[` + strings.Repeat("null,", maxItemsPerPage+50) + `null]}`,
	"concatenated documents":   `{"items":[]}{"items":[{"critical":true}]}`,
}

func TestNastyPayloadsNeverPanic(t *testing.T) {
	validator := application.NewValidator(application.DefaultValidatorConfig(), nil)
	for name, body := range nastyPayloads {
		t.Run(name, func(t *testing.T) {
			server := sensorStub(t, func(_ string, w http.ResponseWriter) { _, _ = w.Write([]byte(body)) })
			client := New(server.URL, "secret", 5*time.Second)
			// Three rounds: first sight, the re-fetch of the same page, and
			// whatever a kept cursor leads to.
			for round := 0; round < 3; round++ {
				readings, _ := client.Fetch(context.Background())
				if len(readings) > maxItemsPerPage*maxPagesPerFetch {
					t.Fatalf("round %d: %d readings from one fetch", round, len(readings))
				}
				for _, reading := range readings {
					if verdict := validator.Assess(reading); !verdict.Rejected {
						t.Fatalf("round %d: garbage accepted as a reading: %+v", round, reading)
					}
					if len(reading.Payload) > maxItemBytes {
						t.Fatalf("round %d: payload of %d bytes escaped the item cap", round, len(reading.Payload))
					}
				}
			}
			_ = client.FeedStatus()
		})
	}
}

func FuzzDecodePage(f *testing.F) {
	for _, body := range nastyPayloads {
		if len(body) < 4096 {
			f.Add([]byte(body))
		}
	}
	f.Add([]byte(`{"generated_at":"2026-11-12T02:43:05Z","items":[` + item("obs-000812", "2026-11-12T02:43:00Z", false) + `],"next_cursor":"eyJvIjo4MTN9"}`))
	client := New("http://unused", "", time.Second)
	client.stations = map[string]station{"stn-padang-01": {Province: "Sumatera Barat"}}
	f.Fuzz(func(t *testing.T, body []byte) {
		result, err := decodePage(body)
		if err == nil && result.damaged {
			t.Fatal("damaged page without an error")
		}
		if err != nil && result.hasNext {
			t.Fatal("a failed page offered a cursor to advance to")
		}
		for _, raw := range result.items {
			reading := client.decodeItem(raw, result)
			_ = dedupeKey(reading)
		}
	})
}

// Every reading of a page carries the one instant the page finished arriving,
// and a critical reading keeps its first receipt when the stream is merely
// read again, so the Aggregator sees an unchanged event. A sensor restart
// (its clock goes backwards) starts a new stream and a new receipt.
func TestReceivedAtIsPerPageAndStableForCriticalReadings(t *testing.T) {
	phase := "first"
	server := sensorStub(t, func(cursor string, w http.ResponseWriter) {
		switch {
		case phase == "first" && cursor == "":
			fmt.Fprintf(w, `{"generated_at":"2026-11-12T00:31:00Z","items":[%s,%s],"next_cursor":"c-1"}`,
				item("obs-1", "2026-11-12T00:29:00Z", false), item("obs-2", "2026-11-12T00:30:00Z", true))
		case phase == "first":
			_, _ = w.Write([]byte(`{"generated_at":"2026-11-12T00:31:00Z","items":[],"next_cursor":null}`))
		case phase == "restarted" && cursor != "":
			_, _ = w.Write([]byte(`{"generated_at":"2026-11-12T00:00:05Z","items":[],"next_cursor":null}`))
		case phase == "restarted":
			fmt.Fprintf(w, `{"generated_at":"2026-11-12T00:31:00Z","items":[%s],"next_cursor":null}`, item("obs-2", "2026-11-12T00:30:00Z", true))
		case cursor == "":
			// "reread": the same stream from the start, sensor clock moved on.
			fmt.Fprintf(w, `{"generated_at":"2026-11-12T02:00:00Z","items":[%s,%s],"next_cursor":"c-1"}`,
				item("obs-1", "2026-11-12T00:29:00Z", false), item("obs-2", "2026-11-12T00:30:00Z", true))
		default:
			_, _ = w.Write([]byte(`{"generated_at":"2026-11-12T02:00:00Z","items":[],"next_cursor":null}`))
		}
	})
	wall := time.Date(2026, time.November, 18, 10, 0, 0, 123456789, time.UTC)
	client := New(server.URL, "secret", time.Second)
	client.now = func() time.Time { return wall }

	first, err := client.Fetch(context.Background())
	if err != nil || len(first) != 2 {
		t.Fatalf("first fetch: %d readings, err %v", len(first), err)
	}
	if !first[0].ReceivedAt.Equal(wall) || !first[1].ReceivedAt.Equal(wall) {
		t.Fatalf("ReceivedAt = %s / %s, want the page receipt %s for both", first[0].ReceivedAt, first[1].ReceivedAt, wall)
	}
	wantGenerated := time.Date(2026, time.November, 12, 0, 31, 0, 0, time.UTC)
	if !first[1].GeneratedAt.Equal(wantGenerated) {
		t.Fatalf("GeneratedAt = %s, want the page's generated_at", first[1].GeneratedAt)
	}
	status := client.FeedStatus()
	if !status.AtTail || !status.GeneratedAt.Equal(wantGenerated) || !status.NewestObservedAt.Equal(time.Date(2026, time.November, 12, 0, 30, 0, 0, time.UTC)) {
		t.Fatalf("feed status = %+v", status)
	}

	// The cursor is treated as forgotten and the same stream is read again.
	phase = "reread"
	wall = wall.Add(10 * time.Minute)
	client.SetCursor("")
	again, err := client.Fetch(context.Background())
	if err != nil || len(again) != 2 {
		t.Fatalf("reread: %d readings, err %v", len(again), err)
	}
	if !again[1].ReceivedAt.Equal(first[1].ReceivedAt) || !again[1].GeneratedAt.Equal(first[1].GeneratedAt) {
		t.Fatalf("critical reading changed on a reread: received %s generated %s", again[1].ReceivedAt, again[1].GeneratedAt)
	}
	if !again[0].ReceivedAt.Equal(wall) {
		t.Fatalf("ordinary reading ReceivedAt = %s, want the new page receipt", again[0].ReceivedAt)
	}

	// The sensor restarts: its clock starts over and so does the stream.
	phase = "restarted"
	wall = wall.Add(10 * time.Minute)
	afterRestart, err := client.Fetch(context.Background())
	if err != nil || len(afterRestart) != 1 {
		t.Fatalf("after restart: %d readings, err %v", len(afterRestart), err)
	}
	if !afterRestart[0].ReceivedAt.Equal(wall) {
		t.Fatalf("after a sensor restart the critical reading kept the old receipt %s, want %s", afterRestart[0].ReceivedAt, wall)
	}
}

// A stream of unknown station IDs must not turn every page into an extra
// /v1/stations request against a sensor that is already misbehaving.
func TestUnknownStationsDoNotMultiplyRequests(t *testing.T) {
	stationRequests := 0
	next := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/stations" {
			stationRequests++
			_, _ = w.Write([]byte(stationsBody))
			return
		}
		next++
		fmt.Fprintf(w, `{"generated_at":"2026-11-12T00:31:00Z","items":[{"observation_id":"o-%d","station_id":"stn-ghost-%d","observed_at":"2026-11-12T00:30:00Z","quality":"ok","wave_height_m":1,"sea_level_anomaly_m":0}],"next_cursor":"c-%d"}`, next, next, next)
	}))
	defer server.Close()
	client := New(server.URL, "secret", time.Second)
	readings, err := client.Fetch(context.Background())
	if err != nil || len(readings) != maxPagesPerFetch {
		t.Fatalf("%d readings, err %v", len(readings), err)
	}
	if stationRequests > 2 {
		t.Fatalf("%d station requests for %d pages of unknown stations, want at most 2", stationRequests, maxPagesPerFetch)
	}
}

// One undecodable station must not cost the registry.
func TestStationRegistryIsDecodedLeniently(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/stations" {
			_, _ = w.Write([]byte(`{"items":[{"station_id":"stn-bad","latitude":"north"},null,"x",
			 {"station_id":"stn-padang-01","name":"Padang","province":"Sumatera Barat","latitude":-0.9492,"longitude":100.3543}]}`))
			return
		}
		fmt.Fprintf(w, `{"generated_at":"2026-11-12T00:31:00Z","items":[%s],"next_cursor":null}`, item("obs-1", "2026-11-12T00:30:00Z", true))
	}))
	defer server.Close()
	readings, err := New(server.URL, "secret", time.Second).Fetch(context.Background())
	if err != nil || len(readings) != 1 || readings[0].AreaName != "Sumatera Barat" {
		t.Fatalf("readings = %+v, err %v; want obs-1 located by the one good station", readings, err)
	}
}
