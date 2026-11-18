package feed

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeTime struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeTime) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}
func (f *fakeTime) add(d time.Duration) {
	f.mu.Lock()
	f.t = f.t.Add(d)
	f.mu.Unlock()
}

type harness struct {
	t   *testing.T
	srv *Server
	ts  *httptest.Server
	ft  *fakeTime
}

func newHarness(t *testing.T, mod func(*Options)) *harness {
	t.Helper()
	ft := &fakeTime{t: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
	opt := Options{
		Token: "tok", AdminToken: "adm", Group: "g", Profile: "practice", Seed: 4031, ExamSeed: 99,
		Speed: 1, Start: defaultStart, Hours: 24, RateMilli: 50, Heartbeat: time.Hour,
		MaxConnections: 2, FromInclusive: true, Now: ft.now,
	}
	if mod != nil {
		mod(&opt)
	}
	srv := NewServer(opt)
	ts := httptest.NewServer(srv)
	t.Cleanup(func() {
		srv.Drop("test end")
		ts.Close()
	})
	return &harness{t, srv, ts, ft}
}

// advance moves real (fake) time and releases what became due.
func (h *harness) advance(d time.Duration) int {
	h.ft.add(d)
	return h.srv.Session().Advance()
}

func (h *harness) do(method, path string, headers map[string]string, body string) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(method, h.ts.URL+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	return resp
}

func (h *harness) feed(path string, extra ...string) *http.Response {
	headers := map[string]string{"Authorization": "Bearer tok"}
	for i := 0; i+1 < len(extra); i += 2 {
		headers[extra[i]] = extra[i+1]
	}
	return h.do("GET", path, headers, "")
}

func (h *harness) admin(method, path, body string) *http.Response {
	return h.do(method, path, map[string]string{"X-Admin-Token": "adm"}, body)
}

func bodyOf(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

type frame struct {
	id, event, data string
	hasID           bool
}

func readFrame(r *bufio.Reader) (frame, error) {
	var f frame
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return f, err
		}
		line = strings.TrimRight(line, "\n")
		switch {
		case line == "":
			return f, nil
		case strings.HasPrefix(line, "id: "):
			f.id, f.hasID = line[4:], true
		case strings.HasPrefix(line, "event: "):
			f.event = line[7:]
		case strings.HasPrefix(line, "data: "):
			f.data = line[6:]
		default:
			return f, io.ErrUnexpectedEOF
		}
	}
}

// take reads n event frames and checks their offsets are first, first+1, ...
func (h *harness) take(resp *http.Response, first, n int) {
	h.t.Helper()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/event-stream" {
		h.t.Fatalf("stream: status %d type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	r := bufio.NewReader(resp.Body)
	for i := 0; i < n; i++ {
		f, err := readFrame(r)
		if err != nil {
			h.t.Fatalf("frame %d: %v", i, err)
		}
		if f.id != strconv.Itoa(first+i) {
			h.t.Fatalf("got offset %q, want %d", f.id, first+i)
		}
		if f.event != "detection" && f.event != "update" && f.event != "retract" || !json.Valid([]byte(f.data)) {
			h.t.Fatalf("bad frame %+v", f)
		}
	}
	resp.Body.Close()
	h.waitConns(0)
}

func (h *harness) waitConns(want int) {
	h.t.Helper()
	for i := 0; i < 400; i++ {
		h.srv.mu.Lock()
		n := h.srv.conns
		h.srv.mu.Unlock()
		if n == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("connection count never reached %d", want)
}

const unauthorized = `{"error":{"code":"unauthorized","message":"token tidak valid"}}`

func TestAuthAndErrors(t *testing.T) {
	h := newHarness(t, nil)
	for _, headers := range []map[string]string{nil, {"Authorization": "Bearer nope"}, {"Authorization": "tok"}} {
		for _, path := range []string{"/v1/session", "/v1/stream?from=0"} {
			resp := h.do("GET", path, headers, "")
			if got := bodyOf(t, resp); resp.StatusCode != 401 || got != unauthorized {
				t.Fatalf("%s: %d %s", path, resp.StatusCode, got)
			}
		}
	}
	if resp := h.do("GET", "/admin/log", map[string]string{"X-Admin-Token": "tok"}, ""); resp.StatusCode != 401 {
		t.Fatalf("admin with feed token: %d", resp.StatusCode)
	}
	for _, path := range []string{"/v1/stream?from=abc", "/v1/stream?from=-1"} {
		resp := h.feed(path)
		if got := bodyOf(t, resp); resp.StatusCode != 400 || !strings.Contains(got, `"code":"bad_request"`) {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, got)
		}
	}
	if resp := h.feed("/v1/stream", "Last-Event-ID", "x"); resp.StatusCode != 400 {
		t.Fatalf("bad Last-Event-ID: %d", resp.StatusCode)
	}
	if resp := h.do("GET", "/health", nil, ""); resp.StatusCode != 200 {
		t.Fatalf("health: %d", resp.StatusCode)
	}
}

func TestSessionAndSpeedChange(t *testing.T) {
	h := newHarness(t, nil)
	var s sessionView
	get := func() sessionView {
		if err := json.Unmarshal([]byte(bodyOf(t, h.feed("/v1/session"))), &s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	if s = get(); s.Group != "g" || s.Profile != "practice" || s.Speed != 1 || s.VirtualNow != "2026-11-12T00:00:00Z" || s.LatestOffset != 0 {
		t.Fatalf("fresh session: %+v", s)
	}
	h.advance(10 * time.Second)
	if s = get(); s.VirtualNow != "2026-11-12T00:00:10Z" {
		t.Fatalf("after 10 s at 1x: %+v", s)
	}
	if resp := h.admin("POST", "/admin/speed", `{"speed":60,"duration_seconds":0.15}`); resp.StatusCode != 200 {
		t.Fatalf("speed: %d %s", resp.StatusCode, bodyOf(t, resp))
	}
	h.advance(10 * time.Second)
	if s = get(); s.Speed != 60 || s.VirtualNow != "2026-11-12T00:10:10Z" {
		t.Fatalf("after 10 s at 60x: %+v", s)
	}
	for i := 0; i < 400 && get().Speed != 1; i++ { // the override reverts on a real timer
		time.Sleep(5 * time.Millisecond)
	}
	h.advance(10 * time.Second)
	if s = get(); s.Speed != 1 || s.VirtualNow != "2026-11-12T00:10:20Z" {
		t.Fatalf("after revert: %+v", s)
	}
	h.admin("POST", "/admin/speed", `{"speed":3600}`).Body.Close() // permanent
	n := h.advance(time.Hour)
	if s = get(); s.Speed != 3600 || s.LatestOffset != n || n == 0 {
		t.Fatalf("permanent speed: %+v released=%d", s, n)
	}
	if resp := h.admin("POST", "/admin/speed", `{"speed":-1}`); resp.StatusCode != 400 {
		t.Fatalf("negative speed: %d", resp.StatusCode)
	}
}

// Offsets are positions in the arrival-ordered log, so two sessions with the
// same seed agree on every offset whatever their speed history was.
func TestOffsetsIndependentOfSpeed(t *testing.T) {
	a := newHarness(t, nil)
	b := newHarness(t, func(o *Options) { o.Speed = 600 })
	a.advance(30 * time.Hour)
	b.advance(time.Minute)
	b.srv.SetSpeed(7, 0)
	b.advance(time.Hour)
	n := b.srv.Session().Latest()
	if n < 100 || a.srv.Session().Latest() <= n {
		t.Fatalf("released a=%d b=%d", a.srv.Session().Latest(), n)
	}
	strip := regexp.MustCompile(`,"released_at":"[^"]*"`)
	path := "/admin/log?limit=" + strconv.Itoa(n)
	la := strip.ReplaceAllString(bodyOf(t, a.admin("GET", path, "")), "")
	lb := strip.ReplaceAllString(bodyOf(t, b.admin("GET", path, "")), "")
	if la != lb || strings.Count(la, "\n") != n-1 {
		t.Fatal("logs differ between sessions with different speeds")
	}
}

func TestReconnectIsGapless(t *testing.T) {
	h := newHarness(t, nil)
	if h.advance(40*time.Hour) < 500 {
		t.Fatal("scenario too small")
	}
	h.take(h.feed("/v1/stream"), 1, 50)
	h.take(h.feed("/v1/stream?from=0"), 1, 50)
	h.take(h.feed("/v1/stream?from=1"), 1, 5)
	h.take(h.feed("/v1/stream", "Last-Event-ID", "50"), 51, 50)
	h.take(h.feed("/v1/stream?from=101"), 101, 50) // inclusive: offset >= 101
	h.take(h.feed("/v1/stream?from=7", "Last-Event-ID", "150"), 151, 300)

	ex := newHarness(t, func(o *Options) { o.FromInclusive = false })
	ex.advance(40 * time.Hour)
	ex.take(ex.feed("/v1/stream?from=0"), 1, 5)
	ex.take(ex.feed("/v1/stream?from=100"), 101, 5) // exclusive: offset > 100
	ex.take(ex.feed("/v1/stream?from=100", "Last-Event-ID", "30"), 31, 5)
}

func TestLiveFollowAndHeartbeat(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.Heartbeat = 30 * time.Millisecond })
	resp := h.feed("/v1/stream?from=1")
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	f, err := readFrame(r)
	if err != nil || f.event != "heartbeat" || f.hasID || f.data != `{"virtual_now":"2026-11-12T00:00:00Z","latest_offset":0}` {
		t.Fatalf("heartbeat: %+v %v", f, err)
	}
	n := h.advance(2 * time.Hour) // releases a batch while the stream is open
	seen := 0
	for seen < n {
		if f, err = readFrame(r); err != nil {
			t.Fatal(err)
		}
		if f.event == "heartbeat" {
			if f.hasID {
				t.Fatal("heartbeat carries an id")
			}
			continue
		}
		seen++
		if f.id != strconv.Itoa(seen) {
			t.Fatalf("live offset %s, want %d", f.id, seen)
		}
	}
}

func TestTooManyConnections(t *testing.T) {
	h := newHarness(t, nil)
	a, b := h.feed("/v1/stream"), h.feed("/v1/stream")
	if a.StatusCode != 200 || b.StatusCode != 200 {
		t.Fatalf("first two streams: %d %d", a.StatusCode, b.StatusCode)
	}
	c := h.feed("/v1/stream")
	if got := bodyOf(t, c); c.StatusCode != 429 || !strings.Contains(got, `"code":"too_many_connections"`) {
		t.Fatalf("third stream: %d %s", c.StatusCode, got)
	}
	// The admin log is not a stream connection.
	if resp := h.admin("GET", "/admin/log", ""); resp.StatusCode != 200 {
		t.Fatalf("admin log: %d", resp.StatusCode)
	}
	a.Body.Close()
	h.waitConns(1)
	d := h.feed("/v1/stream")
	if d.StatusCode != 200 {
		t.Fatalf("stream after one closed: %d", d.StatusCode)
	}
	d.Body.Close()
	b.Body.Close()
}

func TestDropUnavailableAndReset(t *testing.T) {
	h := newHarness(t, nil)
	h.advance(3 * time.Hour)
	before := h.srv.Session().Latest()
	resp := h.feed("/v1/stream", "Last-Event-ID", strconv.Itoa(before))
	h.waitConns(1)
	h.admin("POST", "/admin/drop", "").Body.Close()
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("a dropped stream must end abruptly")
	}
	h.waitConns(0)

	h.admin("POST", "/admin/unavailable", `{"seconds":30}`).Body.Close()
	r := h.feed("/v1/session")
	if got := bodyOf(t, r); r.StatusCode != 503 || !strings.Contains(got, `"code":"unavailable"`) {
		t.Fatalf("unavailable: %d %s", r.StatusCode, got)
	}
	if r = h.feed("/v1/stream"); r.StatusCode != 503 {
		t.Fatalf("stream while unavailable: %d", r.StatusCode)
	}
	h.advance(31 * time.Second)
	if r = h.feed("/v1/session"); r.StatusCode != 200 {
		t.Fatalf("after the window: %d", r.StatusCode)
	}

	// exam/start imitates the assistants' endpoint: a brand-new session.
	stream := h.feed("/v1/stream", "Last-Event-ID", strconv.Itoa(h.srv.Session().Latest()))
	h.waitConns(1)
	r = h.do("POST", "/v1/exam/start", map[string]string{"Authorization": "Bearer tok"}, "")
	var s sessionView
	if err := json.Unmarshal([]byte(bodyOf(t, r)), &s); err != nil || s.Profile != "exam" || s.LatestOffset != 0 || s.VirtualNow != "2026-11-12T00:00:00Z" {
		t.Fatalf("exam start: %+v %v", s, err)
	}
	if _, err := io.ReadAll(stream.Body); err == nil {
		t.Fatal("a reset must cut open streams")
	}
	h.advance(3 * time.Hour)
	exam := bodyOf(t, h.admin("GET", "/admin/log?limit=1", ""))
	if !strings.HasPrefix(exam, `{"offset":1,"event":"`) {
		t.Fatalf("offsets must restart at 1: %s", exam)
	}
	r = h.admin("POST", "/admin/reset", `{"profile":"practice","seed":4031}`)
	if got := bodyOf(t, r); r.StatusCode != 200 || !strings.Contains(got, `"latest_offset":0`) || !strings.Contains(got, `"seed":4031`) {
		t.Fatalf("reset: %d %s", r.StatusCode, got)
	}
	h.advance(3 * time.Hour)
	again := bodyOf(t, h.admin("GET", "/admin/log?limit=1", ""))
	if again == exam || h.srv.Session().Latest() != before {
		t.Fatal("reset with the original seed must reproduce the original log")
	}
}

func TestAdminLogKeepsRawTokens(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.RetractShape = "fields" })
	n := h.advance(40 * time.Hour)
	resp := h.admin("GET", "/admin/log?from=11&limit=100000", "")
	if resp.Header.Get("X-Latest-Offset") != strconv.Itoa(n) {
		t.Fatalf("X-Latest-Offset %q, want %d", resp.Header.Get("X-Latest-Offset"), n)
	}
	lines := strings.Split(bodyOf(t, resp), "\n")
	if len(lines) != n-10 {
		t.Fatalf("%d lines, want %d", len(lines), n-10)
	}
	lineRe := regexp.MustCompile(`^\{"offset":(\d+),"event":"(detection|update|retract)","data":\{.*"latitude":-?\d+\.\d{5},"longitude":-?\d+\.\d{5}.*\},"released_at":"2030-01-0\dT[^"]+Z","virtual_released_at":"2026-11-1\dT[^"]+Z"\}$`)
	retracts := 0
	for i, line := range lines {
		m := lineRe.FindStringSubmatch(line)
		if m == nil || m[1] != strconv.Itoa(i+11) {
			t.Fatalf("line %d: %s", i, line)
		}
		if m[2] == "retract" {
			retracts++
			if strings.Contains(line, `"key"`) || strings.Contains(line, "confidence") {
				t.Fatalf("retract in fields shape: %s", line)
			}
		}
	}
	if retracts == 0 {
		t.Fatal("no retract in the log")
	}
	var sc struct {
		Specials []Special `json:"specials"`
		Stats    Stats     `json:"stats"`
	}
	if err := json.Unmarshal([]byte(bodyOf(t, h.admin("GET", "/admin/scenario", ""))), &sc); err != nil || len(sc.Specials) == 0 || sc.Stats.Events == 0 {
		t.Fatalf("scenario manifest: %v", err)
	}
}
