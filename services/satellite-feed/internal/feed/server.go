package feed

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Options struct {
	Token          string
	AdminToken     string // empty disables the admin API
	Group          string
	Profile        string
	Seed           int64
	ExamSeed       int64
	Speed          float64
	Start          time.Time
	Hours          int
	RateMilli      int
	Heartbeat      time.Duration
	MaxConnections int
	FromInclusive  bool
	RetractShape   string        // "key" or "fields"
	Tick           time.Duration // 0: no background releaser, call Advance yourself
	Now            func() time.Time
	Logger         *slog.Logger
}

type Server struct {
	opt Options
	log *slog.Logger

	mu               sync.Mutex
	sess             *Session
	baseSpeed        float64 // speed a temporary override reverts to
	speedGen         int
	speedTimer       *time.Timer
	dropCh           chan struct{}
	dropStop         chan struct{}
	conns            int
	connSeq          int
	unavailableUntil time.Time
}

func NewServer(opt Options) *Server {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.Logger == nil {
		opt.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if opt.Heartbeat <= 0 {
		opt.Heartbeat = 10 * time.Second
	}
	if opt.MaxConnections <= 0 {
		opt.MaxConnections = 2
	}
	if opt.RetractShape != "fields" {
		opt.RetractShape = "key"
	}
	s := &Server{opt: opt, log: opt.Logger, baseSpeed: opt.Speed, dropCh: make(chan struct{})}
	s.startSession(opt.Profile, opt.Seed)
	return s
}

// Session returns the current session.
func (s *Server) Session() *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sess
}

// startSession begins a brand-new session: offsets restart at 1, the virtual
// clock restarts, and every open stream is cut.
func (s *Server) startSession(profile string, seed int64) *Session {
	sc := Generate(Config{Seed: seed, Start: s.opt.Start, Hours: s.opt.Hours, RateMilli: s.opt.RateMilli})
	s.mu.Lock()
	if s.speedTimer != nil {
		s.speedTimer.Stop()
		s.speedGen++
	}
	old := s.sess
	sess := NewSession(sc, profile, NewClock(s.opt.Now, s.baseSpeed))
	s.sess = sess
	s.mu.Unlock()
	if old != nil {
		close(old.done)
	}
	if s.opt.Tick > 0 {
		go sess.run(s.opt.Tick)
	}
	s.log.Info("session started", "profile", profile, "seed", seed, "events", len(sc.Events),
		"speed", s.baseSpeed, "virtual_start", sc.Cfg.Start.Format(time.RFC3339))
	return sess
}

// SetSpeed changes the virtual clock speed. With a duration the speed
// reverts to the last permanent speed afterwards.
func (s *Server) SetSpeed(speed float64, d time.Duration) {
	s.mu.Lock()
	if s.speedTimer != nil {
		s.speedTimer.Stop()
		s.speedTimer = nil
	}
	s.speedGen++
	gen := s.speedGen
	if d <= 0 {
		s.baseSpeed = speed
	} else {
		s.speedTimer = time.AfterFunc(d, func() { s.revertSpeed(gen) })
	}
	sess := s.sess
	sess.clock.SetSpeed(speed)
	s.mu.Unlock()
	sess.Advance()
	s.log.Info("speed changed", "speed", speed, "duration_seconds", d.Seconds())
}

func (s *Server) revertSpeed(gen int) {
	s.mu.Lock()
	if gen != s.speedGen {
		s.mu.Unlock()
		return
	}
	s.speedTimer = nil
	sess, speed := s.sess, s.baseSpeed
	sess.clock.SetSpeed(speed)
	s.mu.Unlock()
	sess.Advance()
	s.log.Info("speed reverted", "speed", speed)
}

// Drop cuts every open stream.
func (s *Server) Drop(reason string) int {
	s.mu.Lock()
	n := s.conns
	close(s.dropCh)
	s.dropCh = make(chan struct{})
	s.mu.Unlock()
	s.log.Info("streams dropped", "reason", reason, "connections", n)
	return n
}

func (s *Server) scheduleDrops(every, total time.Duration) {
	stop := make(chan struct{})
	s.mu.Lock()
	if s.dropStop != nil {
		close(s.dropStop)
	}
	s.dropStop = stop
	s.mu.Unlock()
	if every <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		end := time.After(total)
		for {
			select {
			case <-stop:
				return
			case <-end:
				s.log.Info("periodic drop finished")
				return
			case <-t.C:
				s.Drop("periodic")
			}
		}
	}()
}

// ---- HTTP ------------------------------------------------------------------

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func same(a, b string) bool {
	return b != "" && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	switch {
	case p == "/health":
		writeJSON(w, map[string]string{"status": "ok"})
	case strings.HasPrefix(p, "/v1/"):
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || !same(token, s.opt.Token) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
			return
		}
		s.mu.Lock()
		wait := s.unavailableUntil.Sub(s.opt.Now())
		s.mu.Unlock()
		if wait > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
			writeError(w, http.StatusServiceUnavailable, "unavailable", "layanan sementara tidak tersedia")
			return
		}
		switch {
		case p == "/v1/session" && r.Method == http.MethodGet:
			writeJSON(w, s.sessionView(s.Session()))
		case p == "/v1/stream" && r.Method == http.MethodGet:
			s.stream(w, r)
		case p == "/v1/exam/start" && r.Method == http.MethodPost:
			s.Drop("exam start")
			writeJSON(w, s.sessionView(s.startSession("exam", s.opt.ExamSeed)))
		default:
			writeError(w, http.StatusNotFound, "not_found", "endpoint tidak ditemukan")
		}
	case strings.HasPrefix(p, "/admin/"):
		if !same(r.Header.Get("X-Admin-Token"), s.opt.AdminToken) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
			return
		}
		s.admin(w, r)
	default:
		writeError(w, http.StatusNotFound, "not_found", "endpoint tidak ditemukan")
	}
}

type sessionView struct {
	Group        string  `json:"group"`
	Profile      string  `json:"profile"`
	Speed        float64 `json:"speed"`
	VirtualNow   string  `json:"virtual_now"`
	LatestOffset int     `json:"latest_offset"`
}

func (s *Server) sessionView(sess *Session) sessionView {
	return sessionView{s.opt.Group, sess.Profile, sess.clock.Speed(), sess.VirtualNow(), sess.Latest()}
}

// startIndex resolves where a stream begins, as a 0-based log index.
// Last-Event-ID: N always means "after N" and wins over ?from=.
func (s *Server) startIndex(r *http.Request) (int, error) {
	if h := strings.TrimSpace(r.Header.Get("Last-Event-ID")); h != "" {
		n, err := strconv.Atoi(h)
		if err != nil || n < 0 {
			return 0, errors.New("Last-Event-ID harus bilangan bulat tidak negatif")
		}
		return n, nil
	}
	if !r.URL.Query().Has("from") {
		return 0, nil
	}
	n, err := strconv.Atoi(r.URL.Query().Get("from"))
	if err != nil || n < 0 {
		return 0, errors.New("from harus bilangan bulat tidak negatif")
	}
	if s.opt.FromInclusive && n > 0 {
		n--
	}
	return n, nil
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	next, err := s.startIndex(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	s.mu.Lock()
	if s.conns >= s.opt.MaxConnections {
		s.mu.Unlock()
		s.log.Warn("stream rejected", "reason", "too_many_connections", "remote", r.RemoteAddr)
		writeError(w, http.StatusTooManyRequests, "too_many_connections", "terlalu banyak koneksi untuk token ini")
		return
	}
	s.conns++
	s.connSeq++
	id, sess, drop := s.connSeq, s.sess, s.dropCh
	s.mu.Unlock()

	first, reason, opened := next+1, "client closed", time.Now()
	s.log.Info("stream opened", "conn", id, "remote", r.RemoteAddr, "from_offset", first)
	defer func() {
		s.mu.Lock()
		s.conns--
		s.mu.Unlock()
		s.log.Info("stream closed", "conn", id, "reason", reason, "next_offset", next+1,
			"sent", next+1-first, "seconds", time.Since(opened).Seconds())
	}()

	rc := http.NewResponseController(w)
	write := func(b []byte) bool {
		_ = rc.SetWriteDeadline(time.Now().Add(20 * time.Second))
		if _, err := w.Write(b); err != nil {
			reason = "write failed"
			return false
		}
		return rc.Flush() == nil
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	_ = rc.Flush()

	// A cut is abrupt (no terminating chunk), like a broken connection.
	cut := func(why string) {
		reason = why
		panic(http.ErrAbortHandler)
	}
	heartbeat := func() bool {
		return write([]byte(`event: heartbeat` + "\n" + `data: {"virtual_now":"` + sess.VirtualNow() +
			`","latest_offset":` + strconv.Itoa(sess.Latest()) + "}\n\n"))
	}
	hb := time.NewTicker(s.opt.Heartbeat)
	defer hb.Stop()
	buf := make([]byte, 0, 64<<10)
	for {
		select {
		case <-drop:
			cut("dropped")
		case <-sess.done:
			cut("session reset")
		case <-r.Context().Done():
			return
		default:
		}
		released, notify := sess.snapshot()
		if next < released {
			end := min(next+256, released)
			buf = buf[:0]
			for i := next; i < end; i++ {
				buf = sess.frame(buf, i, s.opt.RetractShape)
			}
			if !write(buf) {
				return
			}
			next = end
			select {
			case <-hb.C:
				if !heartbeat() {
					return
				}
			default:
			}
			continue
		}
		select {
		case <-notify:
		case <-hb.C:
			if !heartbeat() {
				return
			}
		case <-drop:
			cut("dropped")
		case <-sess.done:
			cut("session reset")
		case <-r.Context().Done():
			return
		}
	}
}

func decodeBody(r *http.Request, v any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil
	}
	return json.Unmarshal(body, v)
}

func (s *Server) state() map[string]any {
	s.mu.Lock()
	sess, conns, base := s.sess, s.conns, s.baseSpeed
	wait := s.unavailableUntil.Sub(s.opt.Now())
	s.mu.Unlock()
	return map[string]any{
		"group": s.opt.Group, "profile": sess.Profile, "seed": sess.Seed,
		"speed": sess.clock.Speed(), "base_speed": base,
		"virtual_now": sess.VirtualNow(), "latest_offset": sess.Latest(),
		"total_events": len(sess.sc.Events), "connections": conns,
		"unavailable_seconds": max(wait.Seconds(), 0),
	}
}

func (s *Server) admin(w http.ResponseWriter, r *http.Request) {
	bad := func(msg string) { writeError(w, http.StatusBadRequest, "bad_request", msg) }
	switch r.Method + " " + r.URL.Path {
	case "GET /admin/state":
	case "POST /admin/speed":
		var in struct {
			Speed           *float64 `json:"speed"`
			DurationSeconds float64  `json:"duration_seconds"`
		}
		if err := decodeBody(r, &in); err != nil || in.Speed == nil || *in.Speed < 0 || *in.Speed > 1e6 || in.DurationSeconds < 0 {
			bad(`butuh {"speed": 0..1e6, "duration_seconds": detik}`)
			return
		}
		s.SetSpeed(*in.Speed, time.Duration(in.DurationSeconds*float64(time.Second)))
	case "POST /admin/drop":
		var in struct {
			EverySeconds float64 `json:"every_seconds"`
			ForSeconds   float64 `json:"for_seconds"`
		}
		if err := decodeBody(r, &in); err != nil || in.EverySeconds < 0 || in.ForSeconds < 0 {
			bad(`butuh {} atau {"every_seconds": N, "for_seconds": M}`)
			return
		}
		s.Drop("admin")
		// A new call always cancels an earlier periodic schedule.
		s.scheduleDrops(time.Duration(in.EverySeconds*float64(time.Second)), time.Duration(in.ForSeconds*float64(time.Second)))
	case "POST /admin/unavailable":
		var in struct {
			Seconds float64 `json:"seconds"`
		}
		if err := decodeBody(r, &in); err != nil || in.Seconds < 0 {
			bad(`butuh {"seconds": N}`)
			return
		}
		s.mu.Lock()
		s.unavailableUntil = s.opt.Now().Add(time.Duration(in.Seconds * float64(time.Second)))
		s.mu.Unlock()
		s.log.Info("unavailable window", "seconds", in.Seconds)
		if in.Seconds > 0 {
			s.Drop("unavailable")
		}
	case "POST /admin/reset":
		in := struct {
			Profile string `json:"profile"`
			Seed    *int64 `json:"seed"`
		}{Profile: s.opt.Profile}
		if err := decodeBody(r, &in); err != nil {
			bad(`butuh {"profile": "...", "seed": N}`)
			return
		}
		seed := s.opt.Seed
		if in.Seed != nil {
			seed = *in.Seed
		}
		s.Drop("reset")
		s.startSession(in.Profile, seed)
	case "GET /admin/scenario":
		sess := s.Session()
		writeJSON(w, map[string]any{
			"seed": sess.Seed, "profile": sess.Profile,
			"virtual_start": sess.sc.Cfg.Start.Format(time.RFC3339),
			"stats":         sess.sc.Stats, "specials": sess.sc.Specials,
		})
		return
	case "GET /admin/log":
		s.adminLog(w, r)
		return
	default:
		writeError(w, http.StatusNotFound, "not_found", "endpoint tidak ditemukan")
		return
	}
	writeJSON(w, s.state())
}

// adminLog pages through released events without using a stream slot.
func (s *Server) adminLog(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from, limit := 1, 1000
	var err error
	if q.Has("from") {
		if from, err = strconv.Atoi(q.Get("from")); err != nil || from < 1 {
			writeError(w, http.StatusBadRequest, "bad_request", "from harus bilangan bulat positif")
			return
		}
	}
	if q.Has("limit") {
		if limit, err = strconv.Atoi(q.Get("limit")); err != nil || limit < 1 {
			writeError(w, http.StatusBadRequest, "bad_request", "limit harus bilangan bulat positif")
			return
		}
	}
	limit = min(limit, 50000)
	sess := s.Session()
	released := sess.Latest()
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Latest-Offset", strconv.Itoa(released))
	buf := make([]byte, 0, 64<<10)
	for i := from - 1; i < released && i < from-1+limit; i++ {
		buf = sess.logLine(buf, i, s.opt.RetractShape)
		if len(buf) > 48<<10 {
			if _, err := w.Write(buf); err != nil {
				return
			}
			buf = buf[:0]
		}
	}
	_, _ = w.Write(buf)
}
