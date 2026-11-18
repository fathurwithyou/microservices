package feed

import (
	"strconv"
	"sync"
	"time"
)

// Clock maps real time to virtual milliseconds since the virtual start.
type Clock struct {
	mu       sync.Mutex
	now      func() time.Time
	baseReal time.Time
	baseVirt float64
	speed    float64
}

func NewClock(now func() time.Time, speed float64) *Clock {
	return &Clock{now: now, baseReal: now(), speed: speed}
}

func (c *Clock) VirtualMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int64(c.baseVirt + float64(c.now().Sub(c.baseReal))/float64(time.Millisecond)*c.speed)
}

func (c *Clock) Speed() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.speed
}

func (c *Clock) SetSpeed(speed float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.now()
	c.baseVirt += float64(n.Sub(c.baseReal)) / float64(time.Millisecond) * c.speed
	c.baseReal, c.speed = n, speed
}

// Session is one run of a scenario: a virtual clock and the released prefix
// of the event log. Offsets are positions in the arrival-ordered log, so they
// do not depend on speed.
type Session struct {
	Profile string
	Seed    int64
	sc      *Scenario
	clock   *Clock

	mu         sync.RWMutex
	released   int
	releasedAt []int64 // real unix nanoseconds
	notify     chan struct{}
	done       chan struct{}
}

func NewSession(sc *Scenario, profile string, clock *Clock) *Session {
	return &Session{
		Profile: profile, Seed: sc.Cfg.Seed, sc: sc, clock: clock,
		releasedAt: make([]int64, len(sc.Events)),
		notify:     make(chan struct{}), done: make(chan struct{}),
	}
}

// Advance releases every event whose arrival time has been reached.
func (s *Session) Advance() int {
	v := s.clock.VirtualMs()
	now := s.clock.now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.released
	for n < len(s.sc.Events) && s.sc.Events[n].ArrivalMs <= v {
		s.releasedAt[n] = now
		n++
	}
	if n != s.released {
		s.released = n
		close(s.notify)
		s.notify = make(chan struct{})
	}
	return n
}

func (s *Session) run(tick time.Duration) {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
			s.Advance()
		}
	}
}

func (s *Session) snapshot() (int, <-chan struct{}) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.released, s.notify
}

func (s *Session) Latest() int {
	n, _ := s.snapshot()
	return n
}

func (s *Session) VirtualNow() string {
	return s.sc.VirtualTime(s.clock.VirtualMs()).Format(time.RFC3339)
}

// frame renders the SSE frame of the event at 0-based index i.
func (s *Session) frame(buf []byte, i int, retractShape string) []byte {
	e := s.sc.Events[i]
	buf = append(buf, "id: "...)
	buf = strconv.AppendInt(buf, int64(i+1), 10)
	buf = append(buf, "\nevent: "...)
	buf = append(buf, e.Kind.String()...)
	buf = append(buf, "\ndata: "...)
	buf = append(buf, s.sc.Cfg.Data(e, retractShape)...)
	return append(buf, "\n\n"...)
}

// logLine renders one NDJSON line of the admin log; data stays raw JSON so
// the latitude/longitude tokens are not reformatted.
func (s *Session) logLine(buf []byte, i int, retractShape string) []byte {
	e := s.sc.Events[i]
	s.mu.RLock()
	at := s.releasedAt[i]
	s.mu.RUnlock()
	buf = append(buf, `{"offset":`...)
	buf = strconv.AppendInt(buf, int64(i+1), 10)
	buf = append(buf, `,"event":"`...)
	buf = append(buf, e.Kind.String()...)
	buf = append(buf, `","data":`...)
	buf = append(buf, s.sc.Cfg.Data(e, retractShape)...)
	buf = append(buf, `,"released_at":"`...)
	if at != 0 {
		buf = time.Unix(0, at).UTC().AppendFormat(buf, time.RFC3339Nano)
	}
	buf = append(buf, `","virtual_released_at":"`...)
	buf = s.sc.VirtualTime(e.ArrivalMs).AppendFormat(buf, time.RFC3339Nano)
	return append(buf, "\"}\n"...)
}

// Dump renders the complete log (released or not) as NDJSON.
func Dump(sc *Scenario, retractShape string, write func([]byte) error) error {
	s := NewSession(sc, "", NewClock(time.Now, 0))
	buf := make([]byte, 0, 512)
	for i := range sc.Events {
		buf = s.logLine(buf[:0], i, retractShape)
		if err := write(buf); err != nil {
			return err
		}
	}
	return nil
}
