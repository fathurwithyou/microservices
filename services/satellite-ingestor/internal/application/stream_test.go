package application

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/adapters/memory"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/ports"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeFeed replays a fixed event log. It answers "from" inclusively, like a
// server that resends the last acknowledged event, and can cut the
// connection after a number of events.
type fakeFeed struct {
	mu       sync.Mutex
	events   []ports.Event
	cuts     []int // events to deliver per connection before dropping it
	session  ports.Session
	connects int
}

func (f *fakeFeed) Stream(ctx context.Context, offset int64, handler ports.EventHandler) error {
	f.mu.Lock()
	f.connects++
	limit := len(f.events)
	if len(f.cuts) > 0 {
		limit, f.cuts = f.cuts[0], f.cuts[1:]
	}
	events := append([]ports.Event(nil), f.events...)
	f.mu.Unlock()
	sent := 0
	for _, event := range events {
		if event.Offset != 0 && event.Offset < offset {
			continue
		}
		if sent >= limit {
			return errors.New("connection reset by peer")
		}
		event.ReceivedAt = time.Now()
		if err := handler(ctx, event); err != nil {
			return err
		}
		sent++
	}
	<-ctx.Done()
	return nil
}

func (f *fakeFeed) Session(context.Context) (ports.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.session, nil
}

// storeSink plays the Aggregator: it upserts by source_ref_id and can fail.
type storeSink struct {
	mu     sync.Mutex
	events map[string]domain.Fire
	sends  int
	fail   func(send int) bool
}

func (s *storeSink) Submit(_ context.Context, fires []domain.Fire) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends++
	if s.fail != nil && s.fail(s.sends) {
		// A partial send: the first fire arrived, then the call failed.
		if len(fires) > 0 {
			s.events[fires[0].SourceRefID] = fires[0]
		}
		return errors.New("aggregator unavailable")
	}
	for _, fire := range fires {
		s.events[fire.SourceRefID] = fire
	}
	return nil
}

func (s *storeSink) content() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for id, fire := range s.events {
		out[id] = fmt.Sprintf("%s|%s|%s|%.2f|%d", fire.Status, strings.Join(fire.DetectionKeys, ","), fire.Severity, fire.TotalFRP, fire.DetectionCount)
	}
	return out
}

func feedEvents(ops []op) []ports.Event {
	events := make([]ports.Event, 0, len(ops)+len(ops)/10)
	for index, o := range ops {
		offset := int64(index + 1)
		switch {
		case o.retract:
			events = append(events, ports.Event{Offset: offset, Kind: ports.KindRetract, DetectionKey: o.detection.Key()})
		case index%17 == 0:
			events = append(events, ports.Event{Offset: offset, Skipped: "undecodable"})
		default:
			events = append(events, ports.Event{Offset: offset, Kind: ports.KindDetection, Detection: o.detection})
		}
		if index%10 == 0 {
			// Heartbeats carry no offset.
			events = append(events, ports.Event{Kind: ports.KindHeartbeat})
		}
	}
	return events
}

// runUntil runs the stream until the committed offset reaches target.
func runUntil(t *testing.T, feed *fakeFeed, state *memory.State, sink *storeSink, target int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stream := NewStream(StreamConfig{
		Feed: feed, State: state, Sink: sink, Logger: testLogger(),
		LateWindow: 8 * time.Hour, MinBackoff: time.Millisecond, MaxBackoff: time.Millisecond,
		SessionInterval: time.Hour,
		Sleep:           func(context.Context, time.Duration) error { return nil },
	})
	done := make(chan struct{})
	go func() { _ = stream.Run(ctx); close(done) }()
	for {
		snapshot, _ := state.Load(ctx)
		if snapshot.Offset >= target {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("stream stuck at offset %d of %d", snapshot.Offset, target)
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done
}

// Dropped connections, a failing Aggregator and failed commits (a crash
// between send and commit) must leave exactly what an undisturbed run leaves.
func TestInterruptedStreamEndsWithTheSameEventsAsAnUndisturbedOne(t *testing.T) {
	for seed := int64(1); seed <= 8; seed++ {
		events := feedEvents(randomOps(seed, 300))
		target := int64(300)
		session := ports.Session{Group: "g", Profile: "practice", LatestOffset: target}

		cleanSink := &storeSink{events: map[string]domain.Fire{}}
		cleanState := memory.New()
		runUntil(t, &fakeFeed{events: events, session: session}, cleanState, cleanSink, target)

		rng := rand.New(rand.NewSource(seed))
		var cuts []int
		for i := 0; i < 12; i++ {
			cuts = append(cuts, 5+rng.Intn(60))
		}
		sink := &storeSink{events: map[string]domain.Fire{}, fail: func(send int) bool { return send%4 == 2 }}
		state := memory.New()
		commits := 0
		state.FailCommit = func(ports.Batch) error {
			commits++
			if commits%5 == 3 {
				return errors.New("node lost before commit")
			}
			return nil
		}
		feed := &fakeFeed{events: events, cuts: cuts, session: session}
		runUntil(t, feed, state, sink, target)

		want, got := cleanSink.content(), sink.content()
		if len(want) == 0 {
			t.Fatalf("seed %d: scenario produced no fires", seed)
		}
		if len(got) != len(want) {
			t.Fatalf("seed %d: %d events, undisturbed run has %d", seed, len(got), len(want))
		}
		for id, content := range want {
			if got[id] != content {
				t.Fatalf("seed %d: event %s = %q, undisturbed %q", seed, id, got[id], content)
			}
		}
		if feed.connects < 3 {
			t.Fatalf("seed %d: only %d connections, interruptions did not happen", seed, feed.connects)
		}
	}
}

func TestOffsetStaysWhenTheAggregatorRefuses(t *testing.T) {
	events := []ports.Event{
		{Offset: 1, Kind: ports.KindDetection, Detection: spot("N20", 10, 0, "n", 5)},
		{Offset: 2, Kind: ports.KindDetection, Detection: spot("N21", 20, 0.3, "n", 5)},
	}
	state := memory.New()
	sink := &storeSink{events: map[string]domain.Fire{}, fail: func(send int) bool { return send <= 3 }}
	feed := &fakeFeed{events: events, session: ports.Session{Group: "g", Profile: "p", LatestOffset: 2}}
	runUntil(t, feed, state, sink, 2)
	if sink.sends < 4 {
		t.Fatalf("batch was not retried until accepted: %d sends", sink.sends)
	}
	if fires := state.Fires(); len(fires) != 1 || fires[0].DetectionCount != 2 {
		t.Fatalf("stored fires = %+v", fires)
	}
}

func TestLateDetectionsAreRecordedNotClustered(t *testing.T) {
	events := []ports.Event{
		{Offset: 1, Kind: ports.KindDetection, Detection: spot("N20", 20*60, 0, "n", 5)},
		{Offset: 2, Kind: ports.KindDetection, Detection: spot("N21", 20*60-30, 0.3, "n", 5)}, // 30 min late: joins
		{Offset: 3, Kind: ports.KindDetection, Detection: spot("N", 20*60-9*60, 0.5, "n", 5)}, // 9 h late: past the 8 h bound
	}
	state := memory.New()
	sink := &storeSink{events: map[string]domain.Fire{}}
	runUntil(t, &fakeFeed{events: events, session: ports.Session{Group: "g", Profile: "p", LatestOffset: 3}}, state, sink, 3)
	late := state.Late()
	if len(late) != 1 || late[0].Offset != 3 {
		t.Fatalf("late records = %+v", late)
	}
	snapshot, _ := state.Load(context.Background())
	if snapshot.Counters["late"] != 1 {
		t.Fatalf("late counter = %d", snapshot.Counters["late"])
	}
	if fires := state.Fires(); len(fires) != 1 || fires[0].DetectionCount != 2 {
		t.Fatalf("the late detection changed a fire: %+v", fires)
	}
}

func TestNewFeedSessionStartsOver(t *testing.T) {
	state := memory.New()
	sink := &storeSink{events: map[string]domain.Fire{}}
	practice := []ports.Event{
		{Offset: 1, Kind: ports.KindDetection, Detection: spot("N20", 10, 0, "n", 5)},
		{Offset: 2, Kind: ports.KindDetection, Detection: spot("N21", 20, 0.3, "n", 5)},
		{Offset: 3, Kind: ports.KindDetection, Detection: spot("N21", 30, 0.4, "n", 5)},
	}
	runUntil(t, &fakeFeed{events: practice, session: ports.Session{Group: "g", Profile: "practice", LatestOffset: 3}}, state, sink, 3)

	// The exam session restarts offsets at 1 with other data.
	exam := []ports.Event{
		{Offset: 1, Kind: ports.KindDetection, Detection: spot("N20", 100, 40, "n", 5)},
		{Offset: 2, Kind: ports.KindDetection, Detection: spot("N21", 110, 40.2, "n", 5)},
	}
	feed := &fakeFeed{events: exam, session: ports.Session{Group: "g", Profile: "exam", LatestOffset: 2}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream := NewStream(StreamConfig{Feed: feed, State: state, Sink: sink, Logger: testLogger(), LateWindow: 8 * time.Hour,
		MinBackoff: time.Millisecond, MaxBackoff: time.Millisecond, SessionInterval: time.Hour,
		Sleep: func(context.Context, time.Duration) error { return nil }})
	done := make(chan struct{})
	go func() { _ = stream.Run(ctx); close(done) }()
	for {
		snapshot, _ := state.Load(ctx)
		if snapshot.Session == "g/exam" && snapshot.Offset == 2 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("did not restart on the new session: %+v", snapshot)
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done
	snapshot, _ := state.Load(context.Background())
	if len(snapshot.Detections) != 2 || len(snapshot.Fires) != 1 {
		t.Fatalf("exam state carries practice data: %d detections, %d fires", len(snapshot.Detections), len(snapshot.Fires))
	}
}

type deniedLeadership struct{}

func (deniedLeadership) Allowed() bool                             { return true }
func (deniedLeadership) StillLeader(context.Context) (bool, error) { return false, nil }

func TestNothingIsWrittenWithoutAValidLease(t *testing.T) {
	state := memory.New()
	sink := &storeSink{events: map[string]domain.Fire{}}
	stream := NewStream(StreamConfig{State: state, Sink: sink, Logger: testLogger(), LateWindow: time.Hour, Leadership: deniedLeadership{}})
	r := &run{stream: stream, engine: NewEngine(time.Hour), counters: map[string]int64{}}
	err := r.process(context.Background(), []ports.Event{
		{Offset: 1, Kind: ports.KindDetection, Detection: spot("N20", 10, 0, "n", 5)},
		{Offset: 2, Kind: ports.KindDetection, Detection: spot("N21", 20, 0.3, "n", 5)},
	})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("process without lease = %v", err)
	}
	if snapshot, _ := state.Load(context.Background()); snapshot.Offset != 0 || sink.sends != 0 {
		t.Fatalf("wrote without a lease: offset %d, sends %d", snapshot.Offset, sink.sends)
	}
}
