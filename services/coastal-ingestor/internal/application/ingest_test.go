package application

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/adapters/memory"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
)

type fakeSource struct {
	readings []domain.Reading
	err      error
	fetches  int
	cursor   string
}

func (s *fakeSource) Fetch(context.Context) ([]domain.Reading, error) {
	s.fetches++
	return s.readings, s.err
}

func (s *fakeSource) Cursor() string     { return s.cursor }
func (s *fakeSource) SetCursor(c string) { s.cursor = c }

type fakeSink struct {
	batches [][]domain.Reading
	err     error
}

func (s *fakeSink) Submit(_ context.Context, readings []domain.Reading) error {
	s.batches = append(s.batches, append([]domain.Reading(nil), readings...))
	return s.err
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)}
}

func validReading(now time.Time, id string) domain.Reading {
	return domain.Reading{
		ID:         id,
		SensorID:   "sensor-1",
		AreaName:   "Sumatera Barat",
		Quality:    "ok",
		OccurredAt: now.Add(-time.Minute),
		ReceivedAt: now,
		Latitude:   -6.2,
		Longitude:  106.8,
		Severity:   domain.SeverityWaspada,
		Payload:    []byte(`{"id":"` + id + `"}`),
	}
}

func newTestIngestor(clock *fakeClock, source *fakeSource, state *memory.State, sink *fakeSink, breaker *CircuitBreaker) *Ingestor {
	return NewIngestor(
		IngestConfig{Interval: time.Second},
		source,
		state,
		sink,
		NewValidator(DefaultValidatorConfig(), clock.Now, nil),
		breaker,
		testLogger(),
		clock.Now,
	)
}

func openBreaker(clock *fakeClock) *CircuitBreaker {
	return NewCircuitBreaker(CircuitConfig{
		FailureThreshold: 1,
		FailureWindow:    time.Minute,
		OpenTimeout:      30 * time.Second,
	}, clock.Now)
}

func TestCycleQuarantinesInvalidReadings(t *testing.T) {
	clock := testClock()
	state := memory.New()
	sink := &fakeSink{}
	source := &fakeSource{readings: []domain.Reading{
		validReading(clock.now, "ok-1"),
		{ID: "", SensorID: "s", AreaName: "Aceh", Quality: "ok", OccurredAt: clock.now, Severity: domain.SeverityNormal},
		{ID: "bad-severity", SensorID: "s", AreaName: "Aceh", Quality: "ok", OccurredAt: clock.now, Severity: "UNKNOWN"},
	}}
	ingestor := newTestIngestor(clock, source, state, sink, NewCircuitBreaker(DefaultCircuitConfig(), clock.Now))

	result, err := ingestor.Cycle(context.Background())
	if err != nil {
		t.Fatalf("Cycle returned error: %v", err)
	}
	if result.Quarantined != 2 {
		t.Fatalf("Quarantined = %d, want 2", result.Quarantined)
	}
	if len(result.Readings) != 1 || result.Readings[0].ID != "ok-1" {
		t.Fatalf("Readings = %+v, want only ok-1", result.Readings)
	}
	if len(sink.batches) != 1 || len(sink.batches[0]) != 1 {
		t.Fatalf("sink batches = %+v, want one batch of one reading", sink.batches)
	}
	records := state.Quarantined()
	if len(records) != 2 {
		t.Fatalf("quarantine records = %d, want 2", len(records))
	}
	if records[0].Reason != domain.ReasonMissingID || records[0].MessageID != "" {
		t.Errorf("first record = %+v, want missing id", records[0])
	}
	if records[1].Reason != domain.ReasonInvalidSeverity {
		t.Errorf("second record = %+v, want invalid severity", records[1])
	}
	lastGood, _, err := state.LastGood(context.Background())
	if err != nil || len(lastGood) != 1 {
		t.Fatalf("LastGood = %+v, %v; want the accepted reading", lastGood, err)
	}
}

func TestOpenCircuitSkipsFetch(t *testing.T) {
	clock := testClock()
	state := memory.New()
	cached := validReading(clock.now, "cached")
	_ = state.SaveLastGood(context.Background(), []domain.Reading{cached}, clock.now)
	sink := &fakeSink{}
	source := &fakeSource{}
	breaker := openBreaker(clock)
	breaker.RecordFailure()
	if breaker.State() != CircuitOpen {
		t.Fatalf("breaker state = %q, want open", breaker.State())
	}

	ingestor := newTestIngestor(clock, source, state, sink, breaker)
	result, err := ingestor.Cycle(context.Background())
	if err != nil {
		t.Fatalf("Cycle returned error: %v", err)
	}
	if source.fetches != 0 {
		t.Fatalf("fetches = %d, want 0 while the circuit is open", source.fetches)
	}
	if len(sink.batches) != 0 {
		t.Fatalf("sink must be skipped while the circuit is open")
	}
	if !result.Stale {
		t.Fatal("result must be marked stale")
	}
}

func TestStaleCacheUsedWhileOpen(t *testing.T) {
	clock := testClock()
	state := memory.New()
	cached := validReading(clock.now, "cached")
	_ = state.SaveLastGood(context.Background(), []domain.Reading{cached}, clock.now)
	breaker := openBreaker(clock)
	breaker.RecordFailure()

	ingestor := newTestIngestor(clock, &fakeSource{}, state, &fakeSink{}, breaker)
	result, err := ingestor.Cycle(context.Background())
	if err != nil {
		t.Fatalf("Cycle returned error: %v", err)
	}
	if len(result.Readings) != 1 || result.Readings[0].ID != "cached" {
		t.Fatalf("Readings = %+v, want the cached reading", result.Readings)
	}
}

func TestSourceFailureServesStaleThenRecoveryClosesCircuit(t *testing.T) {
	clock := testClock()
	state := memory.New()
	cached := validReading(clock.now, "cached")
	_ = state.SaveLastGood(context.Background(), []domain.Reading{cached}, clock.now)

	sink := &fakeSink{}
	source := &fakeSource{err: errors.New("connection refused")}
	breaker := openBreaker(clock)
	ingestor := newTestIngestor(clock, source, state, sink, breaker)

	result, err := ingestor.Cycle(context.Background())
	if err != nil {
		t.Fatalf("Cycle returned error: %v", err)
	}
	if !result.Stale || len(result.Readings) != 1 || result.Readings[0].ID != "cached" {
		t.Fatalf("result = %+v, want stale cached reading", result)
	}
	if breaker.State() != CircuitOpen {
		t.Fatalf("breaker state = %q, want open", breaker.State())
	}
	if len(sink.batches) != 0 {
		t.Fatal("sink must be skipped when the source fails")
	}

	// Still open: the next cycle must not touch the source.
	source.readings = []domain.Reading{validReading(clock.now, "fresh")}
	source.err = nil
	if _, err := ingestor.Cycle(context.Background()); err != nil {
		t.Fatalf("Cycle returned error: %v", err)
	}
	if source.fetches != 1 {
		t.Fatalf("fetches = %d, want 1 while the circuit is open", source.fetches)
	}

	// After the open timeout the probe succeeds and the circuit closes.
	clock.Advance(30 * time.Second)
	result, err = ingestor.Cycle(context.Background())
	if err != nil {
		t.Fatalf("Cycle returned error: %v", err)
	}
	if result.Stale {
		t.Fatal("recovered cycle must not be stale")
	}
	if len(result.Readings) != 1 || result.Readings[0].ID != "fresh" {
		t.Fatalf("Readings = %+v, want fresh reading", result.Readings)
	}
	if breaker.State() != CircuitClosed {
		t.Fatalf("breaker state = %q, want closed", breaker.State())
	}
	if len(sink.batches) != 1 || sink.batches[0][0].ID != "fresh" {
		t.Fatalf("sink batches = %+v, want the fresh reading", sink.batches)
	}
	lastGood, _, err := state.LastGood(context.Background())
	if err != nil || len(lastGood) != 1 || lastGood[0].ID != "fresh" {
		t.Fatalf("LastGood = %+v, %v; want fresh reading", lastGood, err)
	}
}

func TestSinkFailureDoesNotAdvanceLastGood(t *testing.T) {
	clock := testClock()
	state := memory.New()
	cached := validReading(clock.now, "cached")
	_ = state.SaveLastGood(context.Background(), []domain.Reading{cached}, clock.now)

	sink := &fakeSink{err: errors.New("hazard pipeline down")}
	source := &fakeSource{readings: []domain.Reading{validReading(clock.now, "fresh")}}
	ingestor := newTestIngestor(clock, source, state, sink, NewCircuitBreaker(DefaultCircuitConfig(), clock.Now))

	if _, err := ingestor.Cycle(context.Background()); err == nil {
		t.Fatal("Cycle must report the sink failure")
	}
	lastGood, _, err := state.LastGood(context.Background())
	if err != nil || len(lastGood) != 1 || lastGood[0].ID != "cached" {
		t.Fatalf("LastGood = %+v, %v; cache must not advance on sink failure", lastGood, err)
	}
}

func TestCyclePersistsSensorCursor(t *testing.T) {
	clock := testClock()
	state := memory.New()
	source := &fakeSource{readings: []domain.Reading{validReading(clock.now, "ok-1")}, cursor: "page-9"}
	ingestor := newTestIngestor(clock, source, state, &fakeSink{}, NewCircuitBreaker(DefaultCircuitConfig(), clock.Now))

	if _, err := ingestor.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	cursor, err := state.Cursor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cursor != "page-9" {
		t.Fatalf("cursor = %q, want page-9", cursor)
	}
}

type fakeLeadership struct {
	allowed, still bool
}

func (l *fakeLeadership) Allowed() bool { return l.allowed }
func (l *fakeLeadership) StillLeader(context.Context) (bool, error) {
	return l.still, nil
}

func TestStandbyDoesNotPollSensor(t *testing.T) {
	clock := testClock()
	source := &fakeSource{readings: []domain.Reading{validReading(clock.now, "ok-1")}}
	sink := &fakeSink{}
	ingestor := newTestIngestor(clock, source, memory.New(), sink, NewCircuitBreaker(DefaultCircuitConfig(), clock.Now)).
		WithLeadership(&fakeLeadership{})

	if _, err := ingestor.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if source.fetches != 0 || len(sink.batches) != 0 {
		t.Fatalf("standby fetched %d times, submitted %d batches", source.fetches, len(sink.batches))
	}
}

func TestNewLeaderResumesFromCommittedCursor(t *testing.T) {
	clock := testClock()
	state := memory.New()
	if err := state.SaveCursor(context.Background(), "page-from-old-leader"); err != nil {
		t.Fatal(err)
	}
	source := &fakeSource{cursor: "stale-local-page"}
	leadership := &fakeLeadership{allowed: true, still: true}
	ingestor := newTestIngestor(clock, source, state, &fakeSink{}, NewCircuitBreaker(DefaultCircuitConfig(), clock.Now)).
		WithLeadership(leadership)

	if _, err := ingestor.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if source.cursor != "page-from-old-leader" {
		t.Fatalf("cursor = %q, want the committed one", source.cursor)
	}
}

func TestFencedLeaderDropsBatchAfterFreeze(t *testing.T) {
	clock := testClock()
	state := memory.New()
	source := &fakeSource{readings: []domain.Reading{validReading(clock.now, "ok-1")}, cursor: "page-2"}
	sink := &fakeSink{}
	// Local flag still says leader, but the database lease expired while frozen.
	ingestor := newTestIngestor(clock, source, state, sink, NewCircuitBreaker(DefaultCircuitConfig(), clock.Now)).
		WithLeadership(&fakeLeadership{allowed: true, still: false})

	if _, err := ingestor.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.batches) != 0 {
		t.Fatal("fenced leader submitted readings")
	}
	if cursor, _ := state.Cursor(context.Background()); cursor != "" {
		t.Fatalf("fenced leader committed cursor %q", cursor)
	}
}
