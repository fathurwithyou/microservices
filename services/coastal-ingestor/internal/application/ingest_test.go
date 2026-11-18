package application

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/adapters/memory"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
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

// fakeSink records every Submit call in arrival order. fail, when set,
// decides the outcome per reading.
type fakeSink struct {
	mu      sync.Mutex
	batches [][]domain.Reading
	err     error
	fail    func(domain.Reading) error
	// before runs ahead of each call, outside the lock.
	before func(domain.Reading)
}

func (s *fakeSink) Submit(_ context.Context, readings []domain.Reading) error {
	if s.before != nil {
		s.before(readings[0])
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, append([]domain.Reading(nil), readings...))
	if s.fail != nil {
		return s.fail(readings[0])
	}
	return s.err
}

// ids returns the submitted observation IDs in call order.
func (s *fakeSink) ids() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for _, batch := range s.batches {
		for _, reading := range batch {
			ids = append(ids, reading.ID)
		}
	}
	return ids
}

// orderedState records the order of sink and state calls.
type orderedState struct {
	*memory.State
	mu     sync.Mutex
	events *[]string
}

func (s *orderedState) SaveQuarantine(ctx context.Context, records []domain.QuarantineRecord) error {
	s.mu.Lock()
	*s.events = append(*s.events, "quarantine")
	s.mu.Unlock()
	return s.State.SaveQuarantine(ctx, records)
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
		// The sensor clock; deliberately unrelated to any wall clock.
		GeneratedAt: now,
		Latitude:    -6.2,
		Longitude:   106.8,
		Severity:    domain.SeverityWaspada,
		Payload:     []byte(`{"id":"` + id + `"}`),
	}
}

func newTestIngestor(clock *fakeClock, source *fakeSource, state *memory.State, sink *fakeSink, breaker *CircuitBreaker) *Ingestor {
	return NewIngestor(
		IngestConfig{Interval: time.Second},
		source,
		state,
		sink,
		NewValidator(DefaultValidatorConfig(), nil),
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

func criticalReading(now time.Time, id string) domain.Reading {
	reading := validReading(now, id)
	reading.Critical = true
	reading.Severity = domain.SeverityAwas
	return reading
}

// Critical readings are stored before any other work of the cycle: before the
// quarantine records are saved and before any ordinary reading is submitted,
// wherever they sat in the page.
func TestCycleSubmitsCriticalReadingsFirst(t *testing.T) {
	clock := testClock()
	var events []string
	var mu sync.Mutex
	state := &orderedState{State: memory.New(), events: &events}
	sink := &fakeSink{before: func(reading domain.Reading) {
		mu.Lock()
		defer mu.Unlock()
		state.mu.Lock()
		events = append(events, "submit:"+reading.ID)
		state.mu.Unlock()
	}}
	broken := validReading(clock.now, "")
	source := &fakeSource{readings: []domain.Reading{
		validReading(clock.now, "n-1"),
		broken,
		validReading(clock.now, "n-2"),
		criticalReading(clock.now, "crit-1"),
		validReading(clock.now, "n-3"),
		criticalReading(clock.now, "crit-2"),
	}}
	ingestor := NewIngestor(IngestConfig{Interval: time.Second, SubmitConcurrency: 1}, source, state, sink,
		NewValidator(DefaultValidatorConfig(), nil), NewCircuitBreaker(DefaultCircuitConfig(), clock.Now), testLogger(), clock.Now)

	if _, err := ingestor.Cycle(context.Background()); err != nil {
		t.Fatalf("Cycle returned error: %v", err)
	}
	want := "submit:crit-1,submit:crit-2,quarantine,submit:n-1,submit:n-2,submit:n-3"
	if got := strings.Join(events, ","); got != want {
		t.Fatalf("order = %s\nwant    %s", got, want)
	}
}

// A failed submit must not lose the reading: the cursor and last-good cache
// stay where they were, and the next cycle retries exactly what is missing.
func TestSubmitFailureKeepsCursorAndRetries(t *testing.T) {
	clock := testClock()
	state := memory.New()
	_ = state.SaveCursor(context.Background(), "page-1")
	down := true
	sink := &fakeSink{fail: func(reading domain.Reading) error {
		if down && reading.ID == "n-2" {
			return errors.New("aggregator unavailable")
		}
		return nil
	}}
	source := &fakeSource{cursor: "page-2", readings: []domain.Reading{
		validReading(clock.now, "n-1"), validReading(clock.now, "n-2"), criticalReading(clock.now, "crit-1"),
	}}
	ingestor := NewIngestor(IngestConfig{Interval: time.Second, SubmitConcurrency: 1}, source, state, sink,
		NewValidator(DefaultValidatorConfig(), nil), NewCircuitBreaker(DefaultCircuitConfig(), clock.Now), testLogger(), clock.Now)

	if _, err := ingestor.Cycle(context.Background()); err == nil {
		t.Fatal("Cycle must report the failed submit")
	}
	if cursor, _ := state.Cursor(context.Background()); cursor != "page-1" {
		t.Fatalf("cursor = %q after a failed submit, want it left at page-1", cursor)
	}
	if lastGood, _, _ := state.LastGood(context.Background()); len(lastGood) != 0 {
		t.Fatalf("last good advanced on a failed submit: %+v", lastGood)
	}
	if got := strings.Join(sink.ids(), ","); got != "crit-1,n-1,n-2" {
		t.Fatalf("first cycle submitted %s, want crit-1,n-1,n-2", got)
	}

	// The sensor has nothing new; the Aggregator is back.
	down = false
	source.readings, source.cursor = nil, "page-2"
	if _, err := ingestor.Cycle(context.Background()); err != nil {
		t.Fatalf("retry cycle: %v", err)
	}
	if got := strings.Join(sink.ids(), ","); got != "crit-1,n-1,n-2,n-2" {
		t.Fatalf("submitted %s, want only n-2 retried", got)
	}
	if cursor, _ := state.Cursor(context.Background()); cursor != "page-2" {
		t.Fatalf("cursor = %q after the retry succeeded, want page-2", cursor)
	}
	if lastGood, _, _ := state.LastGood(context.Background()); len(lastGood) != 3 {
		t.Fatalf("last good = %d readings, want the whole batch of 3", len(lastGood))
	}
}

// A critical reading that could not be stored is retried ahead of everything
// the next fetch brings.
func TestFailedCriticalIsRetriedBeforeNewWork(t *testing.T) {
	clock := testClock()
	state := memory.New()
	down := true
	sink := &fakeSink{fail: func(domain.Reading) error {
		if down {
			return errors.New("aggregator unavailable")
		}
		return nil
	}}
	source := &fakeSource{cursor: "page-2", readings: []domain.Reading{criticalReading(clock.now, "crit-1"), validReading(clock.now, "n-1")}}
	ingestor := NewIngestor(IngestConfig{Interval: time.Second, SubmitConcurrency: 1}, source, state, sink,
		NewValidator(DefaultValidatorConfig(), nil), NewCircuitBreaker(DefaultCircuitConfig(), clock.Now), testLogger(), clock.Now)
	if _, err := ingestor.Cycle(context.Background()); err == nil {
		t.Fatal("Cycle must report the failed critical submit")
	}
	if cursor, _ := state.Cursor(context.Background()); cursor != "" {
		t.Fatalf("cursor committed although a critical reading is not stored: %q", cursor)
	}

	down = false
	sink.batches = nil
	source.readings, source.cursor = []domain.Reading{validReading(clock.now, "n-2")}, "page-3"
	if _, err := ingestor.Cycle(context.Background()); err != nil {
		t.Fatalf("retry cycle: %v", err)
	}
	if got := strings.Join(sink.ids(), ","); got != "crit-1,n-1,n-2" {
		t.Fatalf("order = %s, want crit-1,n-1,n-2", got)
	}
	if cursor, _ := state.Cursor(context.Background()); cursor != "page-3" {
		t.Fatalf("cursor = %q, want page-3", cursor)
	}
}

// When the submit budget runs out the cycle goes back to the sensor, and a
// critical reading in the next fetch overtakes the ordinary backlog.
func TestCriticalOvertakesBacklogWhenBudgetIsSpent(t *testing.T) {
	clock := testClock()
	state := memory.New()
	sink := &fakeSink{}
	// Each stored reading costs 400 ms of a 1 s budget.
	sink.before = func(domain.Reading) { clock.Advance(400 * time.Millisecond) }
	source := &fakeSource{cursor: "page-2", readings: []domain.Reading{
		validReading(clock.now, "n-1"), validReading(clock.now, "n-2"), validReading(clock.now, "n-3"),
		validReading(clock.now, "n-4"), validReading(clock.now, "n-5"),
	}}
	ingestor := NewIngestor(IngestConfig{Interval: time.Second, SubmitConcurrency: 1, SubmitBudget: time.Second}, source, state, sink,
		NewValidator(DefaultValidatorConfig(), nil), NewCircuitBreaker(DefaultCircuitConfig(), clock.Now), testLogger(), clock.Now)

	if _, err := ingestor.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(sink.ids(), ","); got != "n-1,n-2,n-3" {
		t.Fatalf("first cycle stored %s, want the budget to stop it after n-3", got)
	}
	if cursor, _ := state.Cursor(context.Background()); cursor != "" {
		t.Fatalf("cursor committed with readings still queued: %q", cursor)
	}

	source.readings, source.cursor = []domain.Reading{criticalReading(clock.now, "crit-1")}, "page-3"
	if _, err := ingestor.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(sink.ids(), ","); got != "n-1,n-2,n-3,crit-1,n-4,n-5" {
		t.Fatalf("order = %s, want crit-1 ahead of the queued n-4,n-5", got)
	}
	if cursor, _ := state.Cursor(context.Background()); cursor != "page-3" {
		t.Fatalf("cursor = %q, want page-3 once everything is stored", cursor)
	}
}

func TestSubmitsRunConcurrently(t *testing.T) {
	clock := testClock()
	var mu sync.Mutex
	inFlight, peak := 0, 0
	var releaseOnce sync.Once
	release := make(chan struct{})
	sink := &fakeSink{before: func(domain.Reading) {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		reached := inFlight == 4
		mu.Unlock()
		if reached {
			// Calls keep arriving after the first four were let go, so the
			// count can pass through four more than once.
			releaseOnce.Do(func() { close(release) })
		}
		<-release
		mu.Lock()
		inFlight--
		mu.Unlock()
	}}
	var readings []domain.Reading
	for _, id := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		readings = append(readings, validReading(clock.now, id))
	}
	ingestor := NewIngestor(IngestConfig{Interval: time.Second, SubmitConcurrency: 4}, &fakeSource{readings: readings}, memory.New(), sink,
		NewValidator(DefaultValidatorConfig(), nil), NewCircuitBreaker(DefaultCircuitConfig(), clock.Now), testLogger(), clock.Now)
	if _, err := ingestor.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if peak != 4 {
		t.Fatalf("peak concurrent submits = %d, want 4", peak)
	}
	if len(sink.ids()) != 8 {
		t.Fatalf("stored %d readings, want 8", len(sink.ids()))
	}
}

// A source may return readings together with an error (pages read before a
// later page failed). They are processed, and the failure still counts.
func TestPartialFetchIsProcessedAndCountsAsFailure(t *testing.T) {
	clock := testClock()
	state := memory.New()
	sink := &fakeSink{}
	source := &fakeSource{cursor: "page-5", err: errors.New("page 6 cut short"),
		readings: []domain.Reading{criticalReading(clock.now, "crit-1")}}
	breaker := openBreaker(clock)
	ingestor := newTestIngestor(clock, source, state, sink, breaker)

	result, err := ingestor.Cycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(sink.ids(), ","); got != "crit-1" {
		t.Fatalf("stored %q, want the critical reading that preceded the failure", got)
	}
	if result.Stale || len(result.Readings) != 1 {
		t.Fatalf("result = %+v, want the salvaged reading", result)
	}
	if breaker.State() != CircuitOpen {
		t.Fatalf("breaker = %q, want the failed fetch counted", breaker.State())
	}
	if cursor, _ := state.Cursor(context.Background()); cursor != "page-5" {
		t.Fatalf("cursor = %q, want the position before the failed page", cursor)
	}
}

// metricValue reads a counter or gauge from the default registry; labels is
// "name=value" or empty.
func metricValue(t *testing.T, name, label string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			matched := label == ""
			for _, pair := range metric.GetLabel() {
				if pair.GetName()+"="+pair.GetValue() == label {
					matched = true
				}
			}
			if matched {
				return metric.GetCounter().GetValue() + metric.GetGauge().GetValue()
			}
		}
	}
	return 0
}

// The critical policy end to end: doubtful content is forwarded with a note,
// a broken identity is quarantined, counted, and never silent.
func TestCriticalPolicy(t *testing.T) {
	clock := testClock()
	state := memory.New()
	sink := &fakeSink{}

	suspect := criticalReading(clock.now, "crit-suspect")
	suspect.Quality = "suspect"
	implausible := criticalReading(clock.now, "crit-implausible")
	implausible.HasMeasurements, implausible.WaveHeightM = true, 900
	noStation := criticalReading(clock.now, "crit-nowhere")
	noStation.AreaName = ""
	fromFuture := criticalReading(clock.now, "crit-future")
	fromFuture.OccurredAt = fromFuture.GeneratedAt.Add(time.Hour)
	ordinarySuspect := validReading(clock.now, "n-suspect")
	ordinarySuspect.Quality = "suspect"

	beforeStation := metricValue(t, "coastal_critical_quarantined_total", "reason=unknown_station")
	beforeFuture := metricValue(t, "coastal_critical_quarantined_total", "reason=occurred_at_future")
	beforeForwarded := metricValue(t, "coastal_critical_readings_total", "")

	source := &fakeSource{readings: []domain.Reading{suspect, implausible, noStation, fromFuture, ordinarySuspect}}
	ingestor := newTestIngestor(clock, source, state, sink, NewCircuitBreaker(DefaultCircuitConfig(), clock.Now))
	result, err := ingestor.Cycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	stored := map[string]domain.Reading{}
	for _, batch := range sink.batches {
		stored[batch[0].ID] = batch[0]
	}
	if len(stored) != 2 {
		t.Fatalf("stored %v, want exactly the two critical readings with an intact identity", sink.ids())
	}
	if note := stored["crit-suspect"].Note; !strings.Contains(note, `quality="suspect"`) {
		t.Errorf("suspect critical note = %q, want the quality recorded", note)
	}
	if note := stored["crit-implausible"].Note; !strings.Contains(note, "wave_height_m") {
		t.Errorf("implausible critical note = %q, want the measurement doubt recorded", note)
	}
	if result.Quarantined != 3 {
		t.Fatalf("Quarantined = %d, want 3", result.Quarantined)
	}
	reasons := map[string]domain.QuarantineReason{}
	for _, record := range state.Quarantined() {
		reasons[record.MessageID] = record.Reason
	}
	if reasons["crit-nowhere"] != domain.ReasonUnknownStation || reasons["crit-future"] != domain.ReasonOccurredAtFuture || reasons["n-suspect"] != domain.ReasonQualityRejected {
		t.Fatalf("quarantine reasons = %v", reasons)
	}
	if got := metricValue(t, "coastal_critical_quarantined_total", "reason=unknown_station") - beforeStation; got != 1 {
		t.Errorf("coastal_critical_quarantined_total{unknown_station} moved by %v, want 1", got)
	}
	if got := metricValue(t, "coastal_critical_quarantined_total", "reason=occurred_at_future") - beforeFuture; got != 1 {
		t.Errorf("coastal_critical_quarantined_total{occurred_at_future} moved by %v, want 1", got)
	}
	if got := metricValue(t, "coastal_critical_readings_total", "") - beforeForwarded; got != 2 {
		t.Errorf("coastal_critical_readings_total moved by %v, want 2", got)
	}
}

type fakeFeedSource struct {
	fakeSource
	status domain.FeedStatus
}

func (s *fakeFeedSource) FeedStatus() domain.FeedStatus { return s.status }

// "Healthy but hours behind" is reported on the feed, never by quarantining
// its readings.
func TestStaleFeedIsReportedNotQuarantined(t *testing.T) {
	clock := testClock()
	state := memory.New()
	sensorNow := time.Date(2026, time.November, 12, 9, 0, 0, 0, time.UTC)
	old := validReading(sensorNow, "old-1")
	old.OccurredAt = sensorNow.Add(-3 * time.Hour)
	source := &fakeFeedSource{
		fakeSource: fakeSource{readings: []domain.Reading{old}},
		status:     domain.FeedStatus{GeneratedAt: sensorNow, NewestObservedAt: old.OccurredAt, AtTail: true},
	}
	ingestor := NewIngestor(IngestConfig{Interval: time.Second, StaleAfter: 30 * time.Minute}, source, state, &fakeSink{},
		NewValidator(DefaultValidatorConfig(), nil), NewCircuitBreaker(DefaultCircuitConfig(), clock.Now), testLogger(), clock.Now)

	result, err := ingestor.Cycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Quarantined != 0 || len(result.Readings) != 1 {
		t.Fatalf("result = %+v, want the old reading accepted", result)
	}
	if got := metricValue(t, "coastal_feed_freshness_seconds", ""); got != (3 * time.Hour).Seconds() {
		t.Fatalf("coastal_feed_freshness_seconds = %v, want %v", got, (3 * time.Hour).Seconds())
	}
	if got := metricValue(t, "coastal_sensor_clock_seconds", ""); got != float64(sensorNow.Unix()) {
		t.Fatalf("coastal_sensor_clock_seconds = %v, want %v", got, sensorNow.Unix())
	}
	if !ingestor.feedStale {
		t.Fatal("feed must be flagged stale")
	}
}
