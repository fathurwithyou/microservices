package analytics

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/degraded"
)

type countingRepository struct {
	fakeRepository
	calls   atomic.Int32
	release chan struct{}
	fail    atomic.Bool
}

func (r *countingRepository) Aggregate(ctx context.Context, f Filter) (Snapshot, error) {
	r.calls.Add(1)
	if r.release != nil {
		select {
		case <-r.release:
		case <-ctx.Done():
			return Snapshot{}, ctx.Err()
		}
	}
	if r.fail.Load() {
		return Snapshot{}, errors.New("store unavailable")
	}
	return r.fakeRepository.Aggregate(ctx, f)
}

func TestAnalyticsCacheCoalescesIdenticalDefaultWindows(t *testing.T) {
	repository := &countingRepository{fakeRepository: fakeRepository{rows: seismicRows(7)}, release: make(chan struct{})}
	queries := NewQueries(repository).WithCache(5 * time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// No window given: every caller means "last 7 days up to now",
			// which must be one cache key even though now moves.
			result, err := queries.Get(context.Background(), Filter{})
			if err != nil || len(result.HazardTypes) != 1 || result.HazardTypes[0].Count != 7 {
				t.Errorf("result = %+v, %v", result, err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(repository.release)
	wg.Wait()
	if got := repository.calls.Load(); got != 1 {
		t.Fatalf("store aggregated %d times for 40 identical concurrent requests, want 1", got)
	}
}

func TestAnalyticsCacheServesStaleCountsWhileStoreIsSlowButPartialStaysFresh(t *testing.T) {
	now := time.Date(2026, 11, 17, 19, 40, 0, 0, time.UTC)
	repository := &countingRepository{fakeRepository: fakeRepository{rows: seismicRows(7)}}
	queries := NewQueries(repository).WithCache(5 * time.Second)
	queries.cache.now = func() time.Time { return now }
	filter := Filter{Bucket: BucketDay}
	if _, err := queries.Get(context.Background(), filter); err != nil {
		t.Fatal(err)
	}

	// The store turns slow and a node goes away: counts come from the cache,
	// the completeness marker does not.
	now = now.Add(6 * time.Second)
	repository.release = make(chan struct{})
	repository.fakeRepository.unavailable = []string{"node-d"}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	result, err := queries.Get(ctx, filter)
	if err != nil {
		t.Fatalf("stale counts were not served while the refresh is slow: %v", err)
	}
	if time.Since(started) > 150*time.Millisecond {
		t.Fatal("caller waited for the slow refresh instead of getting the stale counts")
	}
	if len(result.HazardTypes) != 1 || result.HazardTypes[0].Count != 7 {
		t.Fatalf("counts = %+v, want the cached 7", result.HazardTypes)
	}
	if !result.Partial || len(result.UnavailableUnits) != 1 || result.UnavailableUnits[0] != "node-d" {
		t.Fatalf("partial marker = %v %v, want fresh partial with node-d", result.Partial, result.UnavailableUnits)
	}
	close(repository.release)
	// Let that refresh (or one more) store its counts before the store fails.
	key, resolved, _ := queries.resolve(filter)
	<-queries.cache.refresh(context.Background(), key, func(ctx context.Context) (aggregates, error) { return queries.aggregate(ctx, resolved) })

	// Older than resultMaxStale and the store fails: the old counts are
	// still the answer, now marked degraded, with the node known to be down.
	now = now.Add(resultMaxStale)
	repository.fail.Store(true)
	result, err = queries.Get(context.Background(), filter)
	if err != nil || len(result.HazardTypes) != 1 || result.HazardTypes[0].Count != 7 {
		t.Fatalf("result = %+v, %v; want the last counts although the store failed", result, err)
	}
	if !result.Degraded.Degraded() || !result.Partial || len(result.UnavailableUnits) != 1 || result.UnavailableUnits[0] != "node-d" {
		t.Fatalf("marker = %+v partial=%v units=%v, want degraded and partial with node-d", result.Degraded, result.Partial, result.UnavailableUnits)
	}
}

// The 18 Nov outage: the store refuses every statement and no node is known
// to be down. The dashboard keeps its last counts however old, says so
// (partial, unavailable_units ["store"], as_of unchanged), and the marker
// goes once the store answers again.
func TestAnalyticsServesTheLastCountsDegradedWhileTheStoreFails(t *testing.T) {
	clock := newTestClock(time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC))
	answeredAt := clock.Now()
	asOf := clock.Now().Add(-5 * time.Second)
	repository := &countingRepository{fakeRepository: fakeRepository{rows: seismicRows(7), asOf: asOf}}
	queries := NewQueries(repository).WithCache(5 * time.Second)
	queries.cache.now = clock.Now
	if result, err := queries.Get(context.Background(), Filter{}); err != nil || result.Partial || result.Degraded.Degraded() {
		t.Fatalf("first answer = %+v, %v; want live and complete", result, err)
	}

	repository.fail.Store(true)
	clock.Advance(6 * time.Hour)
	for i := 0; i < 3; i++ {
		result, err := queries.Get(context.Background(), Filter{})
		if err != nil {
			t.Fatalf("call %d: the store error reached the caller although counts were cached: %v", i, err)
		}
		if len(result.HazardTypes) != 1 || result.HazardTypes[0].Count != 7 || !result.AsOf.Equal(asOf) {
			t.Fatalf("call %d: result = %+v, want the last counts with their own as_of", i, result)
		}
		if !result.Partial || len(result.UnavailableUnits) != 1 || result.UnavailableUnits[0] != degraded.StoreUnit {
			t.Fatalf("call %d: partial=%v units=%v, want partial with [store]", i, result.Partial, result.UnavailableUnits)
		}
		if !result.Degraded.Since.Equal(answeredAt) {
			t.Fatalf("call %d: degraded since %s, want %s", i, result.Degraded.Since, answeredAt)
		}
		clock.Advance(time.Minute)
	}

	repository.fail.Store(false)
	for wait := time.Now().Add(2 * time.Second); time.Now().Before(wait); time.Sleep(5 * time.Millisecond) {
		clock.Advance(time.Minute)
		if result, err := queries.Get(context.Background(), Filter{}); err == nil && !result.Partial && !result.Degraded.Degraded() {
			return
		}
	}
	t.Fatal("answers stayed degraded after the store recovered")
}

func TestAnalyticsReturnsTheStoreErrorWhenNothingWasAnsweredBefore(t *testing.T) {
	repository := &countingRepository{}
	repository.fail.Store(true)
	queries := NewQueries(repository).WithCache(5 * time.Second)
	if _, err := queries.Get(context.Background(), Filter{}); err == nil {
		t.Fatal("want the store error: there is no earlier answer to fall back on")
	}
}

type unknownHealthRepository struct{ fakeRepository }

func (unknownHealthRepository) UnavailableUnits(context.Context) ([]string, error) {
	return nil, errors.New("gossip_nodes: store down")
}

// The completeness lookup is a store query too. When it fails the counts are
// still answerable; only "complete" can no longer be claimed.
func TestAnalyticsAnswersPartialWhenCompletenessIsUnknown(t *testing.T) {
	queries := NewQueries(unknownHealthRepository{fakeRepository{rows: seismicRows(7)}})
	result, err := queries.Get(context.Background(), Filter{})
	if err != nil {
		t.Fatalf("a failed completeness lookup failed the request: %v", err)
	}
	if len(result.HazardTypes) != 1 || !result.Partial || len(result.UnavailableUnits) != 1 || result.UnavailableUnits[0] != degraded.StoreUnit {
		t.Fatalf("result = %+v, want the counts marked partial with [store]", result)
	}
	if !result.Degraded.Degraded() || !result.Degraded.Since.IsZero() {
		t.Fatalf("marker = %+v, want degraded with live data", result.Degraded)
	}
}

func TestAnalyticsCacheKeysByRequestedWindow(t *testing.T) {
	repository := &countingRepository{}
	queries := NewQueries(repository).WithCache(5 * time.Second)
	since := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for _, filter := range []Filter{{Bucket: BucketDay}, {Bucket: BucketHour}, {Since: since, Until: since.Add(24 * time.Hour), Bucket: BucketDay}, {Bucket: BucketDay}} {
		if _, err := queries.Get(context.Background(), filter); err != nil {
			t.Fatal(err)
		}
	}
	if got := repository.calls.Load(); got != 3 {
		t.Fatalf("store aggregated %d times for 3 distinct windows (+1 repeat), want 3", got)
	}
}

func TestAnalyticsKeepWarmAnswersTheDefaultWindowFromTheCache(t *testing.T) {
	repository := &countingRepository{fakeRepository: fakeRepository{rows: seismicRows(7)}}
	queries := NewQueries(repository).WithCache(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go queries.KeepWarm(ctx, Filter{})
	deadline := time.Now().Add(2 * time.Second)
	for repository.calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("KeepWarm never aggregated the default window")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Let the warm refresh store its entry, then make the store unusable: the
	// default window must still be answered, from the warm cache.
	time.Sleep(20 * time.Millisecond)
	repository.fail.Store(true)
	result, err := queries.Get(context.Background(), Filter{})
	if err != nil || len(result.HazardTypes) != 1 || result.HazardTypes[0].Count != 7 {
		t.Fatalf("result = %+v, %v; want the warmed counts", result, err)
	}
	if repository.calls.Load() != 1 {
		t.Fatalf("store aggregated %d times, want only the warm refresh", repository.calls.Load())
	}
}

func TestAnalyticsCacheTreatsNoBucketAndDayAsOneWindow(t *testing.T) {
	repository := &countingRepository{fakeRepository: fakeRepository{rows: seismicRows(7)}}
	queries := NewQueries(repository).WithCache(time.Hour)
	// KeepWarm asks without a bucket; the Client API always sends bucket=day.
	if _, err := queries.Get(context.Background(), Filter{}); err != nil {
		t.Fatal(err)
	}
	if _, err := queries.Get(context.Background(), Filter{Bucket: BucketDay}); err != nil {
		t.Fatal(err)
	}
	if got := repository.calls.Load(); got != 1 {
		t.Fatalf("store aggregated %d times for the same default window asked with and without bucket=day, want 1", got)
	}
}

// testClock is a settable clock safe to advance while a background refresh
// reads it.
type testClock struct{ nanos atomic.Int64 }

func newTestClock(at time.Time) *testClock {
	clock := &testClock{}
	clock.nanos.Store(at.UnixNano())
	return clock
}

func (c *testClock) Now() time.Time          { return time.Unix(0, c.nanos.Load()).UTC() }
func (c *testClock) Advance(d time.Duration) { c.nanos.Add(int64(d)) }
