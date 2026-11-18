package hazards

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/degraded"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

func TestListCacheCoalescesConcurrentIdenticalListings(t *testing.T) {
	cache := newListCache(time.Second)
	var calls atomic.Int32
	release := make(chan struct{})
	load := func(context.Context) ([]domain.HazardEvent, string, error) {
		calls.Add(1)
		<-release
		return []domain.HazardEvent{{HazardID: "h-1"}}, "", nil
	}
	filter := ListFilter{Source: domain.SourceBMKG, PageSize: 50}

	var wg sync.WaitGroup
	results := make(chan int, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			items, _, _, err := cache.get(context.Background(), filter, load)
			if err != nil {
				t.Error(err)
				return
			}
			results <- len(items)
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	close(results)
	for n := range results {
		if n != 1 {
			t.Fatalf("caller got %d items, want 1", n)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("store queried %d times for 50 identical concurrent listings, want 1", got)
	}
}

func TestListCacheExpiresAndKeysByFilter(t *testing.T) {
	now := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	cache := newListCache(time.Second)
	cache.now = func() time.Time { return now }
	var calls atomic.Int32
	load := func(context.Context) ([]domain.HazardEvent, string, error) {
		calls.Add(1)
		return nil, "", nil
	}
	bmkg := ListFilter{Source: domain.SourceBMKG, PageSize: 50}
	for i := 0; i < 3; i++ {
		_, _, _, _ = cache.get(context.Background(), bmkg, load)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d within the TTL, want 1", calls.Load())
	}
	_, _, _, _ = cache.get(context.Background(), ListFilter{Source: domain.SourcePVMBG, PageSize: 50}, load)
	_, _, _, _ = cache.get(context.Background(), ListFilter{Source: domain.SourceBMKG, PageSize: 50, PageToken: "next"}, load)
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want a separate query per source and per page token", calls.Load())
	}
	now = now.Add(time.Second)
	_, _, _, _ = cache.get(context.Background(), bmkg, load)
	// Past the TTL the stale page is served and one refresh runs in the
	// background.
	for wait := time.Now().Add(time.Second); calls.Load() < 4 && time.Now().Before(wait); {
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() != 4 {
		t.Fatalf("calls = %d after the TTL, want the listing queried again", calls.Load())
	}
}

func TestListCacheDoesNotCacheErrors(t *testing.T) {
	cache := newListCache(time.Minute)
	fail := true
	load := func(context.Context) ([]domain.HazardEvent, string, error) {
		if fail {
			return nil, "", errors.New("store down")
		}
		return []domain.HazardEvent{{HazardID: "h-1"}}, "", nil
	}
	filter := ListFilter{Source: domain.SourceBMKG, PageSize: 50}
	if _, _, _, err := cache.get(context.Background(), filter, load); err == nil {
		t.Fatal("want the store error")
	}
	fail = false
	items, _, _, err := cache.get(context.Background(), filter, load)
	if err != nil || len(items) != 1 {
		t.Fatalf("after recovery: %v %v, want the fresh listing", items, err)
	}
}

func TestListCacheSharedQueryOutlivesTheFirstCallersContext(t *testing.T) {
	cache := newListCache(time.Second)
	release := make(chan struct{})
	load := func(ctx context.Context) ([]domain.HazardEvent, string, error) {
		select {
		case <-release:
			return []domain.HazardEvent{{HazardID: "h-1"}}, "", nil
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	filter := ListFilter{Source: domain.SourceBMKG, PageSize: 50}

	first, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { _, _, _, err := cache.get(first, filter, load); firstDone <- err }()
	time.Sleep(20 * time.Millisecond)
	secondDone := make(chan error, 1)
	go func() { _, _, _, err := cache.get(context.Background(), filter, load); secondDone <- err }()
	time.Sleep(20 * time.Millisecond)

	cancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first caller err = %v, want its own cancellation", err)
	}
	close(release)
	if err := <-secondDone; err != nil {
		t.Fatalf("second caller failed because the first gave up: %v", err)
	}
}

func TestListCacheServesStaleWhileOneRefreshRuns(t *testing.T) {
	now := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	cache := newListCache(time.Second)
	cache.now = func() time.Time { return now }
	var calls atomic.Int32
	release := make(chan struct{})
	load := func(context.Context) ([]domain.HazardEvent, string, error) {
		if calls.Add(1) == 1 {
			return []domain.HazardEvent{{HazardID: "old"}}, "", nil
		}
		<-release
		return []domain.HazardEvent{{HazardID: "new"}}, "", nil
	}
	filter := ListFilter{Source: domain.SourceBMKG, PageSize: 50}
	if items, _, _, _ := cache.get(context.Background(), filter, load); items[0].HazardID != "old" {
		t.Fatalf("first answer = %v", items)
	}
	now = now.Add(2 * time.Second) // stale, within listMaxStale
	for i := 0; i < 20; i++ {
		started := time.Now()
		items, _, _, err := cache.get(context.Background(), filter, load)
		if err != nil || items[0].HazardID != "old" {
			t.Fatalf("stale answer = %v %v, want the old page at once", items, err)
		}
		if time.Since(started) > 100*time.Millisecond {
			t.Fatal("caller waited for the refresh")
		}
	}
	close(release)
	for wait := time.Now().Add(time.Second); time.Now().Before(wait); time.Sleep(5 * time.Millisecond) {
		if items, _, _, _ := cache.get(context.Background(), filter, load); items[0].HazardID == "new" {
			if got := calls.Load(); got != 2 {
				t.Fatalf("store queried %d times, want 1 initial + 1 refresh", got)
			}
			return
		}
	}
	t.Fatal("refreshed page never replaced the stale one")
}

func TestListCacheWaitsWhenAnswerIsTooOld(t *testing.T) {
	now := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	cache := newListCache(time.Second)
	cache.now = func() time.Time { return now }
	var calls atomic.Int32
	load := func(context.Context) ([]domain.HazardEvent, string, error) {
		n := calls.Add(1)
		return []domain.HazardEvent{{HazardID: map[int32]string{1: "old", 2: "new"}[n]}}, "", nil
	}
	filter := ListFilter{Source: domain.SourceBMKG, PageSize: 50}
	_, _, _, _ = cache.get(context.Background(), filter, load)
	now = now.Add(listMaxStale)
	items, _, _, err := cache.get(context.Background(), filter, load)
	if err != nil || items[0].HazardID != "new" {
		t.Fatalf("answer = %v %v, want a fresh page once the cached one is older than listMaxStale", items, err)
	}
}

// The 18 Nov outage: the store refuses every statement for minutes. A page
// that was answered before must still be answered, marked degraded, however
// long ago that was; and the marker must go once the store is back.
func TestListCacheServesTheLastPageDegradedWhileTheStoreFails(t *testing.T) {
	clock := newTestClock(time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC))
	answeredAt := clock.Now()
	cache := newListCache(time.Second)
	cache.now = clock.Now
	var fail atomic.Bool
	storeDown := errors.New("remote wall time is too far ahead")
	load := func(context.Context) ([]domain.HazardEvent, string, error) {
		if fail.Load() {
			return nil, "", storeDown
		}
		return []domain.HazardEvent{{HazardID: "h-1"}}, "next", nil
	}
	filter := ListFilter{Source: domain.SourceBMKG, PageSize: 50}
	if _, _, stale, err := cache.get(context.Background(), filter, load); err != nil || stale.Degraded() {
		t.Fatalf("first answer: %+v %v, want a live page", stale, err)
	}

	fail.Store(true)
	clock.Advance(24 * time.Hour) // far beyond listMaxStale
	for i := 0; i < 3; i++ {
		items, next, stale, err := cache.get(context.Background(), filter, load)
		if err != nil {
			t.Fatalf("call %d: the store error reached the caller although a page was cached: %v", i, err)
		}
		if len(items) != 1 || items[0].HazardID != "h-1" || next != "next" {
			t.Fatalf("call %d: page = %v %q, want the last good one", i, items, next)
		}
		if !stale.Degraded() || !errors.Is(stale.Cause, storeDown) || !stale.Since.Equal(answeredAt) {
			t.Fatalf("call %d: marker = %+v, want degraded since %s because of the store error", i, stale, answeredAt)
		}
		clock.Advance(time.Minute)
	}

	// The store is back: the retry behind the next caller replaces the page
	// and the marker goes.
	fail.Store(false)
	for wait := time.Now().Add(2 * time.Second); time.Now().Before(wait); time.Sleep(5 * time.Millisecond) {
		clock.Advance(time.Minute)
		if _, _, stale, err := cache.get(context.Background(), filter, load); err == nil && !stale.Degraded() {
			return
		}
	}
	t.Fatal("answers stayed degraded after the store recovered")
}

// A store that neither answers nor fails must not hold the request: with an
// old page in hand the wait ends after degraded.Wait.
func TestListCacheGivesTheLastPageWhenTheStoreHangs(t *testing.T) {
	now := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	cache := newListCache(time.Second)
	cache.now = func() time.Time { return now }
	hang := make(chan struct{})
	defer close(hang)
	var calls atomic.Int32
	load := func(ctx context.Context) ([]domain.HazardEvent, string, error) {
		if calls.Add(1) > 1 {
			<-hang
		}
		return []domain.HazardEvent{{HazardID: "h-1"}}, "", nil
	}
	filter := ListFilter{Source: domain.SourceBMKG, PageSize: 50}
	_, _, _, _ = cache.get(context.Background(), filter, load)
	now = now.Add(listMaxStale)
	started := time.Now()
	items, _, stale, err := cache.get(context.Background(), filter, load)
	if err != nil || len(items) != 1 {
		t.Fatalf("answer = %v %v, want the last page", items, err)
	}
	if !errors.Is(stale.Cause, degraded.ErrSlow) {
		t.Fatalf("marker = %+v, want degraded because the store was slow", stale)
	}
	if waited := time.Since(started); waited > degraded.Wait+500*time.Millisecond {
		t.Fatalf("caller waited %s for a hanging store, want about %s", waited, degraded.Wait)
	}
}

// Eviction makes room by dropping old pages. A page whose refresh is failing
// is old by definition and is the only answer its key has: it must survive.
func TestListCacheKeepsFailingPagesWhenItMakesRoom(t *testing.T) {
	now := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	cache := newListCache(time.Second)
	cache.now = func() time.Time { return now }
	old := now.Add(-time.Hour)
	for i := 0; i < listCacheMaxEntries-1; i++ {
		cache.entries[listKey(ListFilter{PageSize: 50, PageToken: string(rune('a' + i))})] = listEntry{at: old}
	}
	cache.entries["failing"] = listEntry{at: old}
	cache.markFailed("failing", errors.New("store down"))

	cache.store("new", listEntry{at: now})
	if _, kept := cache.entries["failing"]; !kept {
		t.Fatal("the last good page of a failing key was evicted for being old")
	}
	if _, stored := cache.entries["new"]; !stored || len(cache.entries) != 2 {
		t.Fatalf("entries = %d, want the failing page and the new one", len(cache.entries))
	}

	// Long after its last failure nobody asks for it any more: it goes.
	now = now.Add(listFailedKeep)
	for i := 0; i < listCacheMaxEntries; i++ {
		cache.entries[listKey(ListFilter{PageSize: 50, PageToken: string(rune('a' + i))})] = listEntry{at: old}
	}
	cache.store("newer", listEntry{at: now})
	if _, kept := cache.entries["failing"]; kept {
		t.Fatal("a page that stopped failing long ago still held its slot")
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
