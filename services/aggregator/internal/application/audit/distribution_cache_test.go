package audit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type slowDistributionRepository struct {
	fakeRepository
	calls   atomic.Int32
	release chan struct{}
	fail    atomic.Bool
}

func (r *slowDistributionRepository) HazardDistribution(ctx context.Context) (Distribution, error) {
	r.calls.Add(1)
	if r.release != nil {
		select {
		case <-r.release:
		case <-ctx.Done():
			return Distribution{}, ctx.Err()
		}
	}
	if r.fail.Load() {
		return Distribution{}, errors.New("show ranges: store busy")
	}
	return Distribution{Ranges: 28, Nodes: []NodeShare{{Node: "node-a", Replicas: 20}}}, nil
}

func TestDistributionCoalescesConcurrentCallers(t *testing.T) {
	repository := &slowDistributionRepository{release: make(chan struct{})}
	queries := NewQueries(repository)
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, _, err := queries.Distribution(context.Background())
			if err != nil || result.Ranges != 28 {
				t.Errorf("distribution = %+v, %v", result, err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(repository.release)
	wg.Wait()
	if got := repository.calls.Load(); got != 1 {
		t.Fatalf("placement queried %d times for 30 concurrent callers, want 1", got)
	}
}

func TestDistributionServesStaleAnswerWhileStoreIsSlow(t *testing.T) {
	clock := newTestClock(time.Date(2026, 11, 17, 21, 0, 0, 0, time.UTC))
	repository := &slowDistributionRepository{}
	queries := NewQueries(repository)
	queries.distribution.now = clock.Now
	if _, _, err := queries.Distribution(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Rebalancing: the placement query now takes longer than the request
	// deadline. Callers get the last answer at once.
	clock.Advance(distributionTTL + time.Second)
	repository.release = make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	result, _, err := queries.Distribution(ctx)
	if err != nil || result.Ranges != 28 {
		t.Fatalf("stale answer not served: %+v, %v", result, err)
	}
	if time.Since(started) > 150*time.Millisecond {
		t.Fatal("caller waited for the slow placement query")
	}
	close(repository.release)
	// Let that refresh (or one more) store its answer before the store fails.
	<-queries.distribution.refresh(context.Background(), repository.HazardDistribution)

	// Too old to reuse silently and the store fails: the last placement is
	// still the answer, marked degraded since the time it was read.
	answeredAt := clock.Now()
	clock.Advance(24 * time.Hour)
	repository.fail.Store(true)
	for i := 0; i < 3; i++ {
		result, stale, err := queries.Distribution(context.Background())
		if err != nil || result.Ranges != 28 {
			t.Fatalf("call %d: distribution = %+v, %v; want the last placement although the store failed", i, result, err)
		}
		if !stale.Degraded() || !stale.Since.Equal(answeredAt) {
			t.Fatalf("call %d: marker = %+v, want degraded since %s", i, stale, answeredAt)
		}
		clock.Advance(time.Minute)
	}

	// The store is back: the retry behind the next caller clears the marker.
	repository.fail.Store(false)
	for wait := time.Now().Add(2 * time.Second); time.Now().Before(wait); time.Sleep(5 * time.Millisecond) {
		clock.Advance(time.Minute)
		if _, stale, err := queries.Distribution(context.Background()); err == nil && !stale.Degraded() {
			return
		}
	}
	t.Fatal("answers stayed degraded after the store recovered")
}

func TestDistributionReturnsTheStoreErrorWhenNothingWasAnsweredBefore(t *testing.T) {
	repository := &slowDistributionRepository{}
	repository.fail.Store(true)
	if _, _, err := NewQueries(repository).Distribution(context.Background()); err == nil {
		t.Fatal("want the store error: there is no earlier placement to fall back on")
	}
}
