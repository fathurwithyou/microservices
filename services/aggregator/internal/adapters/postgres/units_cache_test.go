package postgres

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnavailableUnitsServesStaleWhileOneRefreshRuns(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	repo := &Repository{}
	repo.health.load = func(context.Context) ([]string, error) {
		if calls.Add(1) == 1 {
			return []string{"node-d"}, nil
		}
		<-release
		return nil, nil
	}
	units, err := repo.UnavailableUnits(context.Background())
	if err != nil || len(units) != 1 {
		t.Fatalf("first answer = %v, %v; want [node-d]", units, err)
	}

	// Expire the answer; the refresh blocks until released.
	repo.health.mu.Lock()
	repo.health.at = time.Now().Add(-time.Hour)
	repo.health.mu.Unlock()
	for i := 0; i < 20; i++ {
		started := time.Now()
		units, err := repo.UnavailableUnits(context.Background())
		if err != nil || len(units) != 1 {
			t.Fatalf("stale answer = %v, %v; want the last known [node-d]", units, err)
		}
		if time.Since(started) > 100*time.Millisecond {
			t.Fatal("caller waited for the refresh instead of getting the stale answer")
		}
	}
	for wait := time.Now().Add(time.Second); calls.Load() < 2 && time.Now().Before(wait); {
		time.Sleep(5 * time.Millisecond)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("store lookups = %d, want 1 initial + exactly 1 refresh for 20 stale callers", got)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if units, _ := repo.UnavailableUnits(context.Background()); len(units) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("refreshed answer never replaced the stale one")
}

func TestUnavailableUnitsFirstCallersWaitForTheFirstAnswer(t *testing.T) {
	var calls atomic.Int32
	repo := &Repository{}
	repo.health.load = func(context.Context) ([]string, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return []string{"node-b"}, nil
	}
	results := make(chan int, 10)
	for i := 0; i < 10; i++ {
		go func() {
			units, _ := repo.UnavailableUnits(context.Background())
			results <- len(units)
		}()
	}
	for i := 0; i < 10; i++ {
		if n := <-results; n != 1 {
			t.Fatalf("caller got %d units, want 1", n)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("store lookups = %d for 10 concurrent first callers, want 1", got)
	}
}
