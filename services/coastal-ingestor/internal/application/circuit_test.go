package application

import (
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func newTestBreaker() (*CircuitBreaker, *fakeClock) {
	clock := &fakeClock{now: time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)}
	return NewCircuitBreaker(DefaultCircuitConfig(), clock.Now), clock
}

func fail(t *testing.T, b *CircuitBreaker, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		b.RecordFailure()
	}
}

func TestCircuitOpensAfterThresholdWithinWindow(t *testing.T) {
	b, clock := newTestBreaker()
	if b.State() != CircuitClosed {
		t.Fatalf("initial state = %q, want closed", b.State())
	}
	fail(t, b, 4)
	if b.State() != CircuitClosed {
		t.Fatalf("after 4 failures state = %q, want closed", b.State())
	}
	clock.Advance(29 * time.Second)
	b.RecordFailure()
	if b.State() != CircuitOpen {
		t.Fatalf("after 5 failures state = %q, want open", b.State())
	}
	if b.Allow() {
		t.Fatal("open breaker must reject")
	}
}

func TestCircuitFailureWindowResetsStreak(t *testing.T) {
	b, clock := newTestBreaker()
	fail(t, b, 4)
	clock.Advance(31 * time.Second)
	b.RecordFailure()
	if b.State() != CircuitClosed {
		t.Fatalf("failure outside window should reset streak, state = %q", b.State())
	}
	fail(t, b, 3)
	if b.State() != CircuitClosed {
		t.Fatalf("3 failures after reset must stay closed, state = %q", b.State())
	}
	b.RecordFailure()
	if b.State() != CircuitOpen {
		t.Fatalf("5th failure after reset should open, state = %q", b.State())
	}
}

func TestCircuitSuccessResetsStreak(t *testing.T) {
	b, _ := newTestBreaker()
	fail(t, b, 4)
	b.RecordSuccess()
	fail(t, b, 4)
	if b.State() != CircuitClosed {
		t.Fatalf("success must reset failures, state = %q", b.State())
	}
}

func TestCircuitOpenTimeoutBoundary(t *testing.T) {
	b, clock := newTestBreaker()
	fail(t, b, 5)
	clock.Advance(30*time.Second - time.Nanosecond)
	if b.Allow() {
		t.Fatal("open breaker allowed a request before the timeout")
	}
	clock.Advance(time.Nanosecond)
	if !b.Allow() {
		t.Fatal("open breaker must allow a probe at the timeout boundary")
	}
	if b.State() != CircuitHalfOpen {
		t.Fatalf("state = %q, want half_open", b.State())
	}
}

func TestCircuitHalfOpenAllowsSingleProbe(t *testing.T) {
	b, clock := newTestBreaker()
	fail(t, b, 5)
	clock.Advance(30 * time.Second)

	if !b.Allow() {
		t.Fatal("first probe must be allowed")
	}
	if b.Allow() {
		t.Fatal("second probe must be rejected while the first is in flight")
	}
}

func TestCircuitHalfOpenConcurrentProbes(t *testing.T) {
	b, clock := newTestBreaker()
	fail(t, b, 5)
	clock.Advance(30 * time.Second)

	const goroutines = 100
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		allowed int
	)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Allow() {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowed != 1 {
		t.Fatalf("half-open allowed %d probes, want exactly 1", allowed)
	}
}

func TestCircuitHalfOpenSuccessCloses(t *testing.T) {
	b, clock := newTestBreaker()
	fail(t, b, 5)
	clock.Advance(30 * time.Second)
	if !b.Allow() {
		t.Fatal("probe must be allowed")
	}

	b.RecordSuccess()
	if b.State() != CircuitClosed {
		t.Fatalf("state = %q, want closed", b.State())
	}
	if !b.Allow() {
		t.Fatal("closed breaker must allow requests")
	}
	// A fresh streak is required after recovery.
	fail(t, b, 4)
	if b.State() != CircuitClosed {
		t.Fatalf("state = %q, want closed after 4 failures", b.State())
	}
}

func TestCircuitHalfOpenFailureReopens(t *testing.T) {
	b, clock := newTestBreaker()
	fail(t, b, 5)
	clock.Advance(30 * time.Second)
	if !b.Allow() {
		t.Fatal("probe must be allowed")
	}

	b.RecordFailure()
	if b.State() != CircuitOpen {
		t.Fatalf("state = %q, want open after failed probe", b.State())
	}
	if b.Allow() {
		t.Fatal("reopened breaker must reject")
	}

	clock.Advance(29 * time.Second)
	if b.Allow() {
		t.Fatal("reopened breaker must stay closed until a full timeout")
	}
	clock.Advance(time.Second)
	if !b.Allow() {
		t.Fatal("reopened breaker must allow a probe after another timeout")
	}
}

func TestCircuitZeroConfigUsesDefaults(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)}
	b := NewCircuitBreaker(CircuitConfig{}, clock.Now)
	fail(t, b, 4)
	if b.State() != CircuitClosed {
		t.Fatalf("default threshold should be 5, state = %q", b.State())
	}
	b.RecordFailure()
	if b.State() != CircuitOpen {
		t.Fatalf("state = %q, want open", b.State())
	}
	clock.Advance(30 * time.Second)
	if !b.Allow() {
		t.Fatal("default open timeout should be 30s")
	}
}
