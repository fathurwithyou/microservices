package lease

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeStore keeps one lease row in memory and uses the injected clock exactly
// as a database-backed Store would use the database clock.
type fakeStore struct {
	clock *fakeClock

	mu    sync.Mutex
	token Token
	held  bool
}

func newFakeStore(clock *fakeClock) *fakeStore {
	return &fakeStore{clock: clock}
}

func (s *fakeStore) Acquire(_ context.Context, holder string, ttl time.Duration) (Token, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	if s.held && now.Before(s.token.Expiry) {
		return Token{}, false, nil
	}
	s.token = Token{Holder: holder, Fence: s.token.Fence + 1, Expiry: now.Add(ttl)}
	s.held = true
	return s.token, true, nil
}

func (s *fakeStore) Renew(_ context.Context, token Token, ttl time.Duration) (Token, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.sameToken(token) {
		return Token{}, false, nil
	}
	now := s.clock.Now()
	if !now.Before(s.token.Expiry) {
		return Token{}, false, nil
	}
	s.token.Expiry = now.Add(ttl)
	return s.token, true, nil
}

func (s *fakeStore) Release(_ context.Context, token Token) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sameToken(token) {
		s.held = false
	}
	return nil
}

func (s *fakeStore) Valid(_ context.Context, token Token) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sameToken(token) && s.clock.Now().Before(s.token.Expiry), nil
}

func (s *fakeStore) sameToken(token Token) bool {
	return s.held && s.token.Holder == token.Holder && s.token.Fence == token.Fence
}

func newTestCoordinator() (*Coordinator, *fakeStore, *fakeClock) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	store := newFakeStore(clock)
	return New(store), store, clock
}

func TestAcquireFirst(t *testing.T) {
	c, _, _ := newTestCoordinator()

	token, ok, err := c.Acquire(context.Background(), "a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire = (%v, %v), want ok", err, ok)
	}
	if token.Fence != 1 {
		t.Fatalf("fence = %d, want 1", token.Fence)
	}
	if got := c.Fence(); got != token {
		t.Fatalf("Fence = %+v, want %+v", got, token)
	}
}

func TestSecondAcquireRejectedWhileHeld(t *testing.T) {
	c, _, _ := newTestCoordinator()
	if _, ok, err := c.Acquire(context.Background(), "a", time.Minute); !ok || err != nil {
		t.Fatalf("first Acquire = (%v, %v), want ok", err, ok)
	}

	if _, ok, err := c.Acquire(context.Background(), "b", time.Minute); ok || err != nil {
		t.Fatalf("second Acquire = (%v, %v), want rejected", err, ok)
	}
	if got := c.Fence(); got.Holder != "a" || got.Fence != 1 {
		t.Fatalf("Fence = %+v, want holder a fence 1", got)
	}
}

func TestExpiryAllowsNewHolderWithNextFence(t *testing.T) {
	c, _, clock := newTestCoordinator()
	if _, ok, err := c.Acquire(context.Background(), "a", time.Minute); !ok || err != nil {
		t.Fatalf("Acquire a = (%v, %v)", err, ok)
	}
	clock.Advance(time.Minute)

	token, ok, err := c.Acquire(context.Background(), "b", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire b = (%v, %v), want ok", err, ok)
	}
	if token.Holder != "b" || token.Fence != 2 {
		t.Fatalf("token = %+v, want holder b fence 2", token)
	}
}

func TestRenewWrongHolderRejected(t *testing.T) {
	c, _, clock := newTestCoordinator()
	token, ok, err := c.Acquire(context.Background(), "a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire = (%v, %v)", err, ok)
	}
	clock.Advance(30 * time.Second)

	forged := token
	forged.Holder = "b"
	if _, ok, err := c.Renew(context.Background(), forged, time.Minute); ok || err != nil {
		t.Fatalf("Renew wrong holder = (%v, %v), want rejected", err, ok)
	}
	forged = token
	forged.Fence++
	if _, ok, err := c.Renew(context.Background(), forged, time.Minute); ok || err != nil {
		t.Fatalf("Renew wrong fence = (%v, %v), want rejected", err, ok)
	}
	if got := c.Fence(); got != token {
		t.Fatalf("Fence = %+v, want %+v", got, token)
	}
}

func TestRenewExtendsExpiry(t *testing.T) {
	c, _, clock := newTestCoordinator()
	token, ok, err := c.Acquire(context.Background(), "a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire = (%v, %v)", err, ok)
	}
	clock.Advance(50 * time.Second)

	renewed, ok, err := c.Renew(context.Background(), token, 2*time.Minute)
	if err != nil || !ok {
		t.Fatalf("Renew = (%v, %v), want ok", err, ok)
	}
	if renewed.Fence != token.Fence || !renewed.Expiry.After(token.Expiry) {
		t.Fatalf("renewed = %+v, want same fence and later expiry than %+v", renewed, token)
	}
}

func TestStaleRenewAfterTakeoverRejected(t *testing.T) {
	c, _, clock := newTestCoordinator()
	old, ok, err := c.Acquire(context.Background(), "a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire a = (%v, %v)", err, ok)
	}
	clock.Advance(time.Minute)
	if _, ok, err := c.Acquire(context.Background(), "b", time.Minute); !ok || err != nil {
		t.Fatalf("Acquire b = (%v, %v)", err, ok)
	}

	if _, ok, err := c.Renew(context.Background(), old, time.Minute); ok || err != nil {
		t.Fatalf("Renew stale = (%v, %v), want rejected", err, ok)
	}
}

func TestReleaseAllowsReacquireWithNextFence(t *testing.T) {
	c, _, _ := newTestCoordinator()
	token, ok, err := c.Acquire(context.Background(), "a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire a = (%v, %v)", err, ok)
	}
	if err := c.Release(context.Background(), token); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := c.Fence(); got != (Token{}) {
		t.Fatalf("Fence after release = %+v, want zero", got)
	}

	next, ok, err := c.Acquire(context.Background(), "b", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire b = (%v, %v)", err, ok)
	}
	if next.Fence != token.Fence+1 {
		t.Fatalf("fence = %d, want %d", next.Fence, token.Fence+1)
	}

	if err := c.Release(context.Background(), token); err != nil {
		t.Fatalf("stale Release: %v", err)
	}

	stale, ok, err := c.Acquire(context.Background(), "c", time.Minute)
	if err != nil || ok {
		t.Fatalf("stale Acquire = (%v, %v), want rejected", err, ok)
	}
	if stale != (Token{}) {
		t.Fatalf("stale token = %+v, want zero", stale)
	}
}

func TestConcurrentAcquireHasOneWinner(t *testing.T) {
	c, _, _ := newTestCoordinator()
	const workers = 32

	start := make(chan struct{})
	var wg sync.WaitGroup
	var winners atomic.Int32
	fences := make(chan int64, workers)

	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			token, ok, err := c.Acquire(context.Background(), "holder-"+string(rune('a'+i)), time.Minute)
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			if ok {
				winners.Add(1)
				fences <- token.Fence
			}
		}()
	}
	close(start)
	wg.Wait()
	close(fences)

	if got := winners.Load(); got != 1 {
		t.Fatalf("winners = %d, want 1", got)
	}
	for fence := range fences {
		if fence != 1 {
			t.Fatalf("winner fence = %d, want 1", fence)
		}
	}
}

func TestCoordinatorValidRejectsExpiredLease(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)}
	coordinator := New(newFakeStore(clock))
	if _, ok, err := coordinator.Acquire(context.Background(), "node-a", time.Second); err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	if valid, err := coordinator.Valid(context.Background()); err != nil || !valid {
		t.Fatalf("valid immediately: ok=%v err=%v", valid, err)
	}
	clock.Advance(2 * time.Second)
	if valid, err := coordinator.Valid(context.Background()); err != nil || valid {
		t.Fatalf("valid after expiry: ok=%v err=%v", valid, err)
	}
	if token := coordinator.Fence(); token.Holder != "" {
		t.Fatalf("expired token was not cleared: %+v", token)
	}
}
