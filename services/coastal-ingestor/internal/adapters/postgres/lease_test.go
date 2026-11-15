package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/pkg/lease"
	"github.com/google/uuid"
)

func TestLeaseStoreSingleHolderAndFencing(t *testing.T) {
	databaseURL := os.Getenv("COASTAL_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("COASTAL_DATABASE_URL not set")
	}
	ctx := context.Background()
	state, err := New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	name := "test-" + uuid.NewString()
	first := lease.New(state.LeaseStore(name))
	second := lease.New(state.LeaseStore(name))

	token, ok, err := first.Acquire(ctx, "replica-a", 2*time.Second)
	if err != nil || !ok {
		t.Fatalf("first acquire ok=%v err=%v", ok, err)
	}
	if _, ok, err := second.Acquire(ctx, "replica-b", 2*time.Second); err != nil || ok {
		t.Fatalf("second acquired a live lease: ok=%v err=%v", ok, err)
	}
	time.Sleep(2500 * time.Millisecond)
	taken, ok, err := second.Acquire(ctx, "replica-b", 2*time.Second)
	if err != nil || !ok || taken.Fence <= token.Fence {
		t.Fatalf("takeover ok=%v err=%v fence %d -> %d", ok, err, token.Fence, taken.Fence)
	}
	// The frozen former holder must learn it lost the lease.
	if valid, err := first.Valid(ctx); err != nil || valid {
		t.Fatalf("stale holder still valid=%v err=%v", valid, err)
	}
}
