package grpcadapter

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/client-api/internal/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// A powered-off Aggregator never answers and never resets the connection:
// the attempt only ends when its deadline fires.
func silentPeer(ctx context.Context) (string, error) {
	<-ctx.Done()
	return "", status.FromContextError(ctx.Err()).Err()
}

func shortAttempts(t *testing.T) {
	t.Helper()
	previous := readAttemptTimeout
	readAttemptTimeout = 100 * time.Millisecond
	t.Cleanup(func() { readAttemptTimeout = previous })
}

func TestRetryReadMovesOnAfterSilentInstance(t *testing.T) {
	shortAttempts(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var attempts int32
	started := time.Now()
	value, err := retryRead(ctx, func(ctx context.Context) (string, error) {
		if atomic.AddInt32(&attempts, 1) == 1 {
			return silentPeer(ctx)
		}
		return "answered", nil
	})
	if err != nil || value != "answered" {
		t.Fatalf("read = %q, %v; want answered from the second instance", value, err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("read took %s, the silent instance must cost one attempt only", elapsed)
	}
}

func TestRetryReadGivesUpAfterTwoSilentInstances(t *testing.T) {
	shortAttempts(t)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	var attempts int32
	_, err := retryRead(ctx, func(ctx context.Context) (string, error) {
		atomic.AddInt32(&attempts, 1)
		return silentPeer(ctx)
	})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("err = %v, want DEADLINE_EXCEEDED", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want exactly 2", attempts)
	}
}

// A slow but live instance answers on the retry, which is not capped: the cap
// is there to leave a silent peer, not to turn a 150 ms query into a failure.
func TestRetryReadSecondAttemptKeepsCallerDeadline(t *testing.T) {
	shortAttempts(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var attempts int32
	value, err := retryRead(ctx, func(ctx context.Context) (string, error) {
		if atomic.AddInt32(&attempts, 1) == 1 {
			return silentPeer(ctx)
		}
		select {
		case <-time.After(250 * time.Millisecond): // longer than one capped attempt
			return "slow but alive", nil
		case <-ctx.Done():
			return "", status.FromContextError(ctx.Err()).Err()
		}
	})
	if err != nil || value != "slow but alive" {
		t.Fatalf("read = %q, %v; want the slow instance's answer", value, err)
	}
}

func TestRetryReadDoesNotRetryOtherFailures(t *testing.T) {
	shortAttempts(t)
	var attempts int32
	boom := status.Error(codes.Internal, "boom")
	_, err := retryRead(context.Background(), func(context.Context) (string, error) {
		atomic.AddInt32(&attempts, 1)
		return "", boom
	})
	if !errors.Is(err, boom) || attempts != 1 {
		t.Fatalf("err = %v after %d attempts; want the original error after one attempt", err, attempts)
	}
}

func TestRetryReadStopsWhenCallerDeadlineIsNear(t *testing.T) {
	shortAttempts(t)
	// Room for one attempt plus less than half an attempt: no retry.
	ctx, cancel := context.WithTimeout(context.Background(), 130*time.Millisecond)
	defer cancel()
	var attempts int32
	_, err := retryRead(ctx, func(ctx context.Context) (string, error) {
		atomic.AddInt32(&attempts, 1)
		return silentPeer(ctx)
	})
	if status.Code(err) != codes.DeadlineExceeded || attempts != 1 {
		t.Fatalf("err = %v after %d attempts; want DEADLINE_EXCEEDED after one attempt", err, attempts)
	}
}

// The Aggregator answers Unavailable when its store does not answer and it
// has nothing to fall back on. That must reach the HTTP layer as "upstream
// unavailable" (503), not as an upstream failure (502).
func TestStoreOutageIsUnavailableNotUpstreamFailure(t *testing.T) {
	err := grpcError(status.Error(codes.Unavailable, "aggregate window: remote wall time is too far ahead"))
	if !errors.Is(err, ports.ErrUnavailable) || errors.Is(err, ports.ErrUpstreamFailure) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}

// Unavailable is retried on other instances by the gRPC retry policy, inside
// one attempt; retryRead itself must not add a second round on top, so the
// answer (503) comes at once and well inside the request deadline.
func TestRetryReadStaysWithinTheDeadlineWhenEveryInstanceIsUnavailable(t *testing.T) {
	shortAttempts(t)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	started := time.Now()
	var attempts int32
	_, err := retryRead(ctx, func(context.Context) (string, error) {
		atomic.AddInt32(&attempts, 1)
		return "", status.Error(codes.Unavailable, "store down")
	})
	if status.Code(err) != codes.Unavailable || attempts != 1 {
		t.Fatalf("code = %s after %d attempts, want Unavailable after 1", status.Code(err), attempts)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("read took %s, want an immediate answer", elapsed)
	}
}

func TestStaleSinceComesFromTheAggregatorHeader(t *testing.T) {
	if got := staleSince(metadata.Pairs(staleSinceHeader, "2026-11-18T09:00:00Z")); got != "2026-11-18T09:00:00Z" {
		t.Fatalf("stale since = %q", got)
	}
	if got := staleSince(nil); got != "" {
		t.Fatalf("stale since = %q for a live answer, want empty", got)
	}
}
