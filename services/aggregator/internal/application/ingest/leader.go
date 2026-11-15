package ingest

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/fathurwithyou/microservices/pkg/lease"
)

// Leadership gates source polling to the instance that currently owns the
// database lease. The zero value denies all work.
type Leadership interface {
	Allowed() bool
}

// Leader acquires and renews a lease, flipping Allowed as ownership changes.
type Leader struct {
	allowed     atomic.Bool
	coordinator *lease.Coordinator
}

// NewLeader returns a leader that denies work until the lease is acquired.
func NewLeader(coordinator *lease.Coordinator) *Leader {
	return &Leader{coordinator: coordinator}
}

// Allowed reports whether this instance believes it owns the lease.
func (l *Leader) Allowed() bool { return l.allowed.Load() }

// StillLeader revalidates the lease against the database clock. A process that
// was frozen past the TTL fails this check before its next write.
func (l *Leader) StillLeader(ctx context.Context) (bool, error) {
	if !l.allowed.Load() || l.coordinator == nil {
		return false, nil
	}
	valid, err := l.coordinator.Valid(ctx)
	if err != nil {
		// Unknown is not lost: refuse this write, but leave ownership to the
		// renew loop. Clearing the flag here while Run keeps renewing would
		// park the lease on an instance that never works again, and no other
		// instance could take over.
		return false, err
	}
	if !valid {
		l.allowed.Store(false)
		return false, nil
	}
	return true, nil
}

// Run owns the acquire/renew loop until ctx is cancelled. The database clock,
// not the local clock, governs expiry and fencing.
func (l *Leader) Run(ctx context.Context, coordinator *lease.Coordinator, holder string, ttl, renew time.Duration, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	for ctx.Err() == nil {
		token, acquired, err := coordinator.Acquire(ctx, holder, ttl)
		if err != nil {
			l.allowed.Store(false)
			logger.Error("leader acquire failed", "component", "leader", "error", err)
			if !sleep(ctx, renew) {
				return
			}
			continue
		}
		if !acquired {
			l.allowed.Store(false)
			if !sleep(ctx, renew) {
				return
			}
			continue
		}
		l.allowed.Store(true)
		logger.Info("leader acquired", "component", "leader", "holder", token.Holder, "fence", token.Fence)
		for ctx.Err() == nil {
			if !sleep(ctx, renew) {
				break
			}
			renewed, ok, err := coordinator.Renew(ctx, token, ttl)
			if err != nil || !ok {
				l.allowed.Store(false)
				if err != nil {
					logger.Error("leader renew failed", "component", "leader", "error", err)
				} else {
					logger.Warn("leader lease lost", "component", "leader", "fence", token.Fence)
				}
				break
			}
			token = renewed
			// A renewal conditioned on the database clock proves ownership,
			// so it restores the flag after a definitive StillLeader miss.
			l.allowed.Store(true)
		}
		l.allowed.Store(false)
	}
	l.allowed.Store(false)
}

func sleep(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
