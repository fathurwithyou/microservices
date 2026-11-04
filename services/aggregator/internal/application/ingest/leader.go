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
	allowed atomic.Bool
}

// NewLeader returns a leader that denies work until the lease is acquired.
func NewLeader() *Leader { return &Leader{} }

// Allowed reports whether this instance currently owns the lease.
func (l *Leader) Allowed() bool { return l.allowed.Load() }

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
