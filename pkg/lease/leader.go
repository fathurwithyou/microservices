package lease

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// Leader runs the acquire/renew loop for one Coordinator and exposes whether
// this process may currently act as the single writer.
type Leader struct {
	allowed     atomic.Bool
	coordinator *Coordinator
}

// NewLeader returns a Leader that denies work until the lease is acquired.
func NewLeader(coordinator *Coordinator) *Leader {
	return &Leader{coordinator: coordinator}
}

// Allowed reports whether this instance believes it owns the lease. It is a
// cheap local check; call StillLeader right before a side effect.
func (l *Leader) Allowed() bool { return l.allowed.Load() }

// StillLeader revalidates the lease against the database clock, so a process
// that was frozen past its TTL learns it lost ownership before writing.
func (l *Leader) StillLeader(ctx context.Context) (bool, error) {
	if !l.allowed.Load() || l.coordinator == nil {
		return false, nil
	}
	valid, err := l.coordinator.Valid(ctx)
	if err != nil || !valid {
		l.allowed.Store(false)
		return false, err
	}
	return true, nil
}

// Run owns the acquire and renew loop until ctx is cancelled.
func (l *Leader) Run(ctx context.Context, holder string, ttl, renew time.Duration, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	defer l.allowed.Store(false)
	for ctx.Err() == nil {
		token, acquired, err := l.coordinator.Acquire(ctx, holder, ttl)
		if err != nil || !acquired {
			l.allowed.Store(false)
			if err != nil {
				logger.Error("leader acquire failed", "component", "leader", "error", err)
			}
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
			renewed, ok, err := l.coordinator.Renew(ctx, token, ttl)
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
