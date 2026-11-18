package audit

import (
	"context"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/degraded"
	"golang.org/x/sync/singleflight"
)

// distributionTTL is how long one range-placement answer is reused.
const distributionTTL = 2 * time.Second

// distributionMaxStale bounds how old an answer may be served while a
// refresh runs; beyond it callers wait for the refresh (for at most
// degraded.Wait when the store has stopped answering).
const distributionMaxStale = 30 * time.Second

// distributionQueryTimeout bounds one shared placement query, detached from
// the first caller's context.
const distributionQueryTimeout = 20 * time.Second

// distributionCache serves GET /internal/audit/distribution from one range
// placement query at a time (request coalescing) and keeps the previous
// answer while a refresh runs (stale-while-revalidate).
//
// The query walks SHOW RANGES with replica and lease placement. While the
// cluster rebalances after a node rejoins, and with dead node IDs still
// listed, it took more than the 5 s request deadline and the probe recorded
// timeouts on an otherwise healthy store (P1.4, 17 Nov). Placement changes
// over seconds, so an answer up to distributionTTL old is as good as a live
// one, and up to distributionMaxStale old is still better than none.
//
// When the store does not answer at all (the query fails or outlasts
// degraded.Wait), the last placement is returned however old, with a
// degraded.Info the handler turns into partial: true (see checksumCache).
type distributionCache struct {
	now    func() time.Time
	flight singleflight.Group

	mu    sync.Mutex
	value Distribution
	at    time.Time
	known bool
	// failed is the error of the last placement query, nil once one
	// succeeded; failedAt is when.
	failed   error
	failedAt time.Time
}

type distributionEntry struct {
	value Distribution
	at    time.Time
}

// get returns the placement: cached (fresh, or stale while one query runs),
// or from one query shared by every concurrent caller. When the query fails
// the previous placement is returned with a degraded.Info; the error is
// returned only when there has never been an answer.
func (c *distributionCache) get(ctx context.Context, load func(context.Context) (Distribution, error)) (Distribution, degraded.Info, error) {
	c.mu.Lock()
	value, at, known, failed, failedAt := c.value, c.at, c.known, c.failed, c.failedAt
	c.mu.Unlock()
	stale := func(cause error) degraded.Info { return degraded.Info{Cause: cause, Since: at} }
	if known {
		age := c.now().Sub(at)
		if age < distributionTTL {
			return value, degraded.Info{}, nil
		}
		if failed != nil {
			// Known to be failing: answer at once, retry behind the caller
			// once per distributionTTL.
			if c.now().Sub(failedAt) >= distributionTTL {
				c.refresh(ctx, load)
			}
			return value, stale(failed), nil
		}
		if age < distributionMaxStale {
			c.refresh(ctx, load)
			return value, degraded.Info{}, nil
		}
	}
	result := c.refresh(ctx, load)
	// With an old placement in hand the wait is bounded; without one the
	// caller's own deadline applies.
	var giveUp <-chan time.Time
	if known {
		timer := time.NewTimer(degraded.Wait)
		defer timer.Stop()
		giveUp = timer.C
	}
	select {
	case <-ctx.Done():
		if known {
			return value, stale(ctx.Err()), nil
		}
		return Distribution{}, degraded.Info{}, ctx.Err()
	case <-giveUp:
		return value, stale(degraded.ErrSlow), nil
	case answer := <-result:
		if answer.Err != nil {
			if known {
				return value, stale(answer.Err), nil
			}
			return Distribution{}, degraded.Info{}, answer.Err
		}
		return answer.Val.(distributionEntry).value, degraded.Info{}, nil
	}
}

func (c *distributionCache) refresh(ctx context.Context, load func(context.Context) (Distribution, error)) <-chan singleflight.Result {
	return c.flight.DoChan("distribution", func() (any, error) {
		shared, cancel := context.WithTimeout(context.WithoutCancel(ctx), distributionQueryTimeout)
		defer cancel()
		value, err := load(shared)
		if err != nil {
			c.mu.Lock()
			c.failed, c.failedAt = err, c.now()
			c.mu.Unlock()
			return nil, err
		}
		entry := distributionEntry{value: value, at: c.now()}
		c.mu.Lock()
		c.value, c.at, c.known = entry.value, entry.at, true
		c.failed, c.failedAt = nil, time.Time{}
		c.mu.Unlock()
		return entry, nil
	})
}
