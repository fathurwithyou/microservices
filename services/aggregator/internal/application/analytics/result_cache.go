package analytics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/degraded"
	"golang.org/x/sync/singleflight"
)

// resultCacheMaxEntries bounds the cache; windows beyond it are still
// coalesced, just not kept.
const resultCacheMaxEntries = 256

// resultMaxStale bounds how old an aggregate may be served while a refresh
// runs. Beyond it callers wait for the refresh instead (for at most
// degraded.Wait when the store has stopped answering). Dashboard counts
// tolerate a minute; a listing (15 s) does not.
const resultMaxStale = 60 * time.Second

// resultFailedKeep is how long after its last failed refresh a window is
// exempt from eviction (see listFailedKeep in the hazards list cache).
const resultFailedKeep = 10 * time.Minute

// resultQueryTimeout bounds one shared set of aggregate queries, detached from
// the first caller's context so that caller giving up does not fail the
// others waiting on the same window.
const resultQueryTimeout = 20 * time.Second

// aggregates is the cached part of a Result: the counts and the snapshot
// time they were read at. Partial and UnavailableUnits are never cached, so
// the completeness marker stays as fresh as the store-health check itself.
type aggregates struct {
	HazardTypes []TypeCount
	Severities  []SeverityCount
	Areas       []AreaCount
	Buckets     []TimeBucket
	AsOf        time.Time
}

type resultEntry struct {
	value aggregates
	at    time.Time
	// failed is the error of the last refresh of this window, nil once a
	// refresh succeeded (a success replaces the entry); failedAt is when.
	failed   error
	failedAt time.Time
}

func (e resultEntry) degraded(cause error) degraded.Info {
	return degraded.Info{Cause: cause, Since: e.at}
}

// resultCache serves identical analytics windows from one set of store
// queries (request coalescing) and keeps serving the previous answer while a
// refresh runs once it is older than ttl (stale-while-revalidate).
//
// The four aggregate queries scan every event in the window. During
// re-replication after a node loss they took more than the 5 s request
// deadline on a 390k-event store, and every dashboard request then failed
// with 502 (P1.4, 17 Nov). With the cache one query runs per window per ttl,
// and a slow store makes the dashboard up to resultMaxStale old, not down.
//
// A store that does not answer at all (the refresh fails or outlasts
// degraded.Wait) gets the same treatment as in the hazards list cache: the
// last counts of the window are served however old, with a degraded.Info
// that Queries.Get turns into partial: true. as_of already tells the reader
// how old they are.
type resultCache struct {
	ttl    time.Duration
	now    func() time.Time
	flight singleflight.Group

	mu      sync.Mutex
	entries map[string]resultEntry
}

func newResultCache(ttl time.Duration) *resultCache {
	return &resultCache{ttl: ttl, now: time.Now, entries: map[string]resultEntry{}}
}

// resultKey identifies a window as the caller asked for it, before defaults
// are applied: a request without until means "up to now", and stamping now
// into the key would make every such request a miss.
func resultKey(filter Filter) string {
	return fmt.Sprintf("%d|%d|%s", filter.Since.UnixNano(), filter.Until.UnixNano(), filter.Bucket)
}

// get returns cached aggregates (fresh, or stale while one refresh runs), or
// runs load once for every concurrent caller of the same key and caches its
// result. Errors are not cached: when load fails the previous counts of the
// window are returned with a degraded.Info, and only a window never answered
// before gets the error. Returned slices are shared: read-only.
func (c *resultCache) get(ctx context.Context, key string, load func(context.Context) (aggregates, error)) (aggregates, degraded.Info, error) {
	c.mu.Lock()
	entry, ok := c.entries[key]
	c.mu.Unlock()
	if ok {
		age := c.now().Sub(entry.at)
		if age < c.ttl {
			return entry.value, degraded.Info{}, nil
		}
		if entry.failed != nil {
			// Known to be failing: answer at once, retry behind the caller
			// once per ttl.
			if c.now().Sub(entry.failedAt) >= c.ttl {
				c.refresh(ctx, key, load)
			}
			return entry.value, entry.degraded(entry.failed), nil
		}
		if age < resultMaxStale {
			c.refresh(ctx, key, load)
			return entry.value, degraded.Info{}, nil
		}
	}
	result := c.refresh(ctx, key, load)
	// With old counts in hand the wait is bounded; without them the caller's
	// own deadline applies.
	var giveUp <-chan time.Time
	if ok {
		timer := time.NewTimer(degraded.Wait)
		defer timer.Stop()
		giveUp = timer.C
	}
	select {
	case <-ctx.Done():
		if ok {
			return entry.value, entry.degraded(ctx.Err()), nil
		}
		return aggregates{}, degraded.Info{}, ctx.Err()
	case <-giveUp:
		return entry.value, entry.degraded(degraded.ErrSlow), nil
	case answer := <-result:
		if answer.Err != nil {
			if ok {
				return entry.value, entry.degraded(answer.Err), nil
			}
			return aggregates{}, degraded.Info{}, answer.Err
		}
		return answer.Val.(resultEntry).value, degraded.Info{}, nil
	}
}

func (c *resultCache) refresh(ctx context.Context, key string, load func(context.Context) (aggregates, error)) <-chan singleflight.Result {
	return c.flight.DoChan(key, func() (any, error) {
		shared, cancel := context.WithTimeout(context.WithoutCancel(ctx), resultQueryTimeout)
		defer cancel()
		value, err := load(shared)
		if err != nil {
			c.markFailed(key, err)
			return nil, err
		}
		entry := resultEntry{value: value, at: c.now()}
		c.store(key, entry)
		return entry, nil
	})
}

// markFailed remembers that the store could not refresh key, so the next
// callers are answered from the old counts at once and marked degraded.
func (c *resultCache) markFailed(key string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[key]; ok {
		entry.failed, entry.failedAt = err, c.now()
		c.entries[key] = entry
	}
}

func (c *resultCache) store(key string, entry resultEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, replacing := c.entries[key]; !replacing && len(c.entries) >= resultCacheMaxEntries {
		now := c.now()
		for k, old := range c.entries {
			// Counts whose refresh is failing are the only answer their
			// window has: age alone does not evict them.
			if old.failed != nil && now.Sub(old.failedAt) < resultFailedKeep {
				continue
			}
			if now.Sub(old.at) >= c.ttl {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= resultCacheMaxEntries {
			return
		}
	}
	c.entries[key] = entry
}
