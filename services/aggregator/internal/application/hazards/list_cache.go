package hazards

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/degraded"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"golang.org/x/sync/singleflight"
)

// listCacheMaxEntries bounds the cache; listings beyond it are still
// coalesced, just not kept.
const listCacheMaxEntries = 1024

// listMaxStale bounds how old an answer may be served while a refresh runs.
// Beyond it callers wait for the refresh instead (for at most degraded.Wait
// when the store has stopped answering).
const listMaxStale = 15 * time.Second

// listFailedKeep is how long after its last failed refresh a page is exempt
// from eviction: while the store fails for a key, the old page is the only
// answer there is. The bound keeps pages nobody asks for any more from
// holding the cache full for ever.
const listFailedKeep = 10 * time.Minute

// listQueryTimeout bounds one shared store query. It is detached from the
// first caller's context so that caller giving up does not fail the others
// waiting on the same query.
const listQueryTimeout = 10 * time.Second

// listCache serves identical listings from one store query.
//
// Under load most requests ask for the same page (the newest events of one
// source), and each one used to run its own scatter-gather query over every
// hash bucket of the index. Concurrent identical requests now share one query
// (request coalescing), and its answer is reused for ttl. Once it is older
// than ttl the old answer is still returned at once while one refresh runs in
// the background (stale-while-revalidate): under load the store query itself
// takes seconds, and making every caller wait for it turned each expiry into
// a wave of upstream timeouts. A listing is therefore at most ttl old when the
// store keeps up, and at most listMaxStale old when it is slow. Different
// filters, page sizes or page tokens are different keys.
//
// When the store does not answer at all (the refresh fails or outlasts
// degraded.Wait), the last page of that key is served however old it is,
// together with a degraded.Info the transport turns into partial: true. The
// age limit protects a reader from silently old data; it must not turn an
// outage of the store into an outage of every listing that was answerable a
// minute ago (18 Nov: five minutes of 502 for a page each instance still held).
type listCache struct {
	ttl    time.Duration
	now    func() time.Time
	flight singleflight.Group

	mu      sync.Mutex
	entries map[string]listEntry
}

type listEntry struct {
	items []domain.HazardEvent
	next  string
	at    time.Time
	// failed is the error of the last refresh of this key, nil once a
	// refresh succeeded (a success replaces the entry); failedAt is when.
	failed   error
	failedAt time.Time
}

func (e listEntry) degraded(cause error) degraded.Info {
	return degraded.Info{Cause: cause, Since: e.at}
}

func newListCache(ttl time.Duration) *listCache {
	return &listCache{ttl: ttl, now: time.Now, entries: map[string]listEntry{}}
}

func listKey(filter ListFilter) string {
	return fmt.Sprintf("%s|%s|%s|%d|%s", filter.Source, filter.HazardType, filter.Severity, filter.PageSize, filter.PageToken)
}

// get returns a cached page (fresh, or stale while one refresh runs), or runs
// load once for every concurrent caller of the same key and caches its
// result. Errors are not cached: when load fails the previous page of the key
// is returned with a degraded.Info, and only a key never answered before gets
// the error.
// Callers must treat the returned items as read-only: they are shared.
func (c *listCache) get(ctx context.Context, filter ListFilter, load func(context.Context) ([]domain.HazardEvent, string, error)) ([]domain.HazardEvent, string, degraded.Info, error) {
	key := listKey(filter)
	c.mu.Lock()
	entry, ok := c.entries[key]
	c.mu.Unlock()
	if ok {
		age := c.now().Sub(entry.at)
		if age < c.ttl {
			return entry.items, entry.next, degraded.Info{}, nil
		}
		if entry.failed != nil {
			// The store is known to be failing for this key: answer at once
			// and retry behind the caller, once per ttl. Waiting would add
			// the store's failure latency to every request of the outage.
			if c.now().Sub(entry.failedAt) >= c.ttl {
				c.refresh(ctx, key, load)
			}
			return entry.items, entry.next, entry.degraded(entry.failed), nil
		}
		if age < listMaxStale {
			c.refresh(ctx, key, load)
			return entry.items, entry.next, degraded.Info{}, nil
		}
	}
	result := c.refresh(ctx, key, load)
	// With an old page in hand the wait is bounded; without one there is
	// nothing else to answer with and the caller's own deadline applies.
	var giveUp <-chan time.Time
	if ok {
		timer := time.NewTimer(degraded.Wait)
		defer timer.Stop()
		giveUp = timer.C
	}
	select {
	case <-ctx.Done():
		if ok {
			return entry.items, entry.next, entry.degraded(ctx.Err()), nil
		}
		return nil, "", degraded.Info{}, ctx.Err()
	case <-giveUp:
		return entry.items, entry.next, entry.degraded(degraded.ErrSlow), nil
	case answer := <-result:
		if answer.Err != nil {
			if ok {
				return entry.items, entry.next, entry.degraded(answer.Err), nil
			}
			return nil, "", degraded.Info{}, answer.Err
		}
		fresh := answer.Val.(listEntry)
		return fresh.items, fresh.next, degraded.Info{}, nil
	}
}

// refresh starts (or joins) the one store query for key and returns the
// channel its answer arrives on; callers that serve a stale answer do not
// wait on it.
func (c *listCache) refresh(ctx context.Context, key string, load func(context.Context) ([]domain.HazardEvent, string, error)) <-chan singleflight.Result {
	return c.flight.DoChan(key, func() (any, error) {
		shared, cancel := context.WithTimeout(context.WithoutCancel(ctx), listQueryTimeout)
		defer cancel()
		items, next, err := load(shared)
		if err != nil {
			c.markFailed(key, err)
			return nil, err
		}
		entry := listEntry{items: items, next: next, at: c.now()}
		c.store(key, entry)
		return entry, nil
	})
}

// markFailed remembers that the store could not refresh key, so the next
// callers are answered from the old page at once and marked degraded.
func (c *listCache) markFailed(key string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[key]; ok {
		entry.failed, entry.failedAt = err, c.now()
		c.entries[key] = entry
	}
}

func (c *listCache) store(key string, entry listEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, replacing := c.entries[key]; !replacing && len(c.entries) >= listCacheMaxEntries {
		now := c.now()
		for k, old := range c.entries {
			// A page whose refresh is failing is the only answer its key
			// has: age alone does not evict it.
			if old.failed != nil && now.Sub(old.failedAt) < listFailedKeep {
				continue
			}
			if now.Sub(old.at) >= c.ttl {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= listCacheMaxEntries {
			return
		}
	}
	c.entries[key] = entry
}
