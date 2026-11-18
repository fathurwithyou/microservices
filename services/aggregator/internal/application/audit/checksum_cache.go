package audit

import (
	"context"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/degraded"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"golang.org/x/sync/singleflight"
)

// checksumTTL is how long one source checksum is reused. An auditor comparing
// against the assistants' checksum does not need a figure newer than two
// seconds; ingestion that arrived in between shows up on the next refresh.
const checksumTTL = 2 * time.Second

// checksumMaxStale bounds how old a checksum may be served while a refresh
// runs; beyond it callers wait for the refresh (for at most degraded.Wait
// when the store has stopped answering).
const checksumMaxStale = 30 * time.Second

// checksumQueryTimeout bounds one shared scan, detached from the first
// caller's context.
const checksumQueryTimeout = 20 * time.Second

type checksumValue struct {
	count    int
	checksum string
}

type checksumEntry struct {
	value checksumValue
	at    time.Time
	// failed is the error of the last scan of this source, nil once a scan
	// succeeded (a success replaces the entry); failedAt is when.
	failed   error
	failedAt time.Time
}

func (e checksumEntry) degraded(cause error) degraded.Info {
	return degraded.Info{Cause: cause, Since: e.at}
}

// checksumCache serves GET /internal/audit/checksum per source from one scan
// at a time (request coalescing) and keeps the previous answer while a refresh
// runs (stale-while-revalidate), the same way distributionCache does.
//
// The checksum sorts every source_ref_id of the source (a quarter million
// rows for one source on the testbed). Polled every second during the P1
// probe, with the store busy re-replicating, single scans took over the 5 s
// request deadline (18 Nov); one scan per source per checksumTTL is the
// most the endpoint should cost whatever the polling rate.
//
// When the store does not answer at all (the scan fails or outlasts
// degraded.Wait), the last checksum of the source is returned however old,
// with a degraded.Info: Lampiran A wants partial: true there, not an error,
// and the figure of a minute ago with its as_of is what an auditor can still
// be told (18 Nov: 500 for five minutes while the store refused statements).
type checksumCache struct {
	now    func() time.Time
	flight singleflight.Group

	mu      sync.Mutex
	entries map[domain.Source]checksumEntry
}

// get returns the checksum of source: cached (fresh, or stale while one scan
// runs), or from one scan shared by every concurrent caller. When the scan
// fails the previous checksum is returned with a degraded.Info; only a source
// never scanned before gets the error.
func (c *checksumCache) get(ctx context.Context, source domain.Source, load func(context.Context) (checksumValue, error)) (checksumValue, degraded.Info, error) {
	c.mu.Lock()
	if c.entries == nil {
		c.entries = map[domain.Source]checksumEntry{}
	}
	entry, known := c.entries[source]
	c.mu.Unlock()
	if known {
		age := c.now().Sub(entry.at)
		if age < checksumTTL {
			return entry.value, degraded.Info{}, nil
		}
		if entry.failed != nil {
			// Known to be failing: answer at once, retry behind the caller
			// once per checksumTTL.
			if c.now().Sub(entry.failedAt) >= checksumTTL {
				c.refresh(ctx, source, load)
			}
			return entry.value, entry.degraded(entry.failed), nil
		}
		if age < checksumMaxStale {
			c.refresh(ctx, source, load)
			return entry.value, degraded.Info{}, nil
		}
	}
	result := c.refresh(ctx, source, load)
	// With an old checksum in hand the wait is bounded; without one the
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
			return entry.value, entry.degraded(ctx.Err()), nil
		}
		return checksumValue{}, degraded.Info{}, ctx.Err()
	case <-giveUp:
		return entry.value, entry.degraded(degraded.ErrSlow), nil
	case answer := <-result:
		if answer.Err != nil {
			if known {
				return entry.value, entry.degraded(answer.Err), nil
			}
			return checksumValue{}, degraded.Info{}, answer.Err
		}
		return answer.Val.(checksumEntry).value, degraded.Info{}, nil
	}
}

func (c *checksumCache) refresh(ctx context.Context, source domain.Source, load func(context.Context) (checksumValue, error)) <-chan singleflight.Result {
	return c.flight.DoChan(string(source), func() (any, error) {
		shared, cancel := context.WithTimeout(context.WithoutCancel(ctx), checksumQueryTimeout)
		defer cancel()
		value, err := load(shared)
		if err != nil {
			c.mu.Lock()
			if entry, known := c.entries[source]; known {
				entry.failed, entry.failedAt = err, c.now()
				c.entries[source] = entry
			}
			c.mu.Unlock()
			return nil, err
		}
		entry := checksumEntry{value: value, at: c.now()}
		c.mu.Lock()
		c.entries[source] = entry
		c.mu.Unlock()
		return entry, nil
	})
}
