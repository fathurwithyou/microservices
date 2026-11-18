// Package degraded describes read answers given while the store does not
// answer: the last good answer, explicitly marked as incomplete.
//
// On 18 Nov one store node's clock was pushed five minutes ahead and
// CockroachDB refused every statement for five minutes ("remote wall time is
// too far ahead"). Every read was answered 500/502 for that time, although
// each instance still held the answer it had given seconds before. M2 P1.4
// asks for the opposite: a complete answer, or a partial one that says so.
package degraded

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Wait bounds how long a request waits for a store that has stopped
// answering before it is given the last good answer instead. It has to stay
// well under the client-api's 2 s per-attempt budget: an answer that arrives
// after the caller gave up is no answer.
const Wait = time.Second

// StoreUnit is the unavailable_units entry used when the store as a whole
// did not answer and no node can be named.
const StoreUnit = "store"

// ErrSlow is the cause recorded when the store neither answered nor failed
// within Wait.
var ErrSlow = errors.New("store did not answer in time")

// Info travels with an answer. The zero value is a live answer.
type Info struct {
	// Cause is why the store could not answer; nil for a live answer.
	Cause error
	// Since is when the answer being served was read from the store. Zero
	// when the data itself is live and only the completeness lookup failed.
	Since time.Time
}

// Degraded reports whether the store failed to answer this request.
func (i Info) Degraded() bool { return i.Cause != nil }

// Completeness returns the partial / unavailable_units marker of one answer.
//
// lookup names the store nodes that are down (nil when the caller has none).
// It is itself a store query: when it fails, completeness is unknown, and
// unknown is reported as incomplete rather than turned into an error or,
// worse, into "complete". A degraded answer is always partial: it names the
// nodes last known to be down, or StoreUnit when none are known, so the
// marker is never an empty list.
func Completeness(ctx context.Context, lookup func(context.Context) ([]string, error), answer Info) (bool, []string, Info) {
	var units []string
	if lookup != nil {
		bounded, cancel := context.WithTimeout(ctx, Wait)
		found, err := lookup(bounded)
		cancel()
		units = found
		if err != nil && answer.Cause == nil {
			answer.Cause = fmt.Errorf("storage health: %w", err)
		}
	}
	if !answer.Degraded() {
		return len(units) > 0, units, answer
	}
	if len(units) == 0 {
		units = []string{StoreUnit}
	}
	return true, units, answer
}
