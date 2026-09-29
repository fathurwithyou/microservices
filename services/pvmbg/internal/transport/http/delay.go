package httptransport

import (
	"time"
)

// randomDelay returns a delay uniformly sampled from the state's configured
// range. The RNG is injected through the router so delay behavior is
// deterministic in tests.
func (h handler) randomDelay() time.Duration {
	minDelay, maxDelay := h.state.DelayRange()
	if maxDelay <= minDelay {
		return minDelay
	}
	span := int64(maxDelay - minDelay)
	return minDelay + time.Duration(h.rand.Int64N(span+1))
}
