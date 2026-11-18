package httptransport

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var overloadRejections = promauto.NewCounter(prometheus.CounterOpts{
	Name: "client_api_overload_rejections_total",
	Help: "Requests turned away by the overload gate before tracing, logging and authentication.",
})

// overloadGate is the cheapest possible rejection, in front of every other
// middleware. The class limiter (priorityLimit) decides who is served, but it
// runs after tracing, request logging and token validation, so under a flood
// each rejected request still cost a span, a log line and a token check; at
// thousands of rejections per second that work starved the requests that
// were admitted. Past limit requests in flight the pod is overloaded anyway,
// and this answers 429 with no other work. Below it nothing changes, so Field
// Team priority still applies whenever the pod is merely busy.
// /health and /metrics are never turned away: a busy pod must stay Ready.
//
// The gate cannot authenticate, but it can recognise: priority remembers the
// bearer credentials that the class limiter recently saw belong to the Field
// Team or Ops, and those are let through up to twice the limit. Without it a
// Media flood past the limit turned away Field Team requests too (2.8 % in
// the 18 Nov shedding run), which is exactly what P5.5 forbids. An unknown or
// forged credential gains nothing: it is not in the set.
func overloadGate(limit int, priority *priorityTokens) gin.HandlerFunc {
	var inflight atomic.Int64
	return func(c *gin.Context) {
		switch c.Request.URL.Path {
		case "/health", "/metrics":
			c.Next()
			return
		}
		now := inflight.Add(1)
		if now > int64(limit) && !(now <= 2*int64(limit) && priority.has(c.GetHeader("Authorization"))) {
			inflight.Add(-1)
			overloadRejections.Inc()
			c.Header("Retry-After", "1")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "server overloaded"})
			return
		}
		defer inflight.Add(-1)
		c.Next()
	}
}

// priorityTokens is a small expiring set of Authorization header values that
// were validated as priority class.
type priorityTokens struct {
	mu      sync.RWMutex
	entries map[string]time.Time
}

const (
	priorityTTL = 60 * time.Second
	priorityMax = 20000
)

func newPriorityTokens() *priorityTokens {
	return &priorityTokens{entries: map[string]time.Time{}}
}

func (p *priorityTokens) mark(authorization string) {
	if p == nil || authorization == "" {
		return
	}
	now := time.Now()
	p.mu.RLock()
	until, known := p.entries[authorization]
	p.mu.RUnlock()
	if known && until.Sub(now) > priorityTTL/2 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.entries) >= priorityMax {
		for key, expiry := range p.entries {
			if expiry.Before(now) {
				delete(p.entries, key)
			}
		}
		if len(p.entries) >= priorityMax {
			return
		}
	}
	p.entries[authorization] = now.Add(priorityTTL)
}

func (p *priorityTokens) has(authorization string) bool {
	if p == nil || authorization == "" {
		return false
	}
	p.mu.RLock()
	until, known := p.entries[authorization]
	p.mu.RUnlock()
	return known && time.Now().Before(until)
}
