package application

import (
	"sync"
	"time"
)

// CircuitState is the breaker's current mode.
type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half_open"
)

// CircuitConfig tunes the breaker's trip and recovery behaviour.
type CircuitConfig struct {
	// FailureThreshold is the number of consecutive failures that opens the
	// breaker.
	FailureThreshold int
	// FailureWindow resets the failure streak when the gap since the last
	// failure exceeds it.
	FailureWindow time.Duration
	// OpenTimeout is how long the breaker stays open before a probe.
	OpenTimeout time.Duration
}

// DefaultCircuitConfig returns the production breaker policy.
func DefaultCircuitConfig() CircuitConfig {
	return CircuitConfig{
		FailureThreshold: 5,
		FailureWindow:    30 * time.Second,
		OpenTimeout:      30 * time.Second,
	}
}

// CircuitBreaker trips after repeated failures and allows one probe request
// at a time when half-open. It is safe for concurrent use.
type CircuitBreaker struct {
	cfg CircuitConfig
	now Clock

	mu            sync.Mutex
	state         CircuitState
	failures      int
	lastFailureAt time.Time
	openedAt      time.Time
	probeInFlight bool
}

// NewCircuitBreaker builds a breaker, defaulting non-positive config values
// and a nil clock.
func NewCircuitBreaker(cfg CircuitConfig, now Clock) *CircuitBreaker {
	defaults := DefaultCircuitConfig()
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = defaults.FailureThreshold
	}
	if cfg.FailureWindow <= 0 {
		cfg.FailureWindow = defaults.FailureWindow
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = defaults.OpenTimeout
	}
	if now == nil {
		now = time.Now
	}
	return &CircuitBreaker{cfg: cfg, now: now, state: CircuitClosed}
}

// Allow reports whether a request may proceed. In half-open it admits exactly
// one probe until the outcome is recorded.
func (b *CircuitBreaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if b.now().Sub(b.openedAt) < b.cfg.OpenTimeout {
			return false
		}
		b.setState(CircuitHalfOpen)
		b.probeInFlight = true
		return true
	default: // CircuitHalfOpen
		if b.probeInFlight {
			return false
		}
		b.probeInFlight = true
		return true
	}
}

// RecordSuccess closes the breaker and clears the failure streak.
func (b *CircuitBreaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.setState(CircuitClosed)
	b.failures = 0
	b.lastFailureAt = time.Time{}
	b.openedAt = time.Time{}
	b.probeInFlight = false
}

// RecordFailure counts a failure and trips or reopens the breaker when the
// threshold is reached or a half-open probe fails.
func (b *CircuitBreaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	if b.state == CircuitHalfOpen {
		b.trip(now)
		return
	}
	if b.state == CircuitOpen {
		return
	}
	if !b.lastFailureAt.IsZero() && now.Sub(b.lastFailureAt) > b.cfg.FailureWindow {
		b.failures = 0
	}
	b.failures++
	b.lastFailureAt = now
	if b.failures >= b.cfg.FailureThreshold {
		b.trip(now)
	}
}

// State reports the current breaker state.
func (b *CircuitBreaker) State() CircuitState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *CircuitBreaker) trip(now time.Time) {
	b.setState(CircuitOpen)
	b.openedAt = now
	b.failures = 0
	b.lastFailureAt = time.Time{}
	b.probeInFlight = false
}

// setState records the transition in coastal_circuit_state (0 closed,
// 1 half-open, 2 open) so dashboards show when the sensor is cut off.
func (b *CircuitBreaker) setState(state CircuitState) {
	b.state = state
	switch state {
	case CircuitOpen:
		circuitState.Set(2)
	case CircuitHalfOpen:
		circuitState.Set(1)
	default:
		circuitState.Set(0)
	}
}
