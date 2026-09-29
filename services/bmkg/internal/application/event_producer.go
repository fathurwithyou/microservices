// Package application holds the BMKG event producer.
package application

import (
	"context"
	"time"

	"github.com/fathurwithyou/microservices/services/bmkg/internal/state"
)

// Config configures an EventProducer. Interval must be positive; config
// validation enforces this so time.NewTicker cannot panic.
type Config struct {
	// State receives the events appended on each tick.
	State *state.Memory
	// Interval is the tick period for appending new events.
	Interval time.Duration
	// Now returns the current time. Defaults to time.Now().UTC() so tests
	// can inject a deterministic clock.
	Now func() time.Time
}

// EventProducer periodically appends simulated seismic events to the state.
type EventProducer struct {
	state    *state.Memory
	interval time.Duration
	now      func() time.Time
}

// NewEventProducer builds a producer from cfg.
func NewEventProducer(cfg Config) *EventProducer {
	now := cfg.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &EventProducer{state: cfg.State, interval: cfg.Interval, now: now}
}

// Run appends an event on every tick until ctx is canceled.
func (p *EventProducer) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.state.AppendEvent()
		case <-ctx.Done():
			return
		}
	}
}
