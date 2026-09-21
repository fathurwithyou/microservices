package application

import (
	"context"
	"time"

	"github.com/fathurwithyou/microservices/services/bmkg/internal/state"
)

type EventProducer struct {
	state    *state.Memory
	interval time.Duration
}

func NewEventProducer(state *state.Memory, interval time.Duration) *EventProducer {
	return &EventProducer{state: state, interval: interval}
}

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
