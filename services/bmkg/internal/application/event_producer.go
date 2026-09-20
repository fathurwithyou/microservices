package application

import (
	"context"
	"time"

	"github.com/fathurwithyou/microservices/services/bmkg/internal/store"
)

type EventProducer struct {
	store    *store.Memory
	interval time.Duration
}

func NewEventProducer(store *store.Memory, interval time.Duration) *EventProducer {
	return &EventProducer{store: store, interval: interval}
}

func (p *EventProducer) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.store.AppendEvent()
		case <-ctx.Done():
			return
		}
	}
}
