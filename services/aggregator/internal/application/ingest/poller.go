package ingest

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/ports"
)

type Poller struct {
	store     Store
	bmkg      ports.SourceClient
	pvmbg     ports.SourceClient
	interval  time.Duration
	timeout   time.Duration
	logger    *slog.Logger
	volcanoes map[string]domain.VolcanoReference
}

func NewPoller(store Store, bmkg, pvmbg ports.SourceClient, interval, timeout time.Duration, logger *slog.Logger, volcanoes map[string]domain.VolcanoReference) *Poller {
	return &Poller{store: store, bmkg: bmkg, pvmbg: pvmbg, interval: interval, timeout: timeout, logger: logger, volcanoes: cloneVolcanoes(volcanoes)}
}

func (p *Poller) Run(ctx context.Context) {
	var wait sync.WaitGroup
	wait.Add(2)
	go func() { defer wait.Done(); p.runSource(ctx, domain.SourceBMKG, p.pollBMKG) }()
	go func() { defer wait.Done(); p.runSource(ctx, domain.SourcePVMBG, p.pollPVMBG) }()
	wait.Wait()
}

func cloneVolcanoes(input map[string]domain.VolcanoReference) map[string]domain.VolcanoReference {
	output := make(map[string]domain.VolcanoReference, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
