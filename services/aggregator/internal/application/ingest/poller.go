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
	repository Repository
	bmkg       ports.SourceClient
	pvmbg      ports.SourceClient
	interval   time.Duration
	timeout    time.Duration
	logger     *slog.Logger
	volcanoes  map[string]domain.VolcanoReference
}

type PollerConfig struct {
	Repository Repository
	BMKG       ports.SourceClient
	PVMBG      ports.SourceClient
	Interval   time.Duration
	Timeout    time.Duration
	Logger     *slog.Logger
	Volcanoes  map[string]domain.VolcanoReference
}

func NewPoller(cfg PollerConfig) *Poller {
	return &Poller{repository: cfg.Repository, bmkg: cfg.BMKG, pvmbg: cfg.PVMBG, interval: cfg.Interval, timeout: cfg.Timeout, logger: cfg.Logger, volcanoes: cloneVolcanoes(cfg.Volcanoes)}
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
