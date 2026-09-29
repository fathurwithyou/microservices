package application

import (
	"context"
	"errors"
	"time"

	"github.com/fathurwithyou/microservices/services/pvmbg/internal/state"
)

// ReportProducer appends synthetic reports on a ticker until the context is
// cancelled.
type ReportProducer struct {
	state    *state.Runtime
	interval time.Duration
}

// Config configures a ReportProducer.
type Config struct {
	// State receives appended reports. Required.
	State *state.Runtime
	// Interval between appends. Must be positive; config validation
	// rejects non-positive values because time.NewTicker panics on them.
	Interval time.Duration
}

// NewReportProducer validates cfg and returns a ReportProducer.
func NewReportProducer(cfg Config) (*ReportProducer, error) {
	if cfg.State == nil {
		return nil, errors.New("application: state is required")
	}
	if cfg.Interval <= 0 {
		return nil, errors.New("application: interval must be positive")
	}
	return &ReportProducer{state: cfg.State, interval: cfg.Interval}, nil
}

// Run appends reports until ctx is cancelled.
func (p *ReportProducer) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.state.AppendReport()
		case <-ctx.Done():
			return
		}
	}
}
