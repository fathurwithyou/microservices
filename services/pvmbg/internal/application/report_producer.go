package application

import (
	"context"
	"time"

	"github.com/fathurwithyou/microservices/services/pvmbg/internal/state"
)

type ReportProducer struct {
	state    *state.Runtime
	interval time.Duration
}

func NewReportProducer(state *state.Runtime, interval time.Duration) *ReportProducer {
	return &ReportProducer{state: state, interval: interval}
}

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
