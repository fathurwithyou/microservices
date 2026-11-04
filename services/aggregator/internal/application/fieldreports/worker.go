package fieldreports

import (
	"context"
	"log/slog"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

// Processor performs the downstream work for one accepted report. Returning an
// error marks the report failed and leaves it visible to operators.
type Processor interface {
	Process(context.Context, domain.FieldReport) error
}

// Worker drains accepted field reports asynchronously.
type Worker struct {
	repository Repository
	processor  Processor
	interval   time.Duration
	batch      int
	now        func() time.Time
	logger     *slog.Logger
}

// WorkerConfig configures a Worker.
type WorkerConfig struct {
	Repository Repository
	Processor  Processor
	Interval   time.Duration
	Batch      int
	Now        func() time.Time
	Logger     *slog.Logger
}

// NewWorker validates cfg and applies defaults.
func NewWorker(cfg WorkerConfig) (*Worker, error) {
	if cfg.Repository == nil {
		return nil, ErrInvalidReport
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 100
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Worker{repository: cfg.Repository, processor: cfg.Processor, interval: cfg.Interval, batch: cfg.Batch, now: cfg.Now, logger: cfg.Logger}, nil
}

// Run drains accepted reports until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		w.dispatch(ctx)
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) dispatch(ctx context.Context) {
	reports, err := w.repository.ListAcceptedFieldReports(ctx, w.batch)
	if err != nil {
		w.logger.Error("field report claim failed", "component", "field-reports", "error", err)
		return
	}
	for _, report := range reports {
		status := domain.FieldReportProcessed
		if w.processor != nil {
			if err := w.processor.Process(ctx, report); err != nil {
				status = domain.FieldReportFailed
				w.logger.Error("field report processing failed", "component", "field-reports", "report_id", report.ReportID, "error", err)
			}
		}
		if err := w.repository.MarkFieldReport(ctx, report.ReportID, status, w.now()); err != nil {
			w.logger.Error("field report acknowledgement failed", "component", "field-reports", "report_id", report.ReportID, "error", err)
		}
	}
}
