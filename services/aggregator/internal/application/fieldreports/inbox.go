package fieldreports

import (
	"context"
	"log/slog"

	"github.com/fathurwithyou/microservices/pkg/observability"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

// InboxProcessor is the worker's downstream stage for field reports.
//
// Field reports are an inbox, not hazard events: the public contract defines
// no Source value for them, so they are deliberately never converted into
// HazardEvents, published to RabbitMQ, or counted by the audit checksum.
// "processed" therefore means "durably accepted, validated, and observed",
// not "acted upon". The report itself stays in its inbox table for operators.
//
// The only effects are an operator-visible, correlation-friendly log line and
// the field_reports_processed_total counter, so drained reports show up on the
// dashboards and in the evidence artifacts instead of vanishing silently.
//
// If a Source for field reports is ever added to the contract, replace this
// type with one that submits a HazardEvent; the worker only depends on the
// Processor interface.
type InboxProcessor struct {
	logger *slog.Logger
}

// NewInboxProcessor returns an InboxProcessor that logs to logger.
func NewInboxProcessor(logger *slog.Logger) *InboxProcessor {
	if logger == nil {
		logger = slog.Default()
	}
	return &InboxProcessor{logger: logger}
}

// Process records one drained report. It never fails: the report was already
// validated on submission, and a metric or log write must not turn a durably
// accepted report into a "failed" one.
func (p *InboxProcessor) Process(ctx context.Context, report domain.FieldReport) error {
	observability.ObserveFieldReportProcessed(string(report.HazardType), string(report.Severity))
	p.logger.InfoContext(ctx, "field report drained",
		"component", "field-reports",
		"report_id", report.ReportID,
		"hazard_type", report.HazardType,
		"severity", report.Severity,
		"area_name", report.AreaName,
		"attempt", report.Attempts,
	)
	return nil
}
