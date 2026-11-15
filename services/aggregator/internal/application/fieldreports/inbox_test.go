package fieldreports

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
)

func processedCount(t *testing.T, hazardType, severity string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "field_reports_processed_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["hazard_type"] == hazardType && labels["severity"] == severity {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestInboxProcessorCountsAndLogsWithoutFailing(t *testing.T) {
	var output bytes.Buffer
	processor := NewInboxProcessor(slog.New(slog.NewTextHandler(&output, nil)))
	report := validReport()
	before := processedCount(t, string(report.HazardType), string(report.Severity))

	if err := processor.Process(context.Background(), report); err != nil {
		t.Fatalf("Process() error = %v, want nil", err)
	}

	after := processedCount(t, string(report.HazardType), string(report.Severity))
	if after-before != 1 {
		t.Fatalf("counter delta = %v, want 1", after-before)
	}
	for _, want := range []string{"field report drained", "report_id=" + report.ReportID} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("log %q does not contain %q", output.String(), want)
		}
	}
}

func TestWorkerWithInboxProcessorMarksProcessedAndCountsOnce(t *testing.T) {
	service, repository := testService()
	report := validReport()
	if _, err := service.Submit(context.Background(), report); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	worker, err := NewWorker(WorkerConfig{
		Repository: repository,
		Processor:  NewInboxProcessor(slog.New(slog.NewTextHandler(&output, nil))),
		Now:        func() time.Time { return time.Now().UTC() },
		Logger:     slog.New(slog.NewTextHandler(&output, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	before := processedCount(t, string(report.HazardType), string(report.Severity))

	worker.dispatch(context.Background())
	worker.dispatch(context.Background()) // already processed; must not be drained again

	stored := repository.reports[report.ReportID]
	if stored.Status != domain.FieldReportProcessed || stored.Attempts != 1 {
		t.Fatalf("report = %+v, want processed after exactly one attempt", stored)
	}
	if delta := processedCount(t, string(report.HazardType), string(report.Severity)) - before; delta != 1 {
		t.Fatalf("counter delta = %v, want 1", delta)
	}
}
