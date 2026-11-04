package fieldreports

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type fakeRepository struct {
	reports map[string]domain.FieldReport
}

func (f *fakeRepository) SaveFieldReport(_ context.Context, report domain.FieldReport) (bool, error) {
	if _, ok := f.reports[report.ReportID]; ok {
		return false, nil
	}
	f.reports[report.ReportID] = report
	return true, nil
}

func (f *fakeRepository) ListAcceptedFieldReports(_ context.Context, limit int) ([]domain.FieldReport, error) {
	reports := make([]domain.FieldReport, 0)
	for _, report := range f.reports {
		if report.Status == domain.FieldReportAccepted {
			reports = append(reports, report)
		}
	}
	if limit > 0 && len(reports) > limit {
		reports = reports[:limit]
	}
	return reports, nil
}

func (f *fakeRepository) MarkFieldReport(_ context.Context, reportID string, status domain.FieldReportStatus, processedAt time.Time) error {
	report, ok := f.reports[reportID]
	if !ok {
		return nil
	}
	report.Status = status
	report.Attempts++
	report.ProcessedAt = &processedAt
	f.reports[reportID] = report
	return nil
}

func testService() (*Service, *fakeRepository) {
	repository := &fakeRepository{reports: map[string]domain.FieldReport{}}
	service, err := New(Config{
		Repository: repository,
		Now:        func() time.Time { return time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC) },
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		panic(err)
	}
	return service, repository
}

func validReport() domain.FieldReport {
	return domain.FieldReport{
		ReportID:    "report-1",
		HazardType:  domain.HazardWildfire,
		Severity:    domain.SeveritySiaga,
		AreaName:    "Kalimantan Tengah",
		Latitude:    -2.5,
		Longitude:   113.8,
		OccurredAt:  time.Date(2026, 11, 12, 2, 43, 0, 0, time.UTC),
		Description: "asap tebal",
	}
}

func TestSubmitAcceptsValidReport(t *testing.T) {
	service, repository := testService()
	report, err := service.Submit(context.Background(), validReport())
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if report.Status != domain.FieldReportAccepted {
		t.Fatalf("status = %q", report.Status)
	}
	if _, ok := repository.reports["report-1"]; !ok {
		t.Fatal("report was not stored")
	}
}

func TestSubmitIsIdempotentByReportID(t *testing.T) {
	service, repository := testService()
	if _, err := service.Submit(context.Background(), validReport()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(context.Background(), validReport()); err != nil {
		t.Fatal(err)
	}
	if len(repository.reports) != 1 {
		t.Fatalf("stored reports = %d, want 1", len(repository.reports))
	}
}

func TestSubmitRejectsInvalidReports(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*domain.FieldReport)
	}{
		{"missing id", func(r *domain.FieldReport) { r.ReportID = "" }},
		{"unknown hazard", func(r *domain.FieldReport) { r.HazardType = "TSUNAMI" }},
		{"unknown severity", func(r *domain.FieldReport) { r.Severity = "UNKNOWN" }},
		{"missing area", func(r *domain.FieldReport) { r.AreaName = "" }},
		{"latitude", func(r *domain.FieldReport) { r.Latitude = 91 }},
		{"longitude", func(r *domain.FieldReport) { r.Longitude = -181 }},
		{"missing time", func(r *domain.FieldReport) { r.OccurredAt = time.Time{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, _ := testService()
			report := validReport()
			tt.mutate(&report)
			_, err := service.Submit(context.Background(), report)
			if !errors.Is(err, ErrInvalidReport) {
				t.Fatalf("error = %v, want ErrInvalidReport", err)
			}
		})
	}
}

type errorProcessor struct{ err error }

func (p errorProcessor) Process(context.Context, domain.FieldReport) error { return p.err }

func TestWorkerMarksAcceptedReportProcessed(t *testing.T) {
	service, repository := testService()
	if _, err := service.Submit(context.Background(), validReport()); err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(WorkerConfig{Repository: repository, Now: func() time.Time { return time.Now().UTC() }, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	worker.dispatch(context.Background())
	report := repository.reports["report-1"]
	if report.Status != domain.FieldReportProcessed || report.Attempts != 1 {
		t.Fatalf("report = %+v", report)
	}
}

func TestWorkerMarksProcessingFailure(t *testing.T) {
	service, repository := testService()
	if _, err := service.Submit(context.Background(), validReport()); err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(WorkerConfig{Repository: repository, Processor: errorProcessor{err: errors.New("downstream")}, Now: func() time.Time { return time.Now().UTC() }, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	worker.dispatch(context.Background())
	report := repository.reports["report-1"]
	if report.Status != domain.FieldReportFailed || report.Attempts != 1 {
		t.Fatalf("report = %+v", report)
	}
}
