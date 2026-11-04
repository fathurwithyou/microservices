// Package fieldreports owns the durable inbox for Tim Lapangan submissions.
package fieldreports

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

// ErrInvalidReport is returned when a submission cannot be accepted.
var ErrInvalidReport = errors.New("invalid field report")

// Repository persists field reports idempotently by ReportID.
type Repository interface {
	SaveFieldReport(context.Context, domain.FieldReport) (bool, error)
	ListAcceptedFieldReports(context.Context, int) ([]domain.FieldReport, error)
	MarkFieldReport(context.Context, string, domain.FieldReportStatus, time.Time) error
}

// Service validates and durably accepts field reports.
type Service struct {
	repository Repository
	now        func() time.Time
	logger     *slog.Logger
}

// Config configures a Service.
type Config struct {
	Repository Repository
	Now        func() time.Time
	Logger     *slog.Logger
}

// New returns a field-report service.
func New(cfg Config) (*Service, error) {
	if cfg.Repository == nil {
		return nil, errors.New("fieldreports: repository is required")
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Service{repository: cfg.Repository, now: cfg.Now, logger: cfg.Logger}, nil
}

// Submit validates report, stamps it, and writes it to the durable inbox. A
// repeated ReportID is accepted without creating a second record.
func (s *Service) Submit(ctx context.Context, report domain.FieldReport) (domain.FieldReport, error) {
	report.ReportID = strings.TrimSpace(report.ReportID)
	if err := validate(report); err != nil {
		return domain.FieldReport{}, err
	}
	report.ReceivedAt = s.now()
	report.Status = domain.FieldReportAccepted
	if _, err := s.repository.SaveFieldReport(ctx, report); err != nil {
		return domain.FieldReport{}, fmt.Errorf("save field report: %w", err)
	}
	s.logger.Info("field report accepted", "component", "field-reports", "report_id", report.ReportID, "hazard_type", report.HazardType, "severity", report.Severity)
	return report, nil
}

func validate(report domain.FieldReport) error {
	if report.ReportID == "" {
		return fmt.Errorf("%w: report_id is required", ErrInvalidReport)
	}
	switch report.HazardType {
	case domain.HazardSeismic, domain.HazardVolcanic, domain.HazardWildfire, domain.HazardCoastal:
	default:
		return fmt.Errorf("%w: unknown hazard_type %q", ErrInvalidReport, report.HazardType)
	}
	switch report.Severity {
	case domain.SeverityNormal, domain.SeverityWaspada, domain.SeveritySiaga, domain.SeverityAwas:
	default:
		return fmt.Errorf("%w: unknown severity %q", ErrInvalidReport, report.Severity)
	}
	if strings.TrimSpace(report.AreaName) == "" {
		return fmt.Errorf("%w: area_name is required", ErrInvalidReport)
	}
	if report.Latitude < -90 || report.Latitude > 90 {
		return fmt.Errorf("%w: latitude out of range", ErrInvalidReport)
	}
	if report.Longitude < -180 || report.Longitude > 180 {
		return fmt.Errorf("%w: longitude out of range", ErrInvalidReport)
	}
	if report.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred_at is required", ErrInvalidReport)
	}
	return nil
}
