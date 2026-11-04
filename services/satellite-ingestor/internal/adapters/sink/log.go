// Package sink provides a logging HazardSink used until the aggregator gRPC
// contract is available.
package sink

import (
	"context"
	"log/slog"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

// Log logs submitted fires and discards them.
type Log struct {
	logger *slog.Logger
}

// NewLog returns a HazardSink that records fires as structured log entries.
func NewLog(logger *slog.Logger) *Log {
	return &Log{logger: logger}
}

func (s *Log) Submit(_ context.Context, fires []domain.Fire) error {
	for _, fire := range fires {
		s.logger.Info("fire submitted",
			"service", "satellite-ingestor",
			"component", "sink",
			"source_ref_id", fire.SourceRefID,
			"severity", fire.Severity,
			"detection_count", fire.DetectionCount,
			"total_frp", fire.TotalFRP,
			"occurred_at", fire.OccurredAt,
		)
	}
	return nil
}
