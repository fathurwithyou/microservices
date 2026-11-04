// Package sink provides a logging HazardSink adapter used until the ingestor
// publishes to the hazard pipeline.
package sink

import (
	"context"
	"log/slog"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
)

// Logging records accepted batches without forwarding them.
type Logging struct {
	logger *slog.Logger
}

// NewLogging builds a logging sink.
func NewLogging(logger *slog.Logger) *Logging {
	return &Logging{logger: logger}
}

// Submit logs the batch and succeeds.
func (s *Logging) Submit(_ context.Context, readings []domain.Reading) error {
	s.logger.Info("accepted readings", "component", "sink", "count", len(readings))
	return nil
}
