package ports

import (
	"context"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
)

// HazardSink forwards accepted readings to the hazard pipeline.
type HazardSink interface {
	Submit(ctx context.Context, readings []domain.Reading) error
}
