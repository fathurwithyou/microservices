package ports

import (
	"context"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

// HazardSink publishes clustered fires downstream.
type HazardSink interface {
	Submit(ctx context.Context, fires []domain.Fire) error
}
