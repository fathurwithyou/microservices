package ports

import (
	"context"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

// State is the durable store for the feed cursor, raw detections, and fire
// mappings. Implementations must make each save durable before returning.
type State interface {
	// Offset returns the last committed feed offset, or 0 for none.
	Offset(ctx context.Context) (int64, error)
	// SaveOffset commits the feed cursor.
	SaveOffset(ctx context.Context, offset int64) error
	// Detections returns the retained raw detections.
	Detections(ctx context.Context) ([]domain.Detection, error)
	// SaveDetections replaces the retained raw detections.
	SaveDetections(ctx context.Context, detections []domain.Detection) error
	// Fires returns the current fire mappings.
	Fires(ctx context.Context) ([]domain.Fire, error)
	// SaveFires replaces the current fire mappings.
	SaveFires(ctx context.Context, fires []domain.Fire) error
}
