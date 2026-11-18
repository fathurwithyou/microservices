package ports

import (
	"context"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

// Snapshot is the durable state a replica needs to continue the stream.
type Snapshot struct {
	// Offset is the last committed feed offset, 0 for none.
	Offset int64
	// Session identifies the feed session the offset belongs to.
	Session string
	// Watermark is the latest acquisition time seen.
	Watermark time.Time
	// Detections are the detections that can still change a fire.
	Detections []domain.Detection
	// Fires are the active fires as last sent downstream.
	Fires []domain.Fire
	// Counters are the cumulative reportable counts (late, low confidence,
	// closed fires, closed singles, ...).
	Counters map[string]int64
}

// LateDetection records a detection that arrived past the accepted lateness.
type LateDetection struct {
	Key        string
	Offset     int64
	AcquiredAt time.Time
	Watermark  time.Time
}

// Batch is everything one processed batch changes. Commit applies it in one
// transaction, so the offset never moves without the state it produced.
type Batch struct {
	Offset           int64
	Watermark        time.Time
	UpsertDetections []domain.Detection
	DeleteDetections []string
	// Fires carries active, retracted and closed fires by status.
	Fires    []domain.Fire
	Late     []LateDetection
	Counters map[string]int64
}

// State is the durable store of the stream processor.
type State interface {
	// Load returns the committed state.
	Load(ctx context.Context) (Snapshot, error)
	// Commit applies a batch atomically.
	Commit(ctx context.Context, batch Batch) error
	// Reset forgets everything that belongs to the previous feed session
	// (working set, fires, counters) and starts the new one at offset 0.
	Reset(ctx context.Context, session string) error
}
