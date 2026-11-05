package ports

import (
	"context"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

// EventKind is the feed event discriminator.
type EventKind string

const (
	KindDetection EventKind = "detection"
	KindUpdate    EventKind = "update"
	KindRetract   EventKind = "retract"
	KindHeartbeat EventKind = "heartbeat"
)

// Event is one decoded feed event. Offset is the feed sequence number used as
// the reconnect cursor. Detection is set for detection and update events;
// DetectionKey is set for retract events.
type Event struct {
	Offset       int64
	Kind         EventKind
	Detection    domain.Detection
	DetectionKey string
}

// EventHandler processes one event. It returns an error when the event could
// not be committed, which stops the stream so the caller can reconnect from
// the last checkpointed offset.
type EventHandler func(ctx context.Context, event Event) error

// Session is the feed's current state, used to measure how far behind the
// ingestor is.
type Session struct {
	Group        string `json:"group"`
	Profile      string `json:"profile"`
	Speed        int    `json:"speed"`
	VirtualNow   string `json:"virtual_now"`
	LatestOffset int64  `json:"latest_offset"`
}

// Feed is the inbound hotspot event stream.
type Feed interface {
	// Stream consumes events starting at offset (0 means the beginning or
	// the feed's default) and calls handler for each one. It returns when the
	// connection ends or handler fails; reconnection belongs to the caller.
	Stream(ctx context.Context, offset int64, handler EventHandler) error
	// Session reports the latest feed offset so lag can be exported.
	Session(ctx context.Context) (Session, error)
}
