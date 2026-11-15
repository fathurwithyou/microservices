package hazards

import (
	"encoding/base64"
	"errors"
	"strings"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

// DefaultPageSize and MaxPageSize bound one ListHazards page.
const (
	DefaultPageSize = 100
	MaxPageSize     = 100
)

// ErrInvalidPageToken reports a page token this service did not issue.
var ErrInvalidPageToken = errors.New("invalid page token")

// Cursor is the position after the last item of a page. Listings are ordered
// by occurred_at descending with hazard_id descending as the tiebreaker, so
// the pair is a total order and keyset pagination never skips or repeats a
// row, whichever store range holds it.
type Cursor struct {
	OccurredAt time.Time
	HazardID   string
}

const pageTokenVersion = "v1"

// EncodePageToken returns an opaque token for the position after event.
func EncodePageToken(event domain.HazardEvent) string {
	raw := pageTokenVersion + "|" + event.OccurredAt.UTC().Format(time.RFC3339Nano) + "|" + event.HazardID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodePageToken parses a token from EncodePageToken. An empty token means
// the first page and returns ok=false.
func DecodePageToken(token string) (cursor Cursor, ok bool, err error) {
	if token == "" {
		return Cursor{}, false, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return Cursor{}, false, ErrInvalidPageToken
	}
	parts := strings.SplitN(string(raw), "|", 3)
	if len(parts) != 3 || parts[0] != pageTokenVersion || parts[2] == "" {
		return Cursor{}, false, ErrInvalidPageToken
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, parts[1])
	if err != nil {
		return Cursor{}, false, ErrInvalidPageToken
	}
	return Cursor{OccurredAt: occurredAt, HazardID: parts[2]}, true, nil
}

// Before reports whether event sorts after the cursor position, i.e. belongs
// on a later page.
func (c Cursor) Before(event domain.HazardEvent) bool {
	if !event.OccurredAt.Equal(c.OccurredAt) {
		return event.OccurredAt.Before(c.OccurredAt)
	}
	return event.HazardID < c.HazardID
}

// NormalizePageSize applies the default and the maximum page size.
func NormalizePageSize(size int) int {
	if size <= 0 || size > MaxPageSize {
		return DefaultPageSize
	}
	return size
}
