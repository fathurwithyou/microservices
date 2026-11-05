// Package audit exposes the read-only Audit API required by M2.
package audit

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

// ErrInvalidCursor is returned for a malformed or undecodable cursor.
var ErrInvalidCursor = errors.New("invalid cursor")

// Filter selects audit events. Since applies to ingested_at.
type Filter struct {
	Source domain.Source
	Since  time.Time
	Limit  int
	Cursor string
}

// Location is one physical copy of a hazard row.
type Location struct {
	Node string
	Unit string
	Role string
}

// Repository is the storage capability required by audit queries.
type Repository interface {
	ListAuditEvents(context.Context, Filter) ([]domain.HazardEvent, error)
	SourceRefIDs(context.Context, domain.Source) ([]string, error)
	HazardExists(context.Context, string) (bool, error)
	HazardLocations(context.Context, string) ([]Location, error)
	UnavailableUnits(context.Context) ([]string, error)
	ListIngestFailures(context.Context, domain.Source, int) ([]domain.IngestFailure, error)
}

// Queries serves Audit API reads.
type Queries struct {
	repository Repository
}

// NewQueries returns audit queries backed by repository.
func NewQueries(repository Repository) *Queries {
	return &Queries{repository: repository}
}

// List returns one page ordered by ingested_at then hazard_id.
func (q *Queries) List(ctx context.Context, filter Filter) ([]domain.HazardEvent, string, error) {
	if filter.Limit <= 0 || filter.Limit > 1000 {
		filter.Limit = 100
	}
	if filter.Cursor != "" {
		cursor, err := decodeCursor(filter.Cursor)
		if err != nil {
			return nil, "", err
		}
		filter.Since = cursor.After
		filter.Cursor = cursor.HazardID
	} else {
		filter.Cursor = ""
	}
	events, err := q.repository.ListAuditEvents(ctx, filter)
	if err != nil {
		return nil, "", fmt.Errorf("list audit events: %w", err)
	}
	next := ""
	if len(events) == filter.Limit {
		last := events[len(events)-1]
		next, err = encodeCursor(cursor{After: last.IngestedAt, HazardID: last.HazardID})
		if err != nil {
			return nil, "", err
		}
	}
	return events, next, nil
}

// Checksum returns the SHA-256 over source_ref_id values sorted byte-wise.
func (q *Queries) Checksum(ctx context.Context, source domain.Source) (int, string, error) {
	ids, err := q.repository.SourceRefIDs(ctx, source)
	if err != nil {
		return 0, "", fmt.Errorf("read source ref ids: %w", err)
	}
	sum := sha256.Sum256([]byte(strings.Join(ids, "\n")))
	return len(ids), hex.EncodeToString(sum[:]), nil
}

// Exists reports whether a hazard_id exists. Location metadata is supplied by
// the transport because it depends on the storage topology.
func (q *Queries) Exists(ctx context.Context, hazardID string) (bool, error) {
	return q.repository.HazardExists(ctx, hazardID)
}

// Locations returns the replicas of the range that holds hazardID.
func (q *Queries) Locations(ctx context.Context, hazardID string) ([]Location, error) {
	return q.repository.HazardLocations(ctx, hazardID)
}

// Unavailable returns the storage units that cannot currently be reached.
func (q *Queries) Unavailable(ctx context.Context) ([]string, error) {
	return q.repository.UnavailableUnits(ctx)
}

// Unprocessed returns parked or retrying payloads for operators.
func (q *Queries) Unprocessed(ctx context.Context, source domain.Source, limit int) ([]domain.IngestFailure, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	return q.repository.ListIngestFailures(ctx, source, limit)
}

type cursor struct {
	After    time.Time `json:"ingested_at"`
	HazardID string    `json:"hazard_id"`
}

func encodeCursor(value cursor) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeCursor(value string) (cursor, error) {
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return cursor{}, ErrInvalidCursor
	}
	var result cursor
	if err := json.Unmarshal(payload, &result); err != nil || result.After.IsZero() {
		return cursor{}, ErrInvalidCursor
	}
	return result, nil
}
