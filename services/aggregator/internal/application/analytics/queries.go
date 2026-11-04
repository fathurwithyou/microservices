// Package analytics aggregates hazard events for the M2 dashboard.
package analytics

import (
	"context"
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

// Bucket is the supported time-bucket granularity.
type Bucket string

const (
	BucketHour Bucket = "hour"
	BucketDay  Bucket = "day"
	BucketWeek Bucket = "week"
)

// Filter selects the aggregation window and bucket.
type Filter struct {
	Since  time.Time
	Until  time.Time
	Bucket Bucket
}

// TypeCount is a count per hazard type.
type TypeCount struct {
	HazardType domain.HazardType
	Count      int64
}

// SeverityCount is a count per severity.
type SeverityCount struct {
	Severity domain.Severity
	Count    int64
}

// AreaCount is a count per area.
type AreaCount struct {
	AreaName string
	Count    int64
}

// TimeBucket is a count per time bucket and hazard type.
type TimeBucket struct {
	Start      time.Time
	HazardType domain.HazardType
	Count      int64
}

// Result is the dashboard aggregate.
type Result struct {
	HazardTypes []TypeCount
	Severities  []SeverityCount
	Areas       []AreaCount
	Buckets     []TimeBucket
}

// Repository supplies aggregate queries.
type Repository interface {
	CountByHazardType(context.Context, Filter) ([]TypeCount, error)
	CountBySeverity(context.Context, Filter) ([]SeverityCount, error)
	CountByArea(context.Context, Filter) ([]AreaCount, error)
	CountByTimeBucket(context.Context, Filter) ([]TimeBucket, error)
}

// Queries serves analytics reads.
type Queries struct{ repository Repository }

// NewQueries returns analytics queries.
func NewQueries(repository Repository) *Queries { return &Queries{repository: repository} }

// Get validates filter and returns all dashboard aggregates.
func (q *Queries) Get(ctx context.Context, filter Filter) (Result, error) {
	if filter.Bucket == "" {
		filter.Bucket = BucketDay
	}
	switch filter.Bucket {
	case BucketHour, BucketDay, BucketWeek:
	default:
		return Result{}, fmt.Errorf("invalid bucket %q", filter.Bucket)
	}
	if filter.Until.IsZero() {
		filter.Until = time.Now().UTC()
	}
	if filter.Since.IsZero() {
		filter.Since = filter.Until.Add(-7 * 24 * time.Hour)
	}
	if !filter.Since.Before(filter.Until) {
		return Result{}, fmt.Errorf("since must be before until")
	}
	hazardTypes, err := q.repository.CountByHazardType(ctx, filter)
	if err != nil {
		return Result{}, fmt.Errorf("count hazard types: %w", err)
	}
	severities, err := q.repository.CountBySeverity(ctx, filter)
	if err != nil {
		return Result{}, fmt.Errorf("count severities: %w", err)
	}
	areas, err := q.repository.CountByArea(ctx, filter)
	if err != nil {
		return Result{}, fmt.Errorf("count areas: %w", err)
	}
	buckets, err := q.repository.CountByTimeBucket(ctx, filter)
	if err != nil {
		return Result{}, fmt.Errorf("count time buckets: %w", err)
	}
	return Result{HazardTypes: hazardTypes, Severities: severities, Areas: areas, Buckets: buckets}, nil
}
