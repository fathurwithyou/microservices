// Package analytics aggregates hazard events for the M2 dashboard.
package analytics

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/degraded"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

// ErrInvalidFilter marks a window or bucket the caller got wrong, as opposed
// to a store failure, so the transport can answer 400 instead of 500.
var ErrInvalidFilter = errors.New("invalid analytics filter")

// warmInterval is how often KeepWarm refreshes its window: half of
// resultMaxStale, so a request that finds the entry older than ttl is still
// within the stale budget and is answered at once while the refresh runs.
const warmInterval = resultMaxStale / 2

// Bucket is the supported time-bucket granularity.
type Bucket string

const (
	BucketHour Bucket = "hour"
	BucketDay  Bucket = "day"
	BucketWeek Bucket = "week"
)

// CategoryTsunami is a dashboard-only category. The canonical HazardEvent
// stays SEISMIC (M1 mandatory mapping); the dashboard splits out earthquakes
// that carry tsunami potential or a correlated TsunamiWarning so the
// per-hazard view shows gempa and tsunami separately. Categories are
// exclusive, so their sum equals the number of HazardEvents in the window.
const CategoryTsunami domain.HazardType = "TSUNAMI"

// CategorySQL is the SQL form of Category over the hazard_events table.
const CategorySQL = `CASE WHEN hazard_type = 'SEISMIC' AND (attributes->>'potential_tsunami' = 'true' OR COALESCE(attributes->>'warning_id', '') <> '') THEN 'TSUNAMI' ELSE hazard_type END`

// Category returns the dashboard category of one event.
func Category(event domain.HazardEvent) domain.HazardType {
	if event.HazardType != domain.HazardSeismic {
		return event.HazardType
	}
	if potential, _ := event.Attributes["potential_tsunami"].(bool); potential {
		return CategoryTsunami
	}
	if warningID, _ := event.Attributes["warning_id"].(string); warningID != "" {
		return CategoryTsunami
	}
	return event.HazardType
}

// Filter selects the aggregation window and bucket.
type Filter struct {
	Since  time.Time
	Until  time.Time
	Bucket Bucket
}

// TypeCount is a count per dashboard category (see Category).
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

// Result is the dashboard aggregate. AsOf is the store snapshot the counts
// were read from: the store reads a few seconds behind now (see the postgres
// adapter) and the cache may add its ttl, so the answer says how old it is
// instead of pretending to be live.
//
// Degraded is set when the store did not answer: the counts are then the last
// ones this window was answered with, Partial is true and UnavailableUnits
// is never empty.
type Result struct {
	HazardTypes      []TypeCount
	Severities       []SeverityCount
	Areas            []AreaCount
	Buckets          []TimeBucket
	AsOf             time.Time
	Partial          bool
	UnavailableUnits []string
	Degraded         degraded.Info
}

// Row is one cell of a window scan: the events of one time bucket, dashboard
// category, severity and area.
type Row struct {
	Bucket   time.Time
	Category domain.HazardType
	Severity domain.Severity
	AreaName string
	Count    int64
}

// Snapshot is one scan of a window, read at AsOf. Every dashboard group is
// folded from the same rows, so the four views always add up to the same
// total; four separate scans could each see a different set of events.
type Snapshot struct {
	Rows []Row
	AsOf time.Time
}

// Repository supplies the aggregate scan.
type Repository interface {
	// Aggregate counts the events of one validated window per Row key in a
	// single scan of a single snapshot.
	Aggregate(context.Context, Filter) (Snapshot, error)
	UnavailableUnits(context.Context) ([]string, error)
}

// Queries serves analytics reads.
type Queries struct {
	repository Repository
	cache      *resultCache
}

// NewQueries returns analytics queries.
func NewQueries(repository Repository) *Queries { return &Queries{repository: repository} }

// WithCache makes identical concurrent windows share one set of store queries
// and reuses their counts for ttl (see resultCache). A ttl <= 0 disables it.
func (q *Queries) WithCache(ttl time.Duration) *Queries {
	if ttl > 0 {
		q.cache = newResultCache(ttl)
	}
	return q
}

// Get validates filter and returns all dashboard aggregates.
func (q *Queries) Get(ctx context.Context, filter Filter) (Result, error) {
	key, resolved, err := q.resolve(filter)
	if err != nil {
		return Result{}, err
	}
	load := func(ctx context.Context) (aggregates, error) { return q.aggregate(ctx, resolved) }
	var (
		counts aggregates
		stale  degraded.Info
	)
	if q.cache != nil {
		counts, stale, err = q.cache.get(ctx, key, load)
	} else {
		counts, err = load(ctx)
	}
	if err != nil {
		return Result{}, err
	}
	// Completeness is read every time, never from the cache: a node that just
	// went away must show up as partial even while counts are reused. A
	// failed lookup makes the answer partial, not an error: the counts are
	// there, only their completeness is unknown.
	partial, unavailable, stale := degraded.Completeness(ctx, q.repository.UnavailableUnits, stale)
	return Result{
		HazardTypes: counts.HazardTypes, Severities: counts.Severities, Areas: counts.Areas, Buckets: counts.Buckets,
		AsOf: counts.AsOf, Partial: partial, UnavailableUnits: unavailable, Degraded: stale,
	}, nil
}

// KeepWarm refreshes the aggregates of one window every warmInterval until
// ctx ends, so that window is answered from the cache even by the first
// request after a restart or an idle minute. Without it a cold request runs
// the four scans inside the caller's deadline; on the 500k-event testbed that
// took 2-4 s and the dashboard's default window answered 502 after every idle
// period (18 Nov). The usual caller is main with Filter{}: the window the
// dashboard opens with. A failed refresh is retried at the next tick.
func (q *Queries) KeepWarm(ctx context.Context, filter Filter) {
	if q.cache == nil {
		return
	}
	ticker := time.NewTicker(warmInterval)
	defer ticker.Stop()
	for {
		if key, resolved, err := q.resolve(filter); err == nil {
			done := q.cache.refresh(ctx, key, func(ctx context.Context) (aggregates, error) { return q.aggregate(ctx, resolved) })
			select {
			case <-ctx.Done():
				return
			case <-done:
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// resolve validates filter and fills in the defaults (bucket day, the last
// seven days up to now). The cache key is taken after the bucket default but
// before the time defaults: "no bucket" and "day" are the same window and
// must share one entry (the Client API always sends bucket=day, KeepWarm
// sends none; with separate keys the warmed entry was never the one asked
// for), while a request without until means "up to now", and stamping now
// into the key would make every such request a miss.
func (q *Queries) resolve(filter Filter) (string, Filter, error) {
	if filter.Bucket == "" {
		filter.Bucket = BucketDay
	}
	key := resultKey(filter)
	switch filter.Bucket {
	case BucketHour, BucketDay, BucketWeek:
	default:
		return "", Filter{}, fmt.Errorf("%w: invalid bucket %q", ErrInvalidFilter, filter.Bucket)
	}
	if filter.Until.IsZero() {
		filter.Until = time.Now().UTC()
	}
	if filter.Since.IsZero() {
		filter.Since = filter.Until.Add(-7 * 24 * time.Hour)
	}
	if !filter.Since.Before(filter.Until) {
		return "", Filter{}, fmt.Errorf("%w: since must be before until", ErrInvalidFilter)
	}
	return key, filter, nil
}

// aggregate reads one validated window from the store and folds it into the
// dashboard groups. Until 18 Nov this ran four separate count queries, one
// per group, each a scan of the window: 2-4 s on the 500k-event testbed under
// steady ingest, longer than the 2 s per-attempt budget of the client-api
// retry, so a cache miss could never be answered. One scan costs a quarter of
// that and the groups agree with each other by construction.
func (q *Queries) aggregate(ctx context.Context, filter Filter) (aggregates, error) {
	snapshot, err := q.repository.Aggregate(ctx, filter)
	if err != nil {
		return aggregates{}, fmt.Errorf("aggregate window: %w", err)
	}
	return fold(snapshot), nil
}

// fold sums the snapshot rows into the four dashboard groups, each in the
// order the dashboard shows it: categories and severities by name, areas by
// count (busiest first) then name, buckets by start then category.
func fold(snapshot Snapshot) aggregates {
	type bucketKey struct {
		start    time.Time
		category domain.HazardType
	}
	types := map[domain.HazardType]int64{}
	severities := map[domain.Severity]int64{}
	areas := map[string]int64{}
	buckets := map[bucketKey]int64{}
	for _, row := range snapshot.Rows {
		types[row.Category] += row.Count
		severities[row.Severity] += row.Count
		areas[row.AreaName] += row.Count
		buckets[bucketKey{start: row.Bucket, category: row.Category}] += row.Count
	}
	result := aggregates{AsOf: snapshot.AsOf, HazardTypes: make([]TypeCount, 0, len(types)), Severities: make([]SeverityCount, 0, len(severities)), Areas: make([]AreaCount, 0, len(areas)), Buckets: make([]TimeBucket, 0, len(buckets))}
	for category, count := range types {
		result.HazardTypes = append(result.HazardTypes, TypeCount{HazardType: category, Count: count})
	}
	sort.Slice(result.HazardTypes, func(i, j int) bool { return result.HazardTypes[i].HazardType < result.HazardTypes[j].HazardType })
	for severity, count := range severities {
		result.Severities = append(result.Severities, SeverityCount{Severity: severity, Count: count})
	}
	sort.Slice(result.Severities, func(i, j int) bool { return result.Severities[i].Severity < result.Severities[j].Severity })
	for area, count := range areas {
		result.Areas = append(result.Areas, AreaCount{AreaName: area, Count: count})
	}
	sort.Slice(result.Areas, func(i, j int) bool {
		if result.Areas[i].Count != result.Areas[j].Count {
			return result.Areas[i].Count > result.Areas[j].Count
		}
		return result.Areas[i].AreaName < result.Areas[j].AreaName
	})
	for key, count := range buckets {
		result.Buckets = append(result.Buckets, TimeBucket{Start: key.start, HazardType: key.category, Count: count})
	}
	sort.Slice(result.Buckets, func(i, j int) bool {
		if !result.Buckets[i].Start.Equal(result.Buckets[j].Start) {
			return result.Buckets[i].Start.Before(result.Buckets[j].Start)
		}
		return result.Buckets[i].HazardType < result.Buckets[j].HazardType
	})
	return result
}
