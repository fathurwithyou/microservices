package analytics

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// fakeRepository answers every window with the same snapshot rows.
type fakeRepository struct {
	rows        []Row
	asOf        time.Time
	unavailable []string
}

func (f fakeRepository) Aggregate(context.Context, Filter) (Snapshot, error) {
	return Snapshot{Rows: f.rows, AsOf: f.asOf}, nil
}
func (f fakeRepository) UnavailableUnits(context.Context) ([]string, error) {
	return f.unavailable, nil
}

// seismicRows is one SEISMIC/HIGH cell of count events.
func seismicRows(count int64) []Row {
	return []Row{{Bucket: time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC), Category: "SEISMIC", Severity: "HIGH", AreaName: "Cilacap", Count: count}}
}

func TestGetMarksPartialWhenStorageUnavailable(t *testing.T) {
	queries := NewQueries(fakeRepository{unavailable: []string{"node-c"}})
	result, err := queries.Get(context.Background(), Filter{Bucket: BucketDay})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Partial || len(result.UnavailableUnits) != 1 || result.UnavailableUnits[0] != "node-c" {
		t.Fatalf("result = %+v, want partial with node-c", result)
	}
}

func TestGetDefaultsWindowAndBucket(t *testing.T) {
	var got Filter
	repository := captureRepository{fn: func(filter Filter) { got = filter }}
	queries := NewQueries(repository)
	if _, err := queries.Get(context.Background(), Filter{}); err != nil {
		t.Fatal(err)
	}
	if got.Bucket != BucketDay {
		t.Fatalf("bucket = %q", got.Bucket)
	}
	if got.Until.IsZero() || got.Since.IsZero() || !got.Since.Before(got.Until) {
		t.Fatalf("window = %s..%s", got.Since, got.Until)
	}
}

func TestGetRejectsInvalidWindow(t *testing.T) {
	queries := NewQueries(fakeRepository{})
	_, err := queries.Get(context.Background(), Filter{Since: time.Now(), Until: time.Now().Add(-time.Hour)})
	if err == nil {
		t.Fatal("expected invalid window error")
	}
}

func TestGetRejectsInvalidBucket(t *testing.T) {
	queries := NewQueries(fakeRepository{})
	if _, err := queries.Get(context.Background(), Filter{Bucket: "minute"}); err == nil {
		t.Fatal("expected invalid bucket error")
	}
}

type captureRepository struct{ fn func(Filter) }

func (c captureRepository) Aggregate(_ context.Context, filter Filter) (Snapshot, error) {
	c.fn(filter)
	return Snapshot{}, nil
}
func (c captureRepository) UnavailableUnits(context.Context) ([]string, error) { return nil, nil }

func TestGetFoldsOneSnapshotIntoEveryDashboardGroup(t *testing.T) {
	day := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 11, 17, 23, 40, 12, 0, time.UTC)
	repository := fakeRepository{asOf: asOf, rows: []Row{
		{Bucket: day, Category: "SEISMIC", Severity: "HIGH", AreaName: "Cilacap", Count: 3},
		{Bucket: day, Category: "SEISMIC", Severity: "LOW", AreaName: "Kebumen", Count: 1},
		{Bucket: day, Category: "TSUNAMI", Severity: "HIGH", AreaName: "Cilacap", Count: 2},
		{Bucket: day.Add(24 * time.Hour), Category: "VOLCANIC", Severity: "HIGH", AreaName: "Magelang", Count: 4},
	}}
	result, err := NewQueries(repository).Get(context.Background(), Filter{Bucket: BucketDay})
	if err != nil {
		t.Fatal(err)
	}
	if !result.AsOf.Equal(asOf) {
		t.Fatalf("as_of = %s, want the snapshot's %s", result.AsOf, asOf)
	}
	wantTypes := []TypeCount{{HazardType: "SEISMIC", Count: 4}, {HazardType: "TSUNAMI", Count: 2}, {HazardType: "VOLCANIC", Count: 4}}
	if fmt.Sprint(result.HazardTypes) != fmt.Sprint(wantTypes) {
		t.Fatalf("hazard types = %v, want %v", result.HazardTypes, wantTypes)
	}
	wantSeverities := []SeverityCount{{Severity: "HIGH", Count: 9}, {Severity: "LOW", Count: 1}}
	if fmt.Sprint(result.Severities) != fmt.Sprint(wantSeverities) {
		t.Fatalf("severities = %v, want %v", result.Severities, wantSeverities)
	}
	// Busiest area first, then by name.
	wantAreas := []AreaCount{{AreaName: "Cilacap", Count: 5}, {AreaName: "Magelang", Count: 4}, {AreaName: "Kebumen", Count: 1}}
	if fmt.Sprint(result.Areas) != fmt.Sprint(wantAreas) {
		t.Fatalf("areas = %v, want %v", result.Areas, wantAreas)
	}
	wantBuckets := []TimeBucket{{Start: day, HazardType: "SEISMIC", Count: 4}, {Start: day, HazardType: "TSUNAMI", Count: 2}, {Start: day.Add(24 * time.Hour), HazardType: "VOLCANIC", Count: 4}}
	if fmt.Sprint(result.Buckets) != fmt.Sprint(wantBuckets) {
		t.Fatalf("buckets = %v, want %v", result.Buckets, wantBuckets)
	}
	var total int64
	for _, item := range result.HazardTypes {
		total += item.Count
	}
	if total != 10 {
		t.Fatalf("total = %d, want every row counted once", total)
	}
}
