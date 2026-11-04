package analytics

import (
	"context"
	"testing"
	"time"
)

type fakeRepository struct {
	types      []TypeCount
	severities []SeverityCount
	areas      []AreaCount
	buckets    []TimeBucket
}

func (f fakeRepository) CountByHazardType(context.Context, Filter) ([]TypeCount, error) {
	return f.types, nil
}
func (f fakeRepository) CountBySeverity(context.Context, Filter) ([]SeverityCount, error) {
	return f.severities, nil
}
func (f fakeRepository) CountByArea(context.Context, Filter) ([]AreaCount, error) {
	return f.areas, nil
}
func (f fakeRepository) CountByTimeBucket(context.Context, Filter) ([]TimeBucket, error) {
	return f.buckets, nil
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

func (c captureRepository) CountByHazardType(_ context.Context, filter Filter) ([]TypeCount, error) {
	c.fn(filter)
	return nil, nil
}
func (c captureRepository) CountBySeverity(context.Context, Filter) ([]SeverityCount, error) {
	return nil, nil
}
func (c captureRepository) CountByArea(context.Context, Filter) ([]AreaCount, error) {
	return nil, nil
}
func (c captureRepository) CountByTimeBucket(context.Context, Filter) ([]TimeBucket, error) {
	return nil, nil
}
