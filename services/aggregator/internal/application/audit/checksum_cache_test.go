package audit

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type failingChecksumRepository struct {
	fakeRepository
	fail atomic.Bool
}

func (r *failingChecksumRepository) SourceRefIDs(context.Context, domain.Source) ([]string, error) {
	if r.fail.Load() {
		return nil, errors.New("remote wall time is too far ahead")
	}
	return []string{"a", "b"}, nil
}

// The 18 Nov outage: the store refuses every statement for minutes. The
// checksum an auditor was given before is still given, marked degraded since
// the time it was computed, however long ago; the marker goes with the outage.
func TestChecksumServesTheLastAnswerDegradedWhileTheStoreFails(t *testing.T) {
	clock := newTestClock(time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC))
	answeredAt := clock.Now()
	repository := &failingChecksumRepository{}
	queries := NewQueries(repository)
	queries.checksums.now = clock.Now
	count, checksum, stale, err := queries.Checksum(context.Background(), domain.SourceBMKG)
	if err != nil || count != 2 || stale.Degraded() {
		t.Fatalf("first answer: count=%d marker=%+v err=%v", count, stale, err)
	}

	repository.fail.Store(true)
	clock.Advance(24 * time.Hour) // far beyond checksumMaxStale
	for i := 0; i < 3; i++ {
		gotCount, gotChecksum, stale, err := queries.Checksum(context.Background(), domain.SourceBMKG)
		if err != nil {
			t.Fatalf("call %d: the store error reached the caller although a checksum was cached: %v", i, err)
		}
		if gotCount != count || gotChecksum != checksum {
			t.Fatalf("call %d: answer = %d %s, want the last good one", i, gotCount, gotChecksum)
		}
		if !stale.Degraded() || !stale.Since.Equal(answeredAt) {
			t.Fatalf("call %d: marker = %+v, want degraded since %s", i, stale, answeredAt)
		}
		clock.Advance(time.Minute)
	}

	// A source never answered has nothing to fall back on.
	if _, _, _, err := queries.Checksum(context.Background(), domain.SourcePVMBG); err == nil {
		t.Fatal("want the store error for a source without an earlier answer")
	}

	repository.fail.Store(false)
	for wait := time.Now().Add(2 * time.Second); time.Now().Before(wait); time.Sleep(5 * time.Millisecond) {
		clock.Advance(time.Minute)
		if _, _, stale, err := queries.Checksum(context.Background(), domain.SourceBMKG); err == nil && !stale.Degraded() {
			return
		}
	}
	t.Fatal("answers stayed degraded after the store recovered")
}

// testClock is a settable clock safe to advance while a background refresh
// reads it.
type testClock struct{ nanos atomic.Int64 }

func newTestClock(at time.Time) *testClock {
	clock := &testClock{}
	clock.nanos.Store(at.UnixNano())
	return clock
}

func (c *testClock) Now() time.Time          { return time.Unix(0, c.nanos.Load()).UTC() }
func (c *testClock) Advance(d time.Duration) { c.nanos.Add(int64(d)) }
