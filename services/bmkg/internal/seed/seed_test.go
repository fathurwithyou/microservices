package seed

import (
	"testing"
	"time"
)

func TestInitialDataHonorsCount(t *testing.T) {
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	events, warnings := InitialData(now, 5)
	if len(events) != 5 {
		t.Fatalf("events = %d, want 5", len(events))
	}
	if len(warnings) != 2 {
		t.Fatalf("warnings = %d, want 2", len(warnings))
	}
	if events[0].EventID != "bmkg-seismic-1" {
		t.Fatalf("first event ID = %q, want bmkg-seismic-1", events[0].EventID)
	}
}

func TestInitialDataEmptyOnNonPositiveCount(t *testing.T) {
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	for _, count := range []int{0, -3} {
		events, warnings := InitialData(now, count)
		if len(events) != 0 || len(warnings) != 0 {
			t.Fatalf("count %d seeded %d events and %d warnings, want none", count, len(events), len(warnings))
		}
	}
}
