package state

import (
	"testing"
	"time"
)

func TestAppendEventUsesUniqueRuntimeID(t *testing.T) {
	state := New(Config{SeedCount: 0})
	state.AppendEvent()
	state.AppendEvent()

	events := state.EventsSince(time.Time{})
	if len(events) != 2 {
		t.Fatalf("event count = %d, want 2", len(events))
	}
	if events[0].EventID == events[1].EventID {
		t.Fatalf("runtime event IDs must be unique: %q", events[0].EventID)
	}
}

func TestNewUsesInjectedClock(t *testing.T) {
	fixed := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	state := New(Config{SeedCount: 1, Now: func() time.Time { return fixed }})
	state.AppendEvent()

	events := state.EventsSince(time.Time{})
	if len(events) != 2 {
		t.Fatalf("event count = %d, want 2 (1 seed + 1 runtime)", len(events))
	}
	// The seed event occurred one minute before the injected now; the
	// runtime event is stamped exactly with it.
	if events[0].OccurredAt != fixed.Add(-time.Minute).Format(time.RFC3339) {
		t.Errorf("seed OccurredAt = %q, want %q", events[0].OccurredAt, fixed.Add(-time.Minute).Format(time.RFC3339))
	}
	if events[1].OccurredAt != fixed.Format(time.RFC3339) {
		t.Errorf("runtime OccurredAt = %q, want %q", events[1].OccurredAt, fixed.Format(time.RFC3339))
	}
}

func TestAppendEventKeepsRuntimeValues(t *testing.T) {
	fixed := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	state := New(Config{Now: func() time.Time { return fixed }})
	state.AppendEvent()

	events := state.EventsSince(time.Time{})
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
	event := events[0]
	if event.Magnitude != 5.1 || event.DepthKM != 12 || event.EpicenterLat != -7.1 || event.EpicenterLon != 107.6 {
		t.Fatalf("runtime event values changed: %+v", event)
	}
	if event.RegionName != "Jawa Barat" || !event.PotentialTsunami {
		t.Fatalf("runtime event values changed: %+v", event)
	}
}
