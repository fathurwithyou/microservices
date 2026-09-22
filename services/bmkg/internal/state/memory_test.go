package state

import (
	"testing"
	"time"
)

func TestAppendEventUsesUniqueRuntimeID(t *testing.T) {
	state := New(0)
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
