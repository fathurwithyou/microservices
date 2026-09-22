package state

import (
	"testing"
	"time"
)

func TestAppendReportUsesUniqueRuntimeID(t *testing.T) {
	state := New(0, 0, 0)
	state.AppendReport()
	state.AppendReport()

	reports := state.ReportsSince(time.Time{})
	if len(reports) != 2 {
		t.Fatalf("report count = %d, want 2", len(reports))
	}
	firstID := reports[0]["report_id"]
	if firstID == reports[1]["report_id"] {
		t.Fatalf("runtime report IDs must be unique: %v", firstID)
	}
}
