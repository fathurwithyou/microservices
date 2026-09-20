package seed

import (
	"testing"
	"time"
)

func TestInitialReportsHonorsCount(t *testing.T) {
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	reports := InitialReports(now, 3)
	if len(reports) != 3 {
		t.Fatalf("reports = %d, want 3", len(reports))
	}
	if reports[0]["report_id"] != "pvmbg-report-1" {
		t.Fatalf("first report ID = %v, want pvmbg-report-1", reports[0]["report_id"])
	}
}

func TestInitialReportsEmptyOnNonPositiveCount(t *testing.T) {
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	for _, count := range []int{0, -3} {
		if reports := InitialReports(now, count); len(reports) != 0 {
			t.Fatalf("count %d seeded %d reports, want none", count, len(reports))
		}
	}
}
