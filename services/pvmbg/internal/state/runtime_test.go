package state

import (
	"testing"
	"time"
)

func mustNew(t *testing.T, cfg Config) *Runtime {
	t.Helper()
	runtime, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return runtime
}

func TestAppendReportUsesUniqueRuntimeID(t *testing.T) {
	runtime := mustNew(t, Config{SeedCount: 0})
	runtime.AppendReport()
	runtime.AppendReport()

	reports := runtime.ReportsSince(time.Time{})
	if len(reports) != 2 {
		t.Fatalf("report count = %d, want 2", len(reports))
	}
	if reports[0].ReportID == reports[1].ReportID {
		t.Fatalf("runtime report IDs must be unique: %v", reports[0].ReportID)
	}
}

func TestReportsSinceFiltersByReportedAt(t *testing.T) {
	fixed := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	runtime := mustNew(t, Config{SeedCount: 0, Now: func() time.Time { return fixed }})
	runtime.AppendReport()

	if got := runtime.ReportsSince(fixed.Add(time.Minute)); len(got) != 0 {
		t.Fatalf("future cursor returned %d reports, want none", len(got))
	}
	if got := runtime.ReportsSince(fixed); len(got) != 1 {
		t.Fatalf("exact cursor returned %d reports, want 1", len(got))
	}
}

func TestReportsSinceInjectsConfidenceLevelOnlyInSchemaV2(t *testing.T) {
	fixed := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	runtime := mustNew(t, Config{SeedCount: 1, Now: func() time.Time { return fixed }})

	plain := runtime.ReportsSince(time.Time{})
	if len(plain) != 1 {
		t.Fatalf("seeded reports = %d, want 1", len(plain))
	}
	if plain[0].ConfidenceLevel != nil {
		t.Fatalf("confidence leaked without schemaV2: %+v", plain[0])
	}

	runtime.SetSchemaV2(true)
	versioned := runtime.ReportsSince(time.Time{})
	if versioned[0].ConfidenceLevel == nil || *versioned[0].ConfidenceLevel != 0.82 {
		t.Fatalf("schemaV2 confidence = %+v, want 0.82", versioned[0].ConfidenceLevel)
	}
}

func TestNewRejectsNegativeSeedCount(t *testing.T) {
	if _, err := New(Config{SeedCount: -1}); err == nil {
		t.Fatal("negative seed count must fail")
	}
}
