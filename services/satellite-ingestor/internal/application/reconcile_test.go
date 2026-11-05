package application

import (
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

func fire(id string, keys ...string) domain.Fire {
	return domain.Fire{
		SourceRefID:    id,
		Status:         domain.StatusActive,
		OccurredAt:     time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC),
		AreaName:       "Kalimantan Tengah",
		Latitude:       -2.5,
		Longitude:      113.8,
		DetectionCount: len(keys),
		DetectionKeys:  keys,
		Satellites:     []domain.Satellite{"N20"},
		MaxConfidence:  domain.ConfidenceHigh,
		Severity:       domain.SeveritySiaga,
	}
}

func TestReconcileKeepsIdentifierWhenFireGrows(t *testing.T) {
	old := fire("fire-old", "a", "b")
	fresh := fire("fire-new", "a", "b", "c")

	current, changes := reconcile([]domain.Fire{old}, []domain.Fire{fresh})
	if len(current) != 1 || current[0].SourceRefID != "fire-old" {
		t.Fatalf("current = %+v, want reused id", current)
	}
	if len(changes) != 1 || changes[0].SourceRefID != "fire-old" || changes[0].DetectionCount != 3 {
		t.Fatalf("changes = %+v, want one update", changes)
	}
}

func TestReconcileRetractsDisappearedFire(t *testing.T) {
	old := fire("fire-old", "a", "b")
	_, changes := reconcile([]domain.Fire{old}, nil)
	if len(changes) != 1 {
		t.Fatalf("changes = %+v, want one tombstone", changes)
	}
	got := changes[0]
	if got.Status != domain.StatusRetracted || got.DetectionCount != 0 || len(got.DetectionKeys) != 0 {
		t.Fatalf("tombstone = %+v", got)
	}
}

func TestReconcileMergesAndRetractsOtherFire(t *testing.T) {
	first := fire("fire-a", "a", "b")
	second := fire("fire-b", "c", "d")
	merged := fire("fire-new", "a", "b", "c", "d")

	current, changes := reconcile([]domain.Fire{first, second}, []domain.Fire{merged})
	if len(current) != 1 {
		t.Fatalf("current = %+v", current)
	}
	reused := current[0].SourceRefID
	if reused != "fire-a" && reused != "fire-b" {
		t.Fatalf("reused id = %q", reused)
	}
	var updates, tombstones int
	for _, change := range changes {
		if change.Status == domain.StatusRetracted {
			tombstones++
		} else {
			updates++
		}
	}
	if updates != 1 || tombstones != 1 {
		t.Fatalf("changes = %+v, want one update and one tombstone", changes)
	}
}

func TestReconcileNoChangeProducesNoWrites(t *testing.T) {
	old := fire("fire-old", "a", "b")
	_, changes := reconcile([]domain.Fire{old}, []domain.Fire{old})
	if len(changes) != 0 {
		t.Fatalf("changes = %+v, want none", changes)
	}
}
