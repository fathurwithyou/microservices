package application

import (
	"slices"
	"sort"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

// reconcile maps newly clustered fires onto the previous snapshot. A fire that
// overlaps an old one keeps its source_ref_id so updates reach the same
// HazardEvent. Old fires that disappeared are returned as retracted tombstones.
func reconcile(oldFires, freshFires []domain.Fire) (current, changes []domain.Fire) {
	current = append([]domain.Fire(nil), freshFires...)
	used := make(map[string]bool, len(oldFires))
	for index := range current {
		best, bestOverlap := -1, 0
		for candidate, old := range oldFires {
			if old.Status == domain.StatusRetracted || used[old.SourceRefID] {
				continue
			}
			if overlap := detectionOverlap(current[index].DetectionKeys, old.DetectionKeys); overlap > bestOverlap {
				best, bestOverlap = candidate, overlap
			}
		}
		if best >= 0 {
			current[index].SourceRefID = oldFires[best].SourceRefID
			used[oldFires[best].SourceRefID] = true
		}
	}
	sort.Slice(current, func(i, j int) bool { return current[i].SourceRefID < current[j].SourceRefID })

	oldByID := make(map[string]domain.Fire, len(oldFires))
	for _, fire := range oldFires {
		oldByID[fire.SourceRefID] = fire
	}
	newByID := make(map[string]bool, len(current))
	for _, fire := range current {
		newByID[fire.SourceRefID] = true
		if previous, ok := oldByID[fire.SourceRefID]; !ok || !sameFire(previous, fire) {
			changes = append(changes, fire)
		}
	}
	for _, fire := range oldFires {
		if fire.Status == domain.StatusRetracted || newByID[fire.SourceRefID] {
			continue
		}
		retracted := fire
		retracted.Status = domain.StatusRetracted
		retracted.DetectionCount = 0
		retracted.DetectionKeys = nil
		retracted.Satellites = nil
		retracted.TotalFRP = 0
		changes = append(changes, retracted)
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].SourceRefID < changes[j].SourceRefID })
	return current, changes
}

func detectionOverlap(left, right []string) int {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	own := make(map[string]struct{}, len(left))
	for _, key := range left {
		own[key] = struct{}{}
	}
	overlap := 0
	for _, key := range right {
		if _, ok := own[key]; ok {
			overlap++
		}
	}
	return overlap
}

func sameFire(left, right domain.Fire) bool {
	return left.Status == right.Status &&
		left.Severity == right.Severity &&
		left.AreaName == right.AreaName &&
		left.DetectionCount == right.DetectionCount &&
		left.TotalFRP == right.TotalFRP &&
		left.MaxConfidence == right.MaxConfidence &&
		left.Latitude == right.Latitude &&
		left.Longitude == right.Longitude &&
		left.OccurredAt.Equal(right.OccurredAt) &&
		slices.Equal(left.DetectionKeys, right.DetectionKeys) &&
		slices.Equal(left.Satellites, right.Satellites)
}
