// Package seed generates the deterministic historical dataset the BMKG
// simulation starts from.
package seed

import (
	"strconv"
	"time"

	"github.com/fathurwithyou/microservices/services/bmkg/internal/domain"
)

// Seed generation constants. Values cycle deterministically with the event
// index so the simulated feed looks varied but is reproducible.
const (
	// Magnitudes: baseMagnitude + (index % magnitudeCycle) * magnitudeStep
	// yields the repeating ladder 4.60, 5.15, 5.70, 6.25, 6.80.
	baseMagnitude  = 4.6
	magnitudeStep  = 0.55
	magnitudeCycle = 5

	// Depths: baseDepthKM + index, i.e. depth grows 1 km per seeded event.
	baseDepthKM = 8.0

	// Epicenters drift coordinateStep degrees per event from the base
	// coordinate near Jakarta.
	baseEpicenterLat = -6.2
	baseEpicenterLon = 106.8
	coordinateStep   = 0.1

	regionName = "Jawa Barat"

	// tsunamiCouplingEvery: every 4th event (index % 4 == 0) is flagged
	// with tsunami potential and gets a companion warning.
	tsunamiCouplingEvery = 4

	// threatLevelCycle: warning threat levels cycle through the 3-step
	// ladder via index % 3, deliberately coprime with the %4 coupling so
	// consecutive warnings vary.
	threatLevelCycle = 3

	// warningETAOffset: estimated tsunami arrival is the event time plus
	// 45 minutes.
	warningETAOffset = 45 * time.Minute

	// seedEventSpacing: seeded events occurred one minute apart, counting
	// backwards from now.
	seedEventSpacing = time.Minute
)

// threatLevels is the escalating warning ladder indexed by
// index % threatLevelCycle.
var threatLevels = [...]string{"Waspada", "Siaga", "Awas"}

// InitialData returns count seeded events (and their coupled warnings)
// spaced one minute apart before now.
func InitialData(now time.Time, count int) ([]domain.SeismicEvent, []domain.TsunamiWarning) {
	if count < 0 {
		count = 0
	}
	events := make([]domain.SeismicEvent, 0, count)
	warnings := make([]domain.TsunamiWarning, 0, count/tsunamiCouplingEvery+1)
	for index := 0; index < count; index++ {
		occurredAt := now.Add(-time.Duration(count-index) * seedEventSpacing)
		eventID := "bmkg-seismic-" + strconv.Itoa(index+1)
		potentialTsunami := index%tsunamiCouplingEvery == 0
		events = append(events, domain.SeismicEvent{
			EventID:          eventID,
			Magnitude:        baseMagnitude + float64(index%magnitudeCycle)*magnitudeStep,
			DepthKM:          baseDepthKM + float64(index),
			EpicenterLat:     baseEpicenterLat + float64(index)*coordinateStep,
			EpicenterLon:     baseEpicenterLon + float64(index)*coordinateStep,
			RegionName:       regionName,
			OccurredAt:       occurredAt.Format(time.RFC3339),
			PotentialTsunami: potentialTsunami,
		})
		if potentialTsunami {
			warnings = append(warnings, domain.TsunamiWarning{
				WarningID:        "bmkg-warning-" + strconv.Itoa(index+1),
				RelatedEventID:   eventID,
				ThreatLevel:      threatLevels[index%threatLevelCycle],
				AffectedZones:    []string{"Pesisir Selatan"},
				EstimatedArrival: occurredAt.Add(warningETAOffset).Format(time.RFC3339),
			})
		}
	}
	return events, warnings
}
