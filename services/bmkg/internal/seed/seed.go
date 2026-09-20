package seed

import (
	"strconv"
	"time"

	"github.com/fathurwithyou/microservices/services/bmkg/internal/domain"
)

func InitialData(now time.Time, count int) ([]domain.SeismicEvent, []domain.TsunamiWarning) {
	if count < 0 {
		count = 0
	}
	events := make([]domain.SeismicEvent, 0, count)
	warnings := make([]domain.TsunamiWarning, 0, count/4+1)
	for index := 0; index < count; index++ {
		occurredAt := now.Add(-time.Duration(count-index) * time.Minute)
		eventID := "bmkg-seismic-" + strconv.Itoa(index+1)
		events = append(events, domain.SeismicEvent{
			EventID: eventID, Magnitude: 4.6 + float64(index%5)*0.55, DepthKM: 8 + float64(index),
			EpicenterLat: -6.2 + float64(index)*0.1, EpicenterLon: 106.8 + float64(index)*0.1,
			RegionName: "Jawa Barat", OccurredAt: occurredAt.Format(time.RFC3339), PotentialTsunami: index%4 == 0,
		})
		if index%4 == 0 {
			warnings = append(warnings, domain.TsunamiWarning{
				WarningID: "bmkg-warning-" + strconv.Itoa(index+1), RelatedEventID: eventID,
				ThreatLevel: []string{"Waspada", "Siaga", "Awas"}[index%3], AffectedZones: []string{"Pesisir Selatan"},
				EstimatedArrival: occurredAt.Add(45 * time.Minute).Format(time.RFC3339),
			})
		}
	}
	return events, warnings
}
