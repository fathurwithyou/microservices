package seed

import (
	"strconv"
	"time"

	"github.com/fathurwithyou/microservices/services/pvmbg/internal/domain"
)

// Seed volcano IDs and alert levels cycle deterministically with the report
// index so every seed batch covers all volcanoes and alert levels.
var seedVolcanoIDs = []string{"merapi", "semeru", "anak_krakatau"}

var seedAlertLevels = []string{"Normal", "Waspada", "Siaga", "Awas"}

// InitialReports builds count deterministic historical reports ending at now.
func InitialReports(now time.Time, count int) []domain.VolcanicReport {
	if count < 0 {
		count = 0
	}
	reports := make([]domain.VolcanicReport, 0, count)
	for index := 0; index < count; index++ {
		reportedAt := now.Add(-time.Duration(count-index) * time.Minute)
		reports = append(reports, domain.VolcanicReport{
			ReportID:         "pvmbg-report-" + strconv.Itoa(index+1),
			VolcanoID:        seedVolcanoIDs[index%len(seedVolcanoIDs)],
			AlertLevel:       seedAlertLevels[index%len(seedAlertLevels)],
			EruptionCount24H: float64(index % 4),
			AshColumnHeightM: float64(index * 125),
			ReportedAt:       reportedAt.Format(time.RFC3339),
		})
	}
	return reports
}
