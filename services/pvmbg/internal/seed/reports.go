package seed

import (
	"strconv"
	"time"

	"github.com/fathurwithyou/microservices/services/pvmbg/internal/domain"
)

func InitialReports(now time.Time, count int) []domain.VolcanicReport {
	if count < 0 {
		count = 0
	}
	reports := make([]domain.VolcanicReport, 0, count)
	for index := 0; index < count; index++ {
		reportedAt := now.Add(-time.Duration(count-index) * time.Minute)
		reports = append(reports, domain.VolcanicReport{
			"report_id":           "pvmbg-report-" + strconv.Itoa(index+1),
			"volcano_id":          []string{"merapi", "sem eru", "anak_krakatau"}[index%3],
			"alert_level":         []string{"Normal", "Waspada", "Siaga", "Awas"}[index%4],
			"eruption_count_24h":  index % 4,
			"ash_column_height_m": float64(index * 125),
			"reported_at":         reportedAt.Format(time.RFC3339),
		})
	}
	return reports
}
