package application

import (
	"slices"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

// sameFire reports whether two versions of a fire would produce the same
// HazardEvent, so an unchanged fire is not sent again.
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
