package projection

import "github.com/fathurwithyou/microservices/services/client-api/internal/ports"

// Summary is the public, reduced view of a hazard event.
type Summary struct {
	HazardID   string `json:"hazard_id"`
	Source     string `json:"source"`
	HazardType string `json:"hazard_type"`
	Severity   string `json:"severity"`
	AreaName   string `json:"area_name"`
	OccurredAt string `json:"occurred_at"`
	IngestedAt string `json:"ingested_at"`
}

// ToSummary projects an internal event onto the fields allowed by the
// summary scope.
func ToSummary(event ports.Hazard) Summary {
	return Summary{HazardID: event.HazardID, Source: event.Source, HazardType: event.HazardType, Severity: event.Severity, AreaName: event.AreaName, OccurredAt: event.OccurredAt, IngestedAt: event.IngestedAt}
}

// ToRaw projects the complete event for clients with the raw scope.
func ToRaw(event ports.Hazard) map[string]any {
	return map[string]any{
		"hazard_id":     event.HazardID,
		"source":        event.Source,
		"source_ref_id": event.SourceRefID,
		"hazard_type":   event.HazardType,
		"severity":      event.Severity,
		"area_name":     event.AreaName,
		"latitude":      event.Latitude,
		"longitude":     event.Longitude,
		"occurred_at":   event.OccurredAt,
		"ingested_at":   event.IngestedAt,
		"attributes":    event.Attributes,
	}
}
