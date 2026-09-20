package projection

import eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"

type Summary struct {
	HazardID   string `json:"hazard_id"`
	Source     string `json:"source"`
	HazardType string `json:"hazard_type"`
	Severity   string `json:"severity"`
	AreaName   string `json:"area_name"`
	OccurredAt string `json:"occurred_at"`
	IngestedAt string `json:"ingested_at"`
}

func ToSummary(event *eventspb.HazardEvent) Summary {
	return Summary{HazardID: event.GetHazardId(), Source: event.GetSource().String(), HazardType: event.GetHazardType().String(), Severity: event.GetSeverity().String(), AreaName: event.GetAreaName(), OccurredAt: event.GetOccurredAt(), IngestedAt: event.GetIngestedAt()}
}
func ToRaw(event *eventspb.HazardEvent) map[string]any {
	return map[string]any{"hazard_id": event.GetHazardId(), "source": event.GetSource().String(), "source_ref_id": event.GetSourceRefId(), "hazard_type": event.GetHazardType().String(), "severity": event.GetSeverity().String(), "area_name": event.GetAreaName(), "latitude": event.GetLatitude(), "longitude": event.GetLongitude(), "occurred_at": event.GetOccurredAt(), "ingested_at": event.GetIngestedAt(), "attributes": event.GetAttributes()}
}
