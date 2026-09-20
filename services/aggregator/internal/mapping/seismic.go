package mapping

import (
	"encoding/json"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/google/uuid"
)

func Seismic(raw map[string]json.RawMessage, warning map[string]json.RawMessage, now time.Time) (domain.HazardEvent, error) {
	eventID, err := stringField(raw, "event_id")
	if err != nil {
		return domain.HazardEvent{}, err
	}
	magnitude, err := numberField(raw, "magnitude")
	if err != nil {
		return domain.HazardEvent{}, err
	}
	depth, err := numberField(raw, "depth_km")
	if err != nil {
		return domain.HazardEvent{}, err
	}
	latitude, err := numberField(raw, "epicenter_lat")
	if err != nil {
		return domain.HazardEvent{}, err
	}
	longitude, err := numberField(raw, "epicenter_lon")
	if err != nil {
		return domain.HazardEvent{}, err
	}
	areaName, err := stringField(raw, "region_name")
	if err != nil {
		return domain.HazardEvent{}, err
	}
	occurredAt, err := timeField(raw, "occurred_at")
	if err != nil {
		return domain.HazardEvent{}, err
	}
	potentialTsunami, err := boolField(raw, "potential_tsunami")
	if err != nil {
		return domain.HazardEvent{}, err
	}

	attributes := additionalAttributes(raw, seismicFields)
	attributes["magnitude"] = magnitude
	attributes["depth_km"] = depth
	attributes["potential_tsunami"] = potentialTsunami
	severity := domain.SeverityFromMagnitude(magnitude)
	if len(warning) > 0 {
		severity = addWarningAttributes(attributes, warning, magnitude)
	}

	return domain.HazardEvent{HazardID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("BMKG:"+eventID)).String(), Source: domain.SourceBMKG, SourceRefID: eventID, HazardType: domain.HazardSeismic, Severity: severity, AreaName: areaName, Latitude: latitude, Longitude: longitude, OccurredAt: occurredAt, IngestedAt: now, Attributes: attributes}, nil
}

var seismicFields = fieldSet("event_id", "magnitude", "depth_km", "epicenter_lat", "epicenter_lon", "region_name", "occurred_at", "potential_tsunami")
var warningFields = fieldSet("warning_id", "related_event_id", "threat_level", "affected_zones", "estimated_arrival")

func addWarningAttributes(attributes map[string]any, warning map[string]json.RawMessage, magnitude float64) domain.Severity {
	warningID, _ := stringField(warning, "warning_id")
	threatLevel, _ := stringField(warning, "threat_level")
	affectedZones, _ := stringSliceField(warning, "affected_zones")
	estimatedArrival, _ := stringField(warning, "estimated_arrival")
	attributes["warning_id"] = warningID
	attributes["threat_level"] = threatLevel
	attributes["affected_zones"] = affectedZones
	attributes["estimated_arrival"] = estimatedArrival
	for key, value := range additionalAttributes(warning, warningFields) {
		attributes[key] = value
	}
	return domain.SeverityFromThreat(threatLevel, magnitude)
}
