package grpctransport

import (
	"encoding/json"
	"time"

	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

func hazardFromProto(event *eventspb.HazardEvent) (domain.HazardEvent, error) {
	occurredAt, err := time.Parse(time.RFC3339, event.GetOccurredAt())
	if err != nil {
		return domain.HazardEvent{}, err
	}
	ingestedAt, err := time.Parse(time.RFC3339, event.GetIngestedAt())
	if err != nil {
		return domain.HazardEvent{}, err
	}
	attributes := make(map[string]any, len(event.GetAttributes()))
	for key, value := range event.GetAttributes() {
		var decoded any
		if err := json.Unmarshal([]byte(value), &decoded); err != nil {
			decoded = value
		}
		attributes[key] = decoded
	}
	return domain.HazardEvent{
		HazardID:    event.GetHazardId(),
		Source:      sourceFromProto(event.GetSource()),
		SourceRefID: event.GetSourceRefId(),
		HazardType:  hazardTypeFromProto(event.GetHazardType()),
		Severity:    severityFromProto(event.GetSeverity()),
		AreaName:    event.GetAreaName(),
		Latitude:    event.GetLatitude(),
		Longitude:   event.GetLongitude(),
		OccurredAt:  occurredAt,
		IngestedAt:  ingestedAt,
		Attributes:  attributes,
	}, nil
}
