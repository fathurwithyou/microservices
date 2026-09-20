package eventmessage

import (
	"encoding/json"
	"time"

	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"google.golang.org/protobuf/proto"
)

const RoutingKey = "hazard.event"

func MarshalHazard(event domain.HazardEvent) ([]byte, error) {
	payload := &eventspb.HazardEvent{HazardId: event.HazardID, Source: source(event.Source), SourceRefId: event.SourceRefID, HazardType: hazardType(event.HazardType), Severity: severity(event.Severity), AreaName: event.AreaName, Latitude: event.Latitude, Longitude: event.Longitude, OccurredAt: event.OccurredAt.Format(time.RFC3339), IngestedAt: event.IngestedAt.Format(time.RFC3339), Attributes: map[string]string{}}
	for key, value := range event.Attributes {
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		payload.Attributes[key] = string(encoded)
	}
	return proto.Marshal(payload)
}

func source(value domain.Source) eventspb.Source {
	if value == domain.SourceBMKG {
		return eventspb.Source_SOURCE_BMKG
	}
	return eventspb.Source_SOURCE_PVMBG
}

func hazardType(value domain.HazardType) eventspb.HazardType {
	if value == domain.HazardSeismic {
		return eventspb.HazardType_HAZARD_TYPE_SEISMIC
	}
	return eventspb.HazardType_HAZARD_TYPE_VOLCANIC
}

func severity(value domain.Severity) eventspb.Severity {
	switch value {
	case domain.SeverityNormal:
		return eventspb.Severity_SEVERITY_NORMAL
	case domain.SeverityWaspada:
		return eventspb.Severity_SEVERITY_WASPADA
	case domain.SeveritySiaga:
		return eventspb.Severity_SEVERITY_SIAGA
	case domain.SeverityAwas:
		return eventspb.Severity_SEVERITY_AWAS
	default:
		return eventspb.Severity_SEVERITY_UNSPECIFIED
	}
}
