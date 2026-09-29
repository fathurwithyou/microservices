package eventmessage

import (
	"encoding/json"
	"fmt"
	"time"

	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"google.golang.org/protobuf/proto"
)

// RoutingKey is the AMQP routing key for hazard event messages.
const RoutingKey = "hazard.event"

// MarshalHazard serializes a domain event into the wire-format protobuf
// payload published to consumers. Unknown enum values are rejected instead
// of silently mislabeled, so adding a domain value without updating the
// conversion fails loudly at ingest time.
func MarshalHazard(event domain.HazardEvent) ([]byte, error) {
	sourceValue, err := sourceProto(event.Source)
	if err != nil {
		return nil, err
	}
	hazardTypeValue, err := hazardTypeProto(event.HazardType)
	if err != nil {
		return nil, err
	}
	severityValue, err := severityProto(event.Severity)
	if err != nil {
		return nil, err
	}
	payload := &eventspb.HazardEvent{HazardId: event.HazardID, Source: sourceValue, SourceRefId: event.SourceRefID, HazardType: hazardTypeValue, Severity: severityValue, AreaName: event.AreaName, Latitude: event.Latitude, Longitude: event.Longitude, OccurredAt: event.OccurredAt.Format(time.RFC3339), IngestedAt: event.IngestedAt.Format(time.RFC3339), Attributes: map[string]string{}}
	for key, value := range event.Attributes {
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode attribute %q: %w", key, err)
		}
		payload.Attributes[key] = string(encoded)
	}
	return proto.Marshal(payload)
}

func sourceProto(value domain.Source) (eventspb.Source, error) {
	switch value {
	case domain.SourceBMKG:
		return eventspb.Source_SOURCE_BMKG, nil
	case domain.SourcePVMBG:
		return eventspb.Source_SOURCE_PVMBG, nil
	default:
		return eventspb.Source_SOURCE_UNSPECIFIED, fmt.Errorf("unknown source %q: %w", value, domain.ErrInvalidSourcePayload)
	}
}

func hazardTypeProto(value domain.HazardType) (eventspb.HazardType, error) {
	switch value {
	case domain.HazardSeismic:
		return eventspb.HazardType_HAZARD_TYPE_SEISMIC, nil
	case domain.HazardVolcanic:
		return eventspb.HazardType_HAZARD_TYPE_VOLCANIC, nil
	default:
		return eventspb.HazardType_HAZARD_TYPE_UNSPECIFIED, fmt.Errorf("unknown hazard type %q: %w", value, domain.ErrInvalidSourcePayload)
	}
}

func severityProto(value domain.Severity) (eventspb.Severity, error) {
	switch value {
	case domain.SeverityNormal:
		return eventspb.Severity_SEVERITY_NORMAL, nil
	case domain.SeverityWaspada:
		return eventspb.Severity_SEVERITY_WASPADA, nil
	case domain.SeveritySiaga:
		return eventspb.Severity_SEVERITY_SIAGA, nil
	case domain.SeverityAwas:
		return eventspb.Severity_SEVERITY_AWAS, nil
	default:
		return eventspb.Severity_SEVERITY_UNSPECIFIED, fmt.Errorf("unknown severity %q: %w", value, domain.ErrInvalidSourcePayload)
	}
}
