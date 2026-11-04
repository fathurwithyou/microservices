package grpctransport

import (
	"fmt"
	"time"

	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

func sourceFromProto(value eventspb.Source) domain.Source {
	switch value {
	case eventspb.Source_SOURCE_BMKG:
		return domain.SourceBMKG
	case eventspb.Source_SOURCE_PVMBG:
		return domain.SourcePVMBG
	case eventspb.Source_SOURCE_SATELIT:
		return domain.SourceSatellite
	case eventspb.Source_SOURCE_SENSOR_PANTAI:
		return domain.SourceCoastalSensor
	default:
		return ""
	}
}

func hazardTypeFromProto(value eventspb.HazardType) domain.HazardType {
	switch value {
	case eventspb.HazardType_HAZARD_TYPE_SEISMIC:
		return domain.HazardSeismic
	case eventspb.HazardType_HAZARD_TYPE_VOLCANIC:
		return domain.HazardVolcanic
	case eventspb.HazardType_HAZARD_TYPE_WILDFIRE:
		return domain.HazardWildfire
	case eventspb.HazardType_HAZARD_TYPE_COASTAL:
		return domain.HazardCoastal
	default:
		return ""
	}
}

func severityFromProto(value eventspb.Severity) domain.Severity {
	switch value {
	case eventspb.Severity_SEVERITY_NORMAL:
		return domain.SeverityNormal
	case eventspb.Severity_SEVERITY_WASPADA:
		return domain.SeverityWaspada
	case eventspb.Severity_SEVERITY_SIAGA:
		return domain.SeveritySiaga
	case eventspb.Severity_SEVERITY_AWAS:
		return domain.SeverityAwas
	default:
		return ""
	}
}

func hazardToProto(item domain.HazardEvent) *eventspb.HazardEvent {
	result := &eventspb.HazardEvent{HazardId: item.HazardID, Source: sourceToProto(item.Source), SourceRefId: item.SourceRefID, HazardType: hazardTypeToProto(item.HazardType), Severity: severityToProto(item.Severity), AreaName: item.AreaName, Latitude: item.Latitude, Longitude: item.Longitude, OccurredAt: item.OccurredAt.Format(time.RFC3339), IngestedAt: item.IngestedAt.Format(time.RFC3339), Attributes: map[string]string{}}
	for key, value := range item.Attributes {
		result.Attributes[key] = fmt.Sprint(value)
	}
	return result
}

func sourceStatusToProto(item domain.SourceStatus, source eventspb.Source) *eventspb.SourceStatus {
	result := &eventspb.SourceStatus{Source: source, Status: string(item.Status), LastError: item.LastError}
	if item.LastSuccessAt != nil {
		result.LastSuccessAt = item.LastSuccessAt.Format(time.RFC3339)
	}
	if item.StaleSince != nil {
		result.StaleSince = item.StaleSince.Format(time.RFC3339)
	}
	return result
}

func sourceToProto(value domain.Source) eventspb.Source {
	switch value {
	case domain.SourceBMKG:
		return eventspb.Source_SOURCE_BMKG
	case domain.SourcePVMBG:
		return eventspb.Source_SOURCE_PVMBG
	case domain.SourceSatellite:
		return eventspb.Source_SOURCE_SATELIT
	case domain.SourceCoastalSensor:
		return eventspb.Source_SOURCE_SENSOR_PANTAI
	default:
		return eventspb.Source_SOURCE_UNSPECIFIED
	}
}

func hazardTypeToProto(value domain.HazardType) eventspb.HazardType {
	switch value {
	case domain.HazardSeismic:
		return eventspb.HazardType_HAZARD_TYPE_SEISMIC
	case domain.HazardVolcanic:
		return eventspb.HazardType_HAZARD_TYPE_VOLCANIC
	case domain.HazardWildfire:
		return eventspb.HazardType_HAZARD_TYPE_WILDFIRE
	case domain.HazardCoastal:
		return eventspb.HazardType_HAZARD_TYPE_COASTAL
	default:
		return eventspb.HazardType_HAZARD_TYPE_UNSPECIFIED
	}
}

func severityToProto(value domain.Severity) eventspb.Severity {
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
