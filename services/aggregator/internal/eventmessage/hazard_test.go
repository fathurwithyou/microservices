package eventmessage

import (
	"testing"
	"time"

	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"google.golang.org/protobuf/proto"
)

func testEvent() domain.HazardEvent {
	return domain.HazardEvent{
		HazardID: "h-1", Source: domain.SourceBMKG, SourceRefID: "e-1",
		HazardType: domain.HazardSeismic, Severity: domain.SeverityWaspada,
		AreaName: "Jawa Barat", Latitude: -7.1, Longitude: 107.6,
		OccurredAt: time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC),
		IngestedAt: time.Date(2026, 9, 20, 12, 0, 1, 0, time.UTC),
		Attributes: map[string]any{"magnitude": 5.5},
	}
}

func TestMarshalHazardRoundTrip(t *testing.T) {
	payload, err := MarshalHazard(testEvent())
	if err != nil {
		t.Fatalf("MarshalHazard: %v", err)
	}
	if len(payload) == 0 {
		t.Fatal("empty payload")
	}
}

func TestMarshalHazardAcceptsM2Enums(t *testing.T) {
	tests := []struct {
		name   string
		source domain.Source
		kind   domain.HazardType
		wantS  eventspb.Source
		wantH  eventspb.HazardType
	}{
		{"wildfire", domain.SourceSatellite, domain.HazardWildfire, eventspb.Source_SOURCE_SATELIT, eventspb.HazardType_HAZARD_TYPE_WILDFIRE},
		{"coastal", domain.SourceCoastalSensor, domain.HazardCoastal, eventspb.Source_SOURCE_SENSOR_PANTAI, eventspb.HazardType_HAZARD_TYPE_COASTAL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := testEvent()
			event.Source = tt.source
			event.HazardType = tt.kind
			payload, err := MarshalHazard(event)
			if err != nil {
				t.Fatalf("MarshalHazard: %v", err)
			}
			var decoded eventspb.HazardEvent
			if err := proto.Unmarshal(payload, &decoded); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if decoded.GetSource() != tt.wantS || decoded.GetHazardType() != tt.wantH {
				t.Fatalf("enums = %s/%s, want %s/%s", decoded.GetSource(), decoded.GetHazardType(), tt.wantS, tt.wantH)
			}
		})
	}
}

func TestMarshalHazardRejectsUnknownEnums(t *testing.T) {
	event := testEvent()
	event.Source = "UNKNOWN-SOURCE"
	if _, err := MarshalHazard(event); err == nil {
		t.Fatal("unknown source must fail instead of mislabeling")
	}
	event = testEvent()
	event.HazardType = "UNKNOWN-TYPE"
	if _, err := MarshalHazard(event); err == nil {
		t.Fatal("unknown hazard type must fail instead of mislabeling")
	}
	event = testEvent()
	event.Severity = "UNKNOWN-SEVERITY"
	if _, err := MarshalHazard(event); err == nil {
		t.Fatal("unknown severity must fail instead of mislabeling")
	}
}
