package eventmessage

import (
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
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
