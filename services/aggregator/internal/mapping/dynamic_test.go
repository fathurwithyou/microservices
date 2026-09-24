package mapping

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

func TestVolcanicPreservesAdditiveFields(t *testing.T) {
	raw := map[string]json.RawMessage{}
	for key, value := range map[string]any{"report_id": "r-1", "volcano_id": "merapi", "alert_level": "Waspada", "eruption_count_24h": 2, "ash_column_height_m": 420.0, "reported_at": time.Now().UTC().Format(time.RFC3339), "confidence_level": 0.91, "future_field": "kept"} {
		encoded, _ := json.Marshal(value)
		raw[key] = encoded
	}
	event, err := Volcanic(raw, map[string]domain.VolcanoReference{"merapi": {Name: "Gunung Merapi", Latitude: -7.54, Longitude: 110.44}}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if event.Source != domain.SourcePVMBG || event.SourceRefID != "r-1" || event.HazardType != domain.HazardVolcanic || event.Severity != domain.SeverityWaspada || event.AreaName != "Gunung Merapi" || event.Latitude != -7.54 || event.Longitude != 110.44 {
		t.Fatalf("volcanic canonical mapping = %#v", event)
	}
	if event.Attributes["confidence_level"] != float64(0.91) || event.Attributes["future_field"] != "kept" {
		t.Fatalf("additive fields not preserved: %#v", event.Attributes)
	}
}

func TestSeismicSeverityMapping(t *testing.T) {
	cases := []struct {
		name      string
		magnitude float64
		threat    string
		want      domain.Severity
	}{
		{name: "below five without warning", magnitude: 4.9, want: domain.SeverityNormal},
		{name: "five without warning", magnitude: 5.0, want: domain.SeverityWaspada},
		{name: "six point five without warning", magnitude: 6.5, want: domain.SeveritySiaga},
		{name: "awas warning overrides magnitude", magnitude: 4.2, threat: "Awas", want: domain.SeverityAwas},
		{name: "siaga warning", magnitude: 5.0, threat: "Siaga", want: domain.SeveritySiaga},
		{name: "waspada warning", magnitude: 4.2, threat: "Waspada", want: domain.SeverityWaspada},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			raw := map[string]json.RawMessage{}
			for key, value := range map[string]any{
				"event_id": "e-1", "magnitude": testCase.magnitude, "depth_km": 10.0,
				"epicenter_lat": -6.2, "epicenter_lon": 106.8, "region_name": "Jawa Barat",
				"occurred_at": time.Now().UTC().Format(time.RFC3339), "potential_tsunami": testCase.threat != "",
			} {
				encoded, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				raw[key] = encoded
			}

			var warning map[string]json.RawMessage
			if testCase.threat != "" {
				warning = map[string]json.RawMessage{}
				for key, value := range map[string]any{
					"warning_id": "w-1", "related_event_id": "e-1", "threat_level": testCase.threat,
					"affected_zones": []string{"Pesisir"}, "estimated_arrival": time.Now().UTC().Format(time.RFC3339),
				} {
					encoded, err := json.Marshal(value)
					if err != nil {
						t.Fatal(err)
					}
					warning[key] = encoded
				}
			}

			event, err := Seismic(raw, warning, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if event.Severity != testCase.want {
				t.Fatalf("severity = %q, want %q", event.Severity, testCase.want)
			}
		})
	}
}

func TestSeismicPreservesUnknownFields(t *testing.T) {
	raw := map[string]json.RawMessage{}
	for key, value := range map[string]any{"event_id": "e-1", "magnitude": 5.2, "depth_km": 10.0, "epicenter_lat": -6.2, "epicenter_lon": 106.8, "region_name": "Jawa Barat", "occurred_at": time.Now().UTC().Format(time.RFC3339), "potential_tsunami": false, "new_source_flag": "preserve-me"} {
		encoded, _ := json.Marshal(value)
		raw[key] = encoded
	}
	event, err := Seismic(raw, nil, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if event.Attributes["new_source_flag"] != "preserve-me" {
		t.Fatalf("unknown seismic field not preserved: %#v", event.Attributes)
	}
}
