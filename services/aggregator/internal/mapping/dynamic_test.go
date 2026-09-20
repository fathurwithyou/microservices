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
	if event.Attributes["confidence_level"] != float64(0.91) || event.Attributes["future_field"] != "kept" {
		t.Fatalf("additive fields not preserved: %#v", event.Attributes)
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
