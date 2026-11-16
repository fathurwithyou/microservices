package mapping

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

// breakingReport is the record shape PVMBG sends in mode "breaking".
const breakingReport = `{"report_id":"r-1","volcano_id":"merapi","level":"Siaga","eruption_count_24h":"12","ash_column_height_m":800,"missing_required_field":true}`

func decode(t *testing.T, text string) map[string]json.RawMessage {
	t.Helper()
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		t.Fatal(err)
	}
	return raw
}

var volcanoes = map[string]domain.VolcanoReference{"merapi": {Name: "Gunung Merapi", Latitude: -7.54, Longitude: 110.44}}

func TestBreakingSchemaIsRefusedByDefault(t *testing.T) {
	SetSchemaAdapters(SchemaAdapters{})
	_, err := VolcanicReceived(decode(t, breakingReport), volcanoes, time.Now(), time.Now())
	if !errors.Is(err, domain.ErrInvalidSourcePayload) {
		t.Fatalf("err = %v, want schema violation", err)
	}
}

func TestBreakingAdapterMapsWithArrivalTime(t *testing.T) {
	SetSchemaAdapters(SchemaAdapters{PVMBGBreakingV3: true})
	defer SetSchemaAdapters(SchemaAdapters{})
	received := time.Date(2026, 11, 15, 19, 0, 3, 0, time.UTC)
	event, err := VolcanicReceived(decode(t, breakingReport), volcanoes, received.Add(time.Hour), received)
	if err != nil {
		t.Fatal(err)
	}
	if event.Severity != domain.SeveritySiaga || !event.OccurredAt.Equal(received) || event.SourceRefID != "r-1" {
		t.Fatalf("event = %+v", event)
	}
	if event.Attributes["eruption_count_24h"] != 12.0 || event.Attributes["reported_at_source"] != "received_at" || event.Attributes["schema_adapter"] != "pvmbg-breaking-v3" {
		t.Fatalf("attributes = %v", event.Attributes)
	}
	if _, ok := event.Attributes["level"]; ok {
		t.Fatal("renamed field kept as an extra attribute")
	}
	// Same hazard_id as the v1 record would have, so a later v1 copy cannot
	// create a duplicate.
	v1, err := Volcanic(decode(t, `{"report_id":"r-1","volcano_id":"merapi","alert_level":"Siaga","eruption_count_24h":12,"ash_column_height_m":800,"reported_at":"2026-11-15T19:00:00Z"}`), volcanoes, received)
	if err != nil || v1.HazardID != event.HazardID {
		t.Fatalf("v1 id %s vs adapted %s err %v", v1.HazardID, event.HazardID, err)
	}
}

func TestBreakingAdapterStillRejectsGarbage(t *testing.T) {
	SetSchemaAdapters(SchemaAdapters{PVMBGBreakingV3: true})
	defer SetSchemaAdapters(SchemaAdapters{})
	_, err := VolcanicReceived(decode(t, `{"report_id":"r-2","volcano_id":"merapi","level":"Siaga","eruption_count_24h":"many","ash_column_height_m":1}`), volcanoes, time.Now(), time.Now())
	if !errors.Is(err, domain.ErrInvalidSourcePayload) {
		t.Fatalf("err = %v, want schema violation", err)
	}
}

func TestParseSchemaAdapters(t *testing.T) {
	got, unknown := ParseSchemaAdapters(" pvmbg-breaking-v3 , nope")
	if !got.PVMBGBreakingV3 || len(unknown) != 1 || unknown[0] != "nope" {
		t.Fatalf("got %+v unknown %v", got, unknown)
	}
}
