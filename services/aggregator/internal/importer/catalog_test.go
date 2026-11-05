package importer

import (
	"strings"
	"testing"
	"time"
)

const catalogRow = `{"event_id":"e-1","magnitude":5.6,"depth_km":12,"epicenter_lat":-2.1,"epicenter_lon":110.2,"region_name":"Kalimantan Tengah","occurred_at":"2026-11-12T01:00:00Z","potential_tsunami":false}`

func TestReadAcceptsArrayAndNDJSON(t *testing.T) {
	for name, input := range map[string]string{
		"array":  "[" + catalogRow + "]",
		"ndjson": catalogRow + "\n" + catalogRow + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			rows, err := Read(strings.NewReader(input))
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if name == "ndjson" {
				want = 2
			}
			if len(rows) != want {
				t.Fatalf("rows = %d, want %d", len(rows), want)
			}
		})
	}
}

func TestEventsMapAndReportBadRows(t *testing.T) {
	rows, err := Read(strings.NewReader("[" + catalogRow + `,{"event_id":"bad"}]`))
	if err != nil {
		t.Fatal(err)
	}
	events, failures := Events(rows, time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC))
	if len(events) != 1 || len(failures) != 1 {
		t.Fatalf("events = %d, failures = %d", len(events), len(failures))
	}
	if events[0].SourceRefID != "e-1" || events[0].Attributes["magnitude"] != 5.6 {
		t.Fatalf("event = %+v", events[0])
	}
}
