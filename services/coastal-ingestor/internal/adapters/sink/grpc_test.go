package sink

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
)

func sinkReading() domain.Reading {
	return domain.Reading{
		ID: "obs-000120", SensorID: "stn-padang-01", AreaName: "Sumatera Barat", Quality: "ok",
		OccurredAt:  time.Date(2026, time.November, 12, 0, 30, 0, 0, time.UTC),
		ReceivedAt:  time.Date(2026, time.November, 18, 10, 0, 0, 123456789, time.FixedZone("WIB", 7*3600)),
		GeneratedAt: time.Date(2026, time.November, 12, 0, 30, 20, 0, time.UTC),
		WaveHeightM: 1.8, SeaLevelAnomalyM: 0.42, HasMeasurements: true,
		Severity: domain.SeverityAwas, Critical: true,
	}
}

func TestCriticalReadingCarriesLatencyReferences(t *testing.T) {
	reading := sinkReading()
	reading.Quality, reading.Note = "suspect", `quality="suspect"`
	attributes := readingEvent(reading).GetAttributes()
	want := map[string]string{
		"sensor_id":                `"stn-padang-01"`,
		"quality":                  `"suspect"`,
		"critical":                 `true`,
		"status":                   `"active"`,
		"wave_height_m":            `1.8`,
		"sea_level_anomaly_m":      `0.42`,
		"validation_note":          `"quality=\"suspect\""`,
		"received_at":              `"2026-11-18T03:00:00.123456789Z"`,
		"observation_generated_at": `"2026-11-12T00:30:20Z"`,
	}
	if !reflect.DeepEqual(attributes, want) {
		t.Fatalf("attributes = %v\nwant         %v", attributes, want)
	}
}

// An ordinary reading's event must depend on the observation only: read twice
// (other receipt, other sensor clock) it has to be identical, so the
// Aggregator's upsert stays a no-op and announces nothing.
func TestOrdinaryReadingEventIsStableAcrossRereads(t *testing.T) {
	reading := sinkReading()
	reading.Critical, reading.Severity = false, domain.SeverityNormal
	first := readingEvent(reading).GetAttributes()
	reading.ReceivedAt = reading.ReceivedAt.Add(time.Hour)
	reading.GeneratedAt = reading.GeneratedAt.Add(time.Hour)
	second := readingEvent(reading).GetAttributes()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("attributes changed on a reread:\n%v\n%v", first, second)
	}
	if _, ok := first["received_at"]; ok {
		t.Fatal("ordinary reading carries received_at")
	}
	if first["wave_height_m"] != "1.8" || first["sea_level_anomaly_m"] != "0.42" {
		t.Fatalf("measurements missing: %v", first)
	}
}

func TestUnusableMeasurementsAreLeftOut(t *testing.T) {
	missing := sinkReading()
	missing.HasMeasurements = false
	nan := sinkReading()
	nan.WaveHeightM = math.NaN()
	for name, reading := range map[string]domain.Reading{"missing": missing, "NaN": nan} {
		attributes := readingEvent(reading).GetAttributes()
		if _, ok := attributes["wave_height_m"]; ok {
			t.Errorf("%s: wave_height_m = %q, want it left out", name, attributes["wave_height_m"])
		}
		if attributes["critical"] != "true" {
			t.Errorf("%s: critical flag lost", name)
		}
	}
}
