package application

import (
	"math"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
)

// sensorNow is the sensor's own clock in these tests. It is deliberately far
// from any wall clock: the validator must never look at ours.
var sensorNow = time.Date(2026, time.November, 12, 2, 43, 5, 0, time.UTC)

func baseReading() domain.Reading {
	return domain.Reading{
		ID:               "reading-1",
		SensorID:         "sensor-1",
		AreaName:         "Sumatera Barat",
		Quality:          "ok",
		OccurredAt:       sensorNow.Add(-time.Minute),
		ReceivedAt:       time.Date(2031, time.January, 1, 0, 0, 0, 0, time.UTC),
		GeneratedAt:      sensorNow,
		Latitude:         -6.2,
		Longitude:        106.8,
		WaveHeightM:      1.8,
		SeaLevelAnomalyM: 0.42,
		HasMeasurements:  true,
		Severity:         domain.SeverityWaspada,
		Payload:          []byte(`{"id":"reading-1"}`),
	}
}

func TestValidatorReasons(t *testing.T) {
	seen := map[string]bool{"reading-1": false, "dup-1": true}

	tests := []struct {
		name     string
		mutate   func(*domain.Reading)
		want     domain.QuarantineReason
		rejected bool
	}{
		{name: "valid", mutate: func(*domain.Reading) {}},
		{name: "missing id", mutate: func(r *domain.Reading) { r.ID = "" }, want: domain.ReasonMissingID, rejected: true},
		{name: "blank id", mutate: func(r *domain.Reading) { r.ID = "  \t" }, want: domain.ReasonMissingID, rejected: true},
		{name: "missing sensor id", mutate: func(r *domain.Reading) { r.SensorID = "" }, want: domain.ReasonMissingSensorID, rejected: true},
		{name: "blank sensor id", mutate: func(r *domain.Reading) { r.SensorID = " " }, want: domain.ReasonMissingSensorID, rejected: true},
		{name: "empty quality", mutate: func(r *domain.Reading) { r.Quality = "" }, want: domain.ReasonQualityRejected, rejected: true},
		{name: "expired quality", mutate: func(r *domain.Reading) { r.Quality = "expired" }, want: domain.ReasonQualityRejected, rejected: true},
		{name: "unavailable quality", mutate: func(r *domain.Reading) { r.Quality = "unavailable" }, want: domain.ReasonQualityUnusable, rejected: true},
		{name: "unknown station", mutate: func(r *domain.Reading) { r.AreaName = "" }, want: domain.ReasonUnknownStation, rejected: true},
		{name: "latitude south pole", mutate: func(r *domain.Reading) { r.Latitude = -90 }},
		{name: "latitude north pole", mutate: func(r *domain.Reading) { r.Latitude = 90 }},
		{name: "latitude above range", mutate: func(r *domain.Reading) { r.Latitude = 90.0001 }, want: domain.ReasonInvalidLatitude, rejected: true},
		{name: "latitude below range", mutate: func(r *domain.Reading) { r.Latitude = -90.0001 }, want: domain.ReasonInvalidLatitude, rejected: true},
		{name: "latitude NaN", mutate: func(r *domain.Reading) { r.Latitude = math.NaN() }, want: domain.ReasonInvalidLatitude, rejected: true},
		{name: "longitude west edge", mutate: func(r *domain.Reading) { r.Longitude = -180 }},
		{name: "longitude east edge", mutate: func(r *domain.Reading) { r.Longitude = 180 }},
		{name: "longitude above range", mutate: func(r *domain.Reading) { r.Longitude = 180.0001 }, want: domain.ReasonInvalidLongitude, rejected: true},
		{name: "longitude below range", mutate: func(r *domain.Reading) { r.Longitude = -180.0001 }, want: domain.ReasonInvalidLongitude, rejected: true},
		{name: "longitude NaN", mutate: func(r *domain.Reading) { r.Longitude = math.NaN() }, want: domain.ReasonInvalidLongitude, rejected: true},
		{name: "zero occurred_at", mutate: func(r *domain.Reading) { r.OccurredAt = time.Time{} }, want: domain.ReasonOccurredAtZero, rejected: true},
		{name: "unix epoch occurred_at", mutate: func(r *domain.Reading) { r.OccurredAt = time.Unix(0, 0) }, want: domain.ReasonOccurredAtZero, rejected: true},
		{name: "year beyond RFC 3339", mutate: func(r *domain.Reading) {
			r.OccurredAt = time.Date(12000, 1, 1, 0, 0, 0, 0, time.UTC)
			r.GeneratedAt = time.Time{}
		}, want: domain.ReasonInvalidObservedAt, rejected: true},
		{name: "occurred_at at future skew", mutate: func(r *domain.Reading) { r.OccurredAt = sensorNow.Add(5 * time.Minute) }},
		{name: "occurred_at past future skew", mutate: func(r *domain.Reading) { r.OccurredAt = sensorNow.Add(5*time.Minute + time.Nanosecond) }, want: domain.ReasonOccurredAtFuture, rejected: true},
		// Old data is a property of the feed, not a violation of one reading.
		{name: "occurred_at a day behind the sensor clock", mutate: func(r *domain.Reading) { r.OccurredAt = sensorNow.Add(-24*time.Hour - time.Nanosecond) }},
		{name: "occurred_at a year behind the sensor clock", mutate: func(r *domain.Reading) { r.OccurredAt = sensorNow.Add(-365 * 24 * time.Hour) }},
		{name: "no sensor clock skips the future check", mutate: func(r *domain.Reading) {
			r.GeneratedAt = time.Time{}
			r.OccurredAt = sensorNow.Add(48 * time.Hour)
		}},
		{name: "identity violation from the decoder", mutate: func(r *domain.Reading) { r.IdentityViolation = domain.ReasonMalformedItem }, want: domain.ReasonMalformedItem, rejected: true},
		{name: "identity violation wins over content", mutate: func(r *domain.Reading) {
			r.IdentityViolation = domain.ReasonMissingObservedAt
			r.Quality = "suspect"
		}, want: domain.ReasonMissingObservedAt, rejected: true},
		{name: "content violation from the decoder", mutate: func(r *domain.Reading) {
			r.ContentViolation = domain.ReasonInvalidFieldType
			r.HasMeasurements = false
		}, want: domain.ReasonInvalidFieldType, rejected: true},
		{name: "missing measurement", mutate: func(r *domain.Reading) {
			r.ContentViolation = domain.ReasonMissingMeasurement
			r.HasMeasurements = false
		}, want: domain.ReasonMissingMeasurement, rejected: true},
		{name: "wave height at upper bound", mutate: func(r *domain.Reading) { r.WaveHeightM = domain.MaxWaveHeightM }},
		{name: "wave height above bound", mutate: func(r *domain.Reading) { r.WaveHeightM = domain.MaxWaveHeightM + 0.001 }, want: domain.ReasonInvalidMeasurement, rejected: true},
		{name: "negative wave height", mutate: func(r *domain.Reading) { r.WaveHeightM = -0.1 }, want: domain.ReasonInvalidMeasurement, rejected: true},
		{name: "wave height NaN", mutate: func(r *domain.Reading) { r.WaveHeightM = math.NaN() }, want: domain.ReasonInvalidMeasurement, rejected: true},
		{name: "anomaly at lower bound", mutate: func(r *domain.Reading) { r.SeaLevelAnomalyM = domain.MinSeaLevelAnomalyM }},
		{name: "anomaly below bound", mutate: func(r *domain.Reading) { r.SeaLevelAnomalyM = -31 }, want: domain.ReasonInvalidMeasurement, rejected: true},
		{name: "anomaly infinite", mutate: func(r *domain.Reading) { r.SeaLevelAnomalyM = math.Inf(1) }, want: domain.ReasonInvalidMeasurement, rejected: true},
		{name: "duplicate id", mutate: func(r *domain.Reading) { r.ID = "dup-1" }, want: domain.ReasonDuplicateID, rejected: true},
		{name: "invalid severity", mutate: func(r *domain.Reading) { r.Severity = "UNKNOWN" }, want: domain.ReasonInvalidSeverity, rejected: true},
		{name: "empty severity", mutate: func(r *domain.Reading) { r.Severity = "" }, want: domain.ReasonInvalidSeverity, rejected: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reading := baseReading()
			tc.mutate(&reading)

			got, rejected := NewValidator(DefaultValidatorConfig(), func(id string) bool {
				return seen[id]
			}).Validate(reading)

			if rejected != tc.rejected {
				t.Fatalf("rejected = %v, want %v (reason %q)", rejected, tc.rejected, got)
			}
			if got != tc.want {
				t.Errorf("reason = %q, want %q", got, tc.want)
			}
		})
	}
}

// The sensor restarts its virtual clock at the same date every time it starts,
// so the same reading must get the same verdict whatever our own clock says.
// Under the old wall-clock policy every reading of a restarted sensor was
// quarantined as occurred_at_stale, critical ones included.
func TestValidatorJudgesAgainstSensorClockOnly(t *testing.T) {
	v := NewValidator(DefaultValidatorConfig(), nil)
	restartEpoch := time.Date(2026, time.November, 12, 0, 0, 0, 0, time.UTC)
	reading := baseReading()
	reading.Critical, reading.Severity = true, domain.SeverityAwas
	reading.OccurredAt = restartEpoch.Add(30 * time.Minute)
	reading.GeneratedAt = restartEpoch.Add(30*time.Minute + 20*time.Second)

	for _, wallClock := range []time.Time{
		restartEpoch.Add(30 * time.Minute),     // clocks happen to agree
		restartEpoch.Add(6 * 24 * time.Hour),   // sensor restarted days later: "stale" by our clock
		restartEpoch.Add(-90 * 24 * time.Hour), // "future" by our clock
		time.Now(),                             // whenever this test runs
	} {
		reading.ReceivedAt = wallClock
		if reason, rejected := v.Validate(reading); rejected {
			t.Fatalf("wall clock %s: rejected as %q; the wall clock must not matter", wallClock.Format(time.RFC3339), reason)
		}
	}

	reading.OccurredAt = reading.GeneratedAt.Add(6 * time.Minute)
	if reason, rejected := v.Validate(reading); !rejected || reason != domain.ReasonOccurredAtFuture {
		t.Fatalf("observed_at 6 min ahead of generated_at: got (%q, %v), want occurred_at_future", reason, rejected)
	}
}

func TestValidatorCriticalPolicy(t *testing.T) {
	v := NewValidator(DefaultValidatorConfig(), nil)
	critical := func(mutate func(*domain.Reading)) domain.Reading {
		reading := baseReading()
		reading.Critical, reading.Severity = true, domain.SeverityAwas
		mutate(&reading)
		return reading
	}

	accepted := []struct {
		name     string
		reading  domain.Reading
		wantNote string
	}{
		{"clean", critical(func(*domain.Reading) {}), ""},
		{"suspect quality", critical(func(r *domain.Reading) { r.Quality = "suspect" }), `quality="suspect"`},
		{"unavailable quality", critical(func(r *domain.Reading) { r.Quality = "unavailable" }), "quality=unavailable"},
		{"missing measurements", critical(func(r *domain.Reading) {
			r.HasMeasurements, r.ContentViolation = false, domain.ReasonMissingMeasurement
		}), "missing_measurement"},
		{"implausible wave", critical(func(r *domain.Reading) { r.WaveHeightM = 4000 }), "wave_height_m out of range"},
		{"several doubts", critical(func(r *domain.Reading) {
			r.Quality = ""
			r.SeaLevelAnomalyM = math.NaN()
		}), `quality=""; sea_level_anomaly_m out of range`},
	}
	for _, tc := range accepted {
		t.Run("accepted/"+tc.name, func(t *testing.T) {
			verdict := v.Assess(tc.reading)
			if verdict.Rejected {
				t.Fatalf("rejected as %q; a critical reading with an intact identity must be forwarded", verdict.Reason)
			}
			if verdict.Note != tc.wantNote {
				t.Errorf("note = %q, want %q", verdict.Note, tc.wantNote)
			}
		})
	}

	quarantined := []struct {
		name    string
		reading domain.Reading
		want    domain.QuarantineReason
	}{
		{"no observation id", critical(func(r *domain.Reading) { r.ID = "" }), domain.ReasonMissingID},
		{"no station id", critical(func(r *domain.Reading) { r.SensorID = "" }), domain.ReasonMissingSensorID},
		{"unknown station", critical(func(r *domain.Reading) { r.AreaName = "" }), domain.ReasonUnknownStation},
		{"undecodable item", critical(func(r *domain.Reading) { r.IdentityViolation = domain.ReasonMalformedItem }), domain.ReasonMalformedItem},
		{"no observed_at", critical(func(r *domain.Reading) {
			r.OccurredAt, r.IdentityViolation = time.Time{}, domain.ReasonMissingObservedAt
		}), domain.ReasonMissingObservedAt},
		{"from the sensor's future", critical(func(r *domain.Reading) { r.OccurredAt = r.GeneratedAt.Add(time.Hour) }), domain.ReasonOccurredAtFuture},
	}
	for _, tc := range quarantined {
		t.Run("quarantined/"+tc.name, func(t *testing.T) {
			verdict := v.Assess(tc.reading)
			if !verdict.Rejected || verdict.Reason != tc.want {
				t.Fatalf("verdict = %+v, want rejection %q", verdict, tc.want)
			}
		})
	}
}

func TestValidatorAcceptsEverySeverity(t *testing.T) {
	for _, severity := range []domain.Severity{
		domain.SeverityNormal,
		domain.SeverityWaspada,
		domain.SeveritySiaga,
		domain.SeverityAwas,
	} {
		t.Run(string(severity), func(t *testing.T) {
			reading := baseReading()
			reading.Severity = severity
			if reason, rejected := NewValidator(DefaultValidatorConfig(), nil).Validate(reading); rejected {
				t.Fatalf("unexpected rejection: %q", reason)
			}
		})
	}
}

func TestValidatorCustomConfig(t *testing.T) {
	v := NewValidator(ValidatorConfig{MaxFutureSkew: time.Second}, nil)
	reading := baseReading()

	reading.OccurredAt = sensorNow.Add(2 * time.Second)
	if reason, rejected := v.Validate(reading); !rejected || reason != domain.ReasonOccurredAtFuture {
		t.Errorf("future: got (%q, %v), want future rejection", reason, rejected)
	}
	reading.OccurredAt = sensorNow.Add(time.Second)
	if reason, rejected := v.Validate(reading); rejected {
		t.Errorf("at the skew: rejected as %q", reason)
	}
}

func TestValidatorZeroConfigUsesDefaults(t *testing.T) {
	v := NewValidator(ValidatorConfig{}, nil)
	reading := baseReading()
	reading.OccurredAt = sensorNow.Add(5 * time.Minute)
	if reason, rejected := v.Validate(reading); rejected {
		t.Fatalf("zero config should keep the 5 min default skew, got %q", reason)
	}
}

func TestQuarantineReasonRetryable(t *testing.T) {
	tests := []struct {
		reason domain.QuarantineReason
		want   bool
	}{
		{domain.ReasonMissingID, false},
		{domain.ReasonMissingSensorID, false},
		{domain.ReasonInvalidLatitude, false},
		{domain.ReasonInvalidLongitude, false},
		{domain.ReasonOccurredAtZero, false},
		{domain.ReasonOccurredAtFuture, true},
		{domain.ReasonOccurredAtStale, false},
		{domain.ReasonDuplicateID, false},
		{domain.ReasonInvalidSeverity, false},
	}
	for _, tc := range tests {
		if got := tc.reason.Retryable(); got != tc.want {
			t.Errorf("%q.Retryable() = %v, want %v", tc.reason, got, tc.want)
		}
	}
}
