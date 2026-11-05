package application

import (
	"math"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
)

func TestValidatorReasons(t *testing.T) {
	now := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	base := domain.Reading{
		ID:         "reading-1",
		SensorID:   "sensor-1",
		AreaName:   "Sumatera Barat",
		Quality:    "ok",
		OccurredAt: now.Add(-time.Minute),
		ReceivedAt: now,
		Latitude:   -6.2,
		Longitude:  106.8,
		Severity:   domain.SeverityWaspada,
		Payload:    []byte(`{"id":"reading-1"}`),
	}
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
		{name: "occurred_at at future skew", mutate: func(r *domain.Reading) { r.OccurredAt = now.Add(5 * time.Minute) }},
		{name: "occurred_at past future skew", mutate: func(r *domain.Reading) { r.OccurredAt = now.Add(5*time.Minute + time.Nanosecond) }, want: domain.ReasonOccurredAtFuture, rejected: true},
		{name: "occurred_at at max age", mutate: func(r *domain.Reading) { r.OccurredAt = now.Add(-24 * time.Hour) }},
		{name: "occurred_at past max age", mutate: func(r *domain.Reading) { r.OccurredAt = now.Add(-24*time.Hour - time.Nanosecond) }, want: domain.ReasonOccurredAtStale, rejected: true},
		{name: "duplicate id", mutate: func(r *domain.Reading) { r.ID = "dup-1" }, want: domain.ReasonDuplicateID, rejected: true},
		{name: "invalid severity", mutate: func(r *domain.Reading) { r.Severity = "UNKNOWN" }, want: domain.ReasonInvalidSeverity, rejected: true},
		{name: "empty severity", mutate: func(r *domain.Reading) { r.Severity = "" }, want: domain.ReasonInvalidSeverity, rejected: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reading := base
			tc.mutate(&reading)

			got, rejected := NewValidator(DefaultValidatorConfig(), func() time.Time { return now }, func(id string) bool {
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

func TestValidatorAcceptsEverySeverity(t *testing.T) {
	now := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	for _, severity := range []domain.Severity{
		domain.SeverityNormal,
		domain.SeverityWaspada,
		domain.SeveritySiaga,
		domain.SeverityAwas,
	} {
		t.Run(string(severity), func(t *testing.T) {
			reading := domain.Reading{
				ID: "id", SensorID: "sensor", AreaName: "Aceh", Quality: "ok",
				OccurredAt: now, Latitude: 0, Longitude: 0, Severity: severity,
			}
			if reason, rejected := NewValidator(DefaultValidatorConfig(), func() time.Time { return now }, nil).Validate(reading); rejected {
				t.Fatalf("unexpected rejection: %q", reason)
			}
		})
	}
}

func TestValidatorCustomConfig(t *testing.T) {
	now := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	cfg := ValidatorConfig{MaxFutureSkew: time.Second, MaxAge: time.Hour}
	v := NewValidator(cfg, func() time.Time { return now }, nil)
	reading := domain.Reading{ID: "id", SensorID: "sensor", AreaName: "Aceh", Quality: "ok", Latitude: 0, Longitude: 0, Severity: domain.SeverityNormal}

	reading.OccurredAt = now.Add(2 * time.Second)
	if reason, rejected := v.Validate(reading); !rejected || reason != domain.ReasonOccurredAtFuture {
		t.Errorf("future: got (%q, %v), want future rejection", reason, rejected)
	}

	reading.OccurredAt = now.Add(-2 * time.Hour)
	if reason, rejected := v.Validate(reading); !rejected || reason != domain.ReasonOccurredAtStale {
		t.Errorf("stale: got (%q, %v), want stale rejection", reason, rejected)
	}
}

func TestValidatorZeroConfigUsesDefaults(t *testing.T) {
	now := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	v := NewValidator(ValidatorConfig{}, func() time.Time { return now }, nil)
	reading := domain.Reading{
		ID: "id", SensorID: "sensor", AreaName: "Aceh", Quality: "ok",
		Latitude: 0, Longitude: 0, Severity: domain.SeverityNormal,
		OccurredAt: now.Add(-24 * time.Hour),
	}
	if reason, rejected := v.Validate(reading); rejected {
		t.Fatalf("zero config should keep the 24h default, got %q", reason)
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
