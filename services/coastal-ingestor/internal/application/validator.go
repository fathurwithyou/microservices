package application

import (
	"strings"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
)

// Clock returns the current time; tests inject a fixed clock.
type Clock func() time.Time

// ValidatorConfig holds the timestamp policy for incoming readings.
type ValidatorConfig struct {
	// MaxFutureSkew tolerates readings whose occurred_at is slightly ahead of
	// the ingestor clock.
	MaxFutureSkew time.Duration
	// MaxAge rejects readings older than this at validation time.
	MaxAge time.Duration
}

// DefaultValidatorConfig returns the production timestamp policy.
func DefaultValidatorConfig() ValidatorConfig {
	return ValidatorConfig{
		MaxFutureSkew: 5 * time.Minute,
		MaxAge:        24 * time.Hour,
	}
}

// Validator rejects readings that cannot be ingested. It is stateless apart
// from the injected duplicate check, so the caller decides how ID sets are
// stored.
type Validator struct {
	cfg  ValidatorConfig
	now  Clock
	seen func(id string) bool
}

// NewValidator builds a Validator, defaulting zero durations and a nil clock.
// seen may be nil when duplicate detection is not wired yet.
func NewValidator(cfg ValidatorConfig, now Clock, seen func(id string) bool) *Validator {
	defaults := DefaultValidatorConfig()
	if cfg.MaxFutureSkew <= 0 {
		cfg.MaxFutureSkew = defaults.MaxFutureSkew
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = defaults.MaxAge
	}
	if now == nil {
		now = time.Now
	}
	return &Validator{cfg: cfg, now: now, seen: seen}
}

// Validate reports the first quarantine reason for a reading, or false when
// the reading is acceptable.
func (v *Validator) Validate(reading domain.Reading) (domain.QuarantineReason, bool) {
	if strings.TrimSpace(reading.ID) == "" {
		return domain.ReasonMissingID, true
	}
	if strings.TrimSpace(reading.SensorID) == "" {
		return domain.ReasonMissingSensorID, true
	}
	// Negated comparisons also reject NaN.
	if !(reading.Latitude >= -90 && reading.Latitude <= 90) {
		return domain.ReasonInvalidLatitude, true
	}
	if !(reading.Longitude >= -180 && reading.Longitude <= 180) {
		return domain.ReasonInvalidLongitude, true
	}
	if reading.OccurredAt.IsZero() {
		return domain.ReasonOccurredAtZero, true
	}
	now := v.now()
	if reading.OccurredAt.After(now.Add(v.cfg.MaxFutureSkew)) {
		return domain.ReasonOccurredAtFuture, true
	}
	if reading.OccurredAt.Before(now.Add(-v.cfg.MaxAge)) {
		return domain.ReasonOccurredAtStale, true
	}
	if v.seen != nil && v.seen(reading.ID) {
		return domain.ReasonDuplicateID, true
	}
	if !reading.Severity.Valid() {
		return domain.ReasonInvalidSeverity, true
	}
	return "", false
}
