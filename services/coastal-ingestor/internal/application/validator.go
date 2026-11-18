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
	// MaxFutureSkew tolerates readings whose observed_at is slightly ahead of
	// the sensor's own clock (generated_at of the page they arrived in).
	MaxFutureSkew time.Duration
}

// DefaultValidatorConfig returns the production timestamp policy.
func DefaultValidatorConfig() ValidatorConfig {
	return ValidatorConfig{MaxFutureSkew: 5 * time.Minute}
}

// Validator decides which readings enter the hazard pipeline.
//
// Timestamps are never compared with the BNPB wall clock. Sensor Pantai runs
// on its own (virtual, accelerated, restartable) clock, so the only clock an
// observed_at can be judged against is the sensor's: generated_at of the same
// response. A reading violates the contract when observed_at is missing,
// unparseable, zero, or later than generated_at + MaxFutureSkew. A reading
// that is merely old is not a violation; "the sensor says it is healthy while
// its newest data is hours old" is a property of the feed and is reported by
// the coastal_feed_freshness_seconds gauge instead.
//
// Policy for readings flagged critical (stated once, here):
//
//	A reading with critical=true whose IDENTITY is intact is ACCEPTED and
//	forwarded even when the rest of it is doubtful. Identity is intact when
//	the item decoded as an object, observation_id is present, station_id
//	names a station in the registry (with usable coordinates), and
//	observed_at parses and is not from the sensor's future. A quality other
//	than "ok", missing or implausible measurements, or any other content
//	violation is recorded in the reading's Note (forwarded as the
//	validation_note attribute next to the reported quality) and does not
//	stop it: a false tsunami alarm costs an unnecessary evacuation, a missed
//	one costs a village. Only a critical item with a broken identity is
//	quarantined, because there is nothing addressable to notify about; the
//	ingestor counts it in coastal_critical_quarantined_total and logs the
//	raw item at ERROR so a human sees it.
//
// Non-critical readings keep the strict policy: any violation quarantines.
type Validator struct {
	cfg  ValidatorConfig
	seen func(id string) bool
}

// Verdict is the outcome of validating one reading.
type Verdict struct {
	// Rejected reports that the reading must be quarantined for Reason.
	Rejected bool
	Reason   domain.QuarantineReason
	// Note lists the reservations of an accepted critical reading.
	Note string
}

// NewValidator builds a Validator, defaulting a zero skew. seen may be nil
// when duplicate detection is not wired.
func NewValidator(cfg ValidatorConfig, seen func(id string) bool) *Validator {
	if cfg.MaxFutureSkew <= 0 {
		cfg.MaxFutureSkew = DefaultValidatorConfig().MaxFutureSkew
	}
	return &Validator{cfg: cfg, seen: seen}
}

// Validate reports the first quarantine reason for a reading, or false when
// the reading is acceptable.
func (v *Validator) Validate(reading domain.Reading) (domain.QuarantineReason, bool) {
	verdict := v.Assess(reading)
	return verdict.Reason, verdict.Rejected
}

// Assess applies the policy described on Validator.
func (v *Validator) Assess(reading domain.Reading) Verdict {
	if reason, broken := v.identity(reading); broken {
		return Verdict{Rejected: true, Reason: reason}
	}
	concerns := v.content(reading)
	if len(concerns) == 0 {
		return Verdict{}
	}
	if reading.Critical {
		notes := make([]string, len(concerns))
		for i, concern := range concerns {
			notes[i] = concern.note
		}
		return Verdict{Note: strings.Join(notes, "; ")}
	}
	return Verdict{Rejected: true, Reason: concerns[0].reason}
}

// identity reports the first reason the reading cannot be tied to one
// observation at one known place and time.
func (v *Validator) identity(reading domain.Reading) (domain.QuarantineReason, bool) {
	if reading.IdentityViolation != "" {
		return reading.IdentityViolation, true
	}
	if strings.TrimSpace(reading.ID) == "" {
		return domain.ReasonMissingID, true
	}
	if strings.TrimSpace(reading.SensorID) == "" {
		return domain.ReasonMissingSensorID, true
	}
	if strings.TrimSpace(reading.AreaName) == "" {
		return domain.ReasonUnknownStation, true
	}
	// Negated comparisons also reject NaN.
	if !(reading.Latitude >= -90 && reading.Latitude <= 90) {
		return domain.ReasonInvalidLatitude, true
	}
	if !(reading.Longitude >= -180 && reading.Longitude <= 180) {
		return domain.ReasonInvalidLongitude, true
	}
	if reading.OccurredAt.IsZero() || reading.OccurredAt.Unix() == 0 {
		return domain.ReasonOccurredAtZero, true
	}
	// RFC 3339 cannot express years beyond 9999 and the Aggregator rejects
	// them; negative years are equally meaningless.
	if year := reading.OccurredAt.UTC().Year(); year < 1 || year > 9999 {
		return domain.ReasonInvalidObservedAt, true
	}
	// Without a sensor clock there is nothing honest to compare with, so the
	// future check is skipped rather than borrowed from our wall clock.
	if !reading.GeneratedAt.IsZero() && reading.OccurredAt.After(reading.GeneratedAt.Add(v.cfg.MaxFutureSkew)) {
		return domain.ReasonOccurredAtFuture, true
	}
	return "", false
}

type concern struct {
	reason domain.QuarantineReason
	note   string
}

// content lists every doubt about a reading whose identity is intact, in the
// order a non-critical reading is rejected for them.
func (v *Validator) content(reading domain.Reading) []concern {
	var concerns []concern
	switch reading.Quality {
	case "ok":
	case "unavailable":
		concerns = append(concerns, concern{domain.ReasonQualityUnusable, "quality=unavailable"})
	default:
		concerns = append(concerns, concern{domain.ReasonQualityRejected, "quality=" + quoteShort(reading.Quality)})
	}
	if reading.ContentViolation != "" {
		concerns = append(concerns, concern{reading.ContentViolation, string(reading.ContentViolation)})
	}
	if reading.HasMeasurements {
		// Negated comparisons also reject NaN and infinities.
		if !(reading.WaveHeightM >= domain.MinWaveHeightM && reading.WaveHeightM <= domain.MaxWaveHeightM) {
			concerns = append(concerns, concern{domain.ReasonInvalidMeasurement, "wave_height_m out of range"})
		} else if !(reading.SeaLevelAnomalyM >= domain.MinSeaLevelAnomalyM && reading.SeaLevelAnomalyM <= domain.MaxSeaLevelAnomalyM) {
			concerns = append(concerns, concern{domain.ReasonInvalidMeasurement, "sea_level_anomaly_m out of range"})
		}
	}
	if v.seen != nil && v.seen(reading.ID) {
		concerns = append(concerns, concern{domain.ReasonDuplicateID, "duplicate_id"})
	}
	if !reading.Severity.Valid() {
		concerns = append(concerns, concern{domain.ReasonInvalidSeverity, "invalid_severity"})
	}
	return concerns
}

// quoteShort keeps an attacker-sized quality string out of the note.
func quoteShort(value string) string {
	const limit = 32
	if len(value) > limit {
		value = strings.ToValidUTF8(value[:limit], "") + "..."
	}
	return `"` + value + `"`
}
