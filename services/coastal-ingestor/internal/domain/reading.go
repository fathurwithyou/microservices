package domain

import "time"

// Source identifies the feed a reading or quarantined message came from.
type Source string

// SourceCoastalSensor identifies readings produced by coastal sensor feeds.
const SourceCoastalSensor Source = "SENSOR_PANTAI"

// Severity is the alert level carried by a coastal reading.
type Severity string

const (
	SeverityNormal  Severity = "NORMAL"
	SeverityWaspada Severity = "WASPADA"
	SeveritySiaga   Severity = "SIAGA"
	SeverityAwas    Severity = "AWAS"
)

// Valid reports whether the severity is one the ingestor accepts.
func (s Severity) Valid() bool {
	switch s {
	case SeverityNormal, SeverityWaspada, SeveritySiaga, SeverityAwas:
		return true
	default:
		return false
	}
}

// Reading is one raw coastal sensor message before validation.
type Reading struct {
	ID       string
	SensorID string
	AreaName string
	Quality  string
	// OccurredAt is observed_at on the sensor's own clock.
	OccurredAt time.Time
	// ReceivedAt is the ingestor wall clock right after the HTTP response
	// carrying this reading was read (one value per page). For a critical
	// reading it is the first receipt of that observation, so a repeated
	// read of the same stream does not look like a new event downstream.
	ReceivedAt time.Time
	// GeneratedAt is generated_at of the page: the sensor's "now". Zero when
	// the page did not carry a usable value.
	GeneratedAt time.Time
	Latitude    float64
	Longitude   float64
	// WaveHeightM and SeaLevelAnomalyM are valid only when HasMeasurements.
	WaveHeightM      float64
	SeaLevelAnomalyM float64
	HasMeasurements  bool
	Severity         Severity
	Critical         bool
	// IdentityViolation is set by the source adapter when the item could not
	// be decoded far enough to know which observation it is (not an object,
	// wrong type or absence of observation_id, station_id, observed_at).
	// Such a reading is always quarantined.
	IdentityViolation QuarantineReason
	// ContentViolation is set by the source adapter when the identity is
	// intact but another contract field is unusable (quality, measurements,
	// critical). It quarantines a normal reading; a critical one is still
	// forwarded, see Validator.
	ContentViolation QuarantineReason
	// Note explains an accepted-with-reservations critical reading; the sink
	// forwards it as the validation_note attribute.
	Note string
	// Payload is the raw item exactly as the sensor sent it.
	Payload []byte
}

// FeedStatus is what the last fetch learned about the feed as a whole.
type FeedStatus struct {
	// GeneratedAt is the newest sensor clock seen.
	GeneratedAt time.Time
	// NewestObservedAt is the newest plausible observed_at seen since the
	// sensor stream last (re)started; zero when none was seen yet.
	NewestObservedAt time.Time
	// AtTail reports that the last fetch reached the end of the stream
	// (next_cursor null), so GeneratedAt minus NewestObservedAt is the age
	// of the newest data the sensor has.
	AtTail bool
}
