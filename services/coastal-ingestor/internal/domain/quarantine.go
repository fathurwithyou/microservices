package domain

import "time"

// QuarantineReason is the typed cause for rejecting a reading.
type QuarantineReason string

const (
	ReasonMissingID        QuarantineReason = "missing_id"
	ReasonMissingSensorID  QuarantineReason = "missing_sensor_id"
	ReasonUnknownStation   QuarantineReason = "unknown_station"
	ReasonQualityUnusable  QuarantineReason = "quality_unusable"
	ReasonQualityRejected  QuarantineReason = "quality_rejected"
	ReasonInvalidLatitude  QuarantineReason = "invalid_latitude"
	ReasonInvalidLongitude QuarantineReason = "invalid_longitude"
	ReasonOccurredAtZero   QuarantineReason = "occurred_at_zero"
	ReasonOccurredAtFuture QuarantineReason = "occurred_at_future"
	// ReasonOccurredAtStale is no longer produced: the sensor runs on its own
	// virtual clock, so an old observed_at says nothing about one reading.
	// Staleness is a feed-level condition (coastal_feed_freshness_seconds).
	// The constant stays for records written by earlier versions.
	ReasonOccurredAtStale QuarantineReason = "occurred_at_stale"
	ReasonDuplicateID     QuarantineReason = "duplicate_id"
	ReasonInvalidSeverity QuarantineReason = "invalid_severity"

	// Reasons found while decoding one item of a sensor page. One broken
	// item is set aside on its own; it never fails the page it arrived in.

	// ReasonMalformedItem marks an item that is not a JSON object (a string,
	// number, null, array) or a page whose "items" is not an array.
	ReasonMalformedItem QuarantineReason = "malformed_item"
	// ReasonOversizedItem marks an item larger than the per-item cap; only
	// its leading bytes are kept as the quarantine payload.
	ReasonOversizedItem QuarantineReason = "oversized_item"
	// ReasonInvalidFieldType marks a contract field of the wrong JSON type
	// (for example "wave_height_m":"abc" or a numeric observation_id).
	ReasonInvalidFieldType QuarantineReason = "invalid_field_type"
	// ReasonMissingObservedAt marks an absent, null, or empty observed_at.
	ReasonMissingObservedAt QuarantineReason = "missing_observed_at"
	// ReasonInvalidObservedAt marks an observed_at that is not RFC 3339 or
	// lies outside the years the pipeline can represent.
	ReasonInvalidObservedAt QuarantineReason = "invalid_observed_at"
	// ReasonMissingMeasurement marks an absent or null wave_height_m or
	// sea_level_anomaly_m.
	ReasonMissingMeasurement QuarantineReason = "missing_measurement"
	// ReasonInvalidMeasurement marks a non-finite measurement or one outside
	// the physical bounds in MeasurementBounds.
	ReasonInvalidMeasurement QuarantineReason = "invalid_measurement"
)

// Physical bounds for a measurement to be believable. They are deliberately
// generous: the largest instrumentally recorded waves are near 30 m and the
// largest tsunami sea-level anomalies at a tide gauge near 10-20 m, so anything
// outside these ranges is a sensor fault, not an observation.
const (
	MinWaveHeightM      = 0.0
	MaxWaveHeightM      = 50.0
	MinSeaLevelAnomalyM = -30.0
	MaxSeaLevelAnomalyM = 30.0
)

// Retryable reports whether redelivering the same message later can succeed.
// A future timestamp becomes valid as time passes, an unavailable sensor may
// recover, and the remaining reasons are permanent for this payload.
func (r QuarantineReason) Retryable() bool {
	return r == ReasonOccurredAtFuture || r == ReasonQualityUnusable
}

// QuarantineRecord is the durable form of a rejected reading.
type QuarantineRecord struct {
	MessageID  string
	Source     Source
	ReceivedAt time.Time
	Reason     QuarantineReason
	Retryable  bool
	Payload    []byte
}
