package domain

import "time"

// QuarantineReason is the typed cause for rejecting a reading.
type QuarantineReason string

const (
	ReasonMissingID        QuarantineReason = "missing_id"
	ReasonMissingSensorID  QuarantineReason = "missing_sensor_id"
	ReasonInvalidLatitude  QuarantineReason = "invalid_latitude"
	ReasonInvalidLongitude QuarantineReason = "invalid_longitude"
	ReasonOccurredAtZero   QuarantineReason = "occurred_at_zero"
	ReasonOccurredAtFuture QuarantineReason = "occurred_at_future"
	ReasonOccurredAtStale  QuarantineReason = "occurred_at_stale"
	ReasonDuplicateID      QuarantineReason = "duplicate_id"
	ReasonInvalidSeverity  QuarantineReason = "invalid_severity"
)

// Retryable reports whether redelivering the same message later can succeed.
// A future timestamp becomes valid as time passes; the remaining reasons are
// permanent for this payload.
func (r QuarantineReason) Retryable() bool {
	return r == ReasonOccurredAtFuture
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
