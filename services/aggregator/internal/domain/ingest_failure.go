package domain

import "time"

// FailureStatus classifies a payload that could not be mapped.
type FailureStatus string

const (
	FailureRetrying  FailureStatus = "retrying"
	FailureParked    FailureStatus = "parked"
	FailureRecovered FailureStatus = "recovered"
)

// IngestFailure is a raw source payload kept for inspection and replay.
type IngestFailure struct {
	MessageID  string
	Source     Source
	ReceivedAt time.Time
	Reason     string
	Retryable  bool
	Attempts   int
	Status     FailureStatus
	Payload    []byte
}
