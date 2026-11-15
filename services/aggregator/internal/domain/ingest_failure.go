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
	// Reason is why the message first failed. It never changes, so an
	// operator always sees the original cause.
	Reason string
	// LastError is the outcome of the most recent automatic retry, if any.
	LastError string
	Retryable bool
	Attempts  int
	Status    FailureStatus
	// Payload holds the raw bytes. Listings leave it empty and report only
	// PayloadBytes; load one message to read the bytes.
	Payload      []byte
	PayloadBytes int
}

// FailureFilter selects failures for a listing, oldest first.
type FailureFilter struct {
	// Source limits the listing to one feed; empty means all feeds.
	Source Source
	// Status limits the listing to one state; empty means parked or retrying.
	Status FailureStatus
	Limit  int
	// After and AfterID resume after the last row of a previous page.
	After   time.Time
	AfterID string
}
