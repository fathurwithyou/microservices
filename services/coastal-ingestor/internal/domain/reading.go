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
	ID         string
	SensorID   string
	OccurredAt time.Time
	ReceivedAt time.Time
	Latitude   float64
	Longitude  float64
	Severity   Severity
	Critical   bool
	Payload    []byte
}
