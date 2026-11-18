package domain

import "time"

type Severity string

const (
	SeverityWaspada Severity = "WASPADA"
	SeveritySiaga   Severity = "SIAGA"
	SeverityAwas    Severity = "AWAS"
)

const (
	StatusActive    = "active"
	StatusRetracted = "retracted"
	// StatusClosed marks a fire whose detections are all older than the
	// accepted lateness plus the linking window, so no admitted detection
	// can change it any more. It is a state of the ingestor only: the
	// HazardEvent keeps the last active content.
	StatusClosed = "closed"
)

// Fire is a cluster of at least two connected detections.
type Fire struct {
	SourceRefID    string
	Status         string
	OccurredAt     time.Time
	AreaName       string
	Latitude       float64
	Longitude      float64
	DetectionCount int
	Satellites     []Satellite
	MaxConfidence  Confidence
	TotalFRP       float64
	DetectionKeys  []string
	Severity       Severity
}

// SeverityFor grades a fire by detection count and total fire radiative power.
func SeverityFor(detectionCount int, totalFRP float64) Severity {
	switch {
	case detectionCount >= 20 || totalFRP >= 100:
		return SeverityAwas
	case detectionCount >= 6 || totalFRP >= 30:
		return SeveritySiaga
	default:
		return SeverityWaspada
	}
}
