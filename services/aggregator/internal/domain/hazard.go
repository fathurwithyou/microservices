package domain

import (
	"errors"
	"time"
)

type Source string

const (
	SourceBMKG  Source = "BMKG"
	SourcePVMBG Source = "PVMBG"
)

type HazardType string

const (
	HazardSeismic  HazardType = "SEISMIC"
	HazardVolcanic HazardType = "VOLCANIC"
)

type Severity string

const (
	SeverityNormal  Severity = "NORMAL"
	SeverityWaspada Severity = "WASPADA"
	SeveritySiaga   Severity = "SIAGA"
	SeverityAwas    Severity = "AWAS"
)

type HazardEvent struct {
	HazardID    string
	Source      Source
	SourceRefID string
	HazardType  HazardType
	Severity    Severity
	AreaName    string
	Latitude    float64
	Longitude   float64
	OccurredAt  time.Time
	IngestedAt  time.Time
	Attributes  map[string]any
}

type VolcanoReference struct {
	Name                string
	Latitude, Longitude float64
}

type SourceStatus struct {
	Source        Source
	Status        string
	LastSuccessAt *time.Time
	StaleSince    *time.Time
	LastError     string
}

var (
	ErrInvalidSourcePayload = errors.New("invalid source payload")
	ErrNotFound             = errors.New("hazard not found")
)

func SeverityFromMagnitude(magnitude float64) Severity {
	switch {
	case magnitude < 5:
		return SeverityNormal
	case magnitude < 6.5:
		return SeverityWaspada
	default:
		return SeveritySiaga
	}
}

func SeverityFromThreat(threat string, magnitude float64) Severity {
	switch threat {
	case "Awas":
		return SeverityAwas
	case "Siaga":
		return SeveritySiaga
	case "Waspada":
		return SeverityWaspada
	default:
		return SeverityFromMagnitude(magnitude)
	}
}

func SeverityFromAlert(alert string) (Severity, bool) {
	switch alert {
	case "Normal":
		return SeverityNormal, true
	case "Waspada":
		return SeverityWaspada, true
	case "Siaga":
		return SeveritySiaga, true
	case "Awas":
		return SeverityAwas, true
	default:
		return "", false
	}
}
