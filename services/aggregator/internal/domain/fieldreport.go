package domain

import (
	"errors"
	"time"
)

// FieldReportStatus tracks a field report from durable acceptance through
// asynchronous processing.
type FieldReportStatus string

const (
	FieldReportAccepted   FieldReportStatus = "accepted"
	FieldReportProcessing FieldReportStatus = "processing"
	FieldReportProcessed  FieldReportStatus = "processed"
	FieldReportFailed     FieldReportStatus = "failed"
)

// FieldReport is the durable inbox record for a Tim Lapangan submission. It is
// intentionally separate from HazardEvent until the public contract defines a
// source value for field reports.
type FieldReport struct {
	ReportID    string
	HazardType  HazardType
	Severity    Severity
	AreaName    string
	Latitude    float64
	Longitude   float64
	OccurredAt  time.Time
	ReceivedAt  time.Time
	Description string
	Status      FieldReportStatus
	Attempts    int
	ProcessedAt *time.Time
}

var ErrInvalidFieldReport = errors.New("invalid field report")
