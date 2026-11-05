package ports

import (
	"context"
	"time"
)

// TokenPair is the client-api-owned view of a pair issued by the auth service.
type TokenPair struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
	ClientID     string    `json:"client_id"`
	Scopes       []string  `json:"scopes"`
}

// ValidatedToken is the auth service result needed to authenticate an HTTP request.
type ValidatedToken struct {
	Valid    bool
	ClientID string
	Scopes   []string
}

// HazardFilter contains the client-api filters forwarded to the aggregator.
type HazardFilter struct {
	Source     string
	HazardType string
	Severity   string
	PageSize   int32
	PageToken  string
	Raw        bool
}

// HazardPage is a page of normalized hazard events.
type HazardPage struct {
	Hazards          []Hazard
	NextPageToken    string
	Partial          bool
	UnavailableUnits []string
}

// Hazard is the client-api-owned representation of an aggregator event.
type Hazard struct {
	HazardID    string
	Source      string
	SourceRefID string
	HazardType  string
	Severity    string
	AreaName    string
	Latitude    float64
	Longitude   float64
	OccurredAt  string
	IngestedAt  string
	Attributes  map[string]string
}

// SourceStatus is the client-api-owned representation of feed health.
type SourceStatus struct {
	Source        string `json:"source,omitempty"`
	Status        string `json:"status"`
	LastSuccessAt string `json:"last_success_at,omitempty"`
	StaleSince    string `json:"stale_since,omitempty"`
	LastError     string `json:"last_error,omitempty"`
}

// FieldReportInput is a Tim Lapangan submission forwarded to the aggregator.
type FieldReportInput struct {
	ReportID    string  `json:"report_id" binding:"required"`
	HazardType  string  `json:"hazard_type" binding:"required"`
	Severity    string  `json:"severity" binding:"required"`
	AreaName    string  `json:"area_name" binding:"required"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
	OccurredAt  string  `json:"occurred_at" binding:"required"`
	Description string  `json:"description"`
}

// FieldReport is the accepted submission returned by the aggregator.
type FieldReport struct {
	ReportID string `json:"report_id"`
	Status   string `json:"status"`
}

// AnalyticsFilter selects the dashboard aggregation window.
type AnalyticsFilter struct {
	Since  string
	Until  string
	Bucket string
}

// Analytics is the client-api view of dashboard aggregates.
type Analytics struct {
	HazardTypes      []AnalyticsTypeCount `json:"hazard_types"`
	Severities       []AnalyticsSeverity  `json:"severities"`
	Areas            []AnalyticsArea      `json:"areas"`
	Buckets          []AnalyticsBucket    `json:"time_buckets"`
	Partial          bool                 `json:"partial"`
	UnavailableUnits []string             `json:"unavailable_units"`
}

type AnalyticsTypeCount struct {
	HazardType string `json:"hazard_type"`
	Count      int64  `json:"count"`
}
type AnalyticsSeverity struct {
	Severity string `json:"severity"`
	Count    int64  `json:"count"`
}
type AnalyticsArea struct {
	AreaName string `json:"area_name"`
	Count    int64  `json:"count"`
}
type AnalyticsBucket struct {
	BucketStart string `json:"bucket_start"`
	HazardType  string `json:"hazard_type"`
	Count       int64  `json:"count"`
}

// Auth is the outbound capability required from the auth service.
type Auth interface {
	Issue(context.Context, string, string) (*TokenPair, error)
	Refresh(context.Context, string) (*TokenPair, error)
	Validate(context.Context, string) (*ValidatedToken, error)
}

// Aggregator is the outbound capability required from the hazard aggregator.
type Aggregator interface {
	List(context.Context, HazardFilter) (HazardPage, error)
	GetHazard(context.Context, string, bool) (Hazard, error)
	Status(context.Context, string) (SourceStatus, error)
	SubmitFieldReport(context.Context, FieldReportInput) (FieldReport, error)
	Analytics(context.Context, AnalyticsFilter) (Analytics, error)
}
