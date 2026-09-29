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
	Hazards       []Hazard
	NextPageToken string
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
}
