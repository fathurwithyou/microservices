// Package scopes defines the authorization scope vocabulary shared between
// the auth service and its clients. These values are part of the service
// contract, not business logic: each service still enforces its own policy.
package scopes

const (
	// HazardReadSummary permits reading summarized hazard data.
	HazardReadSummary = "hazard:read:summary"
	// HazardReadRaw permits reading raw hazard payloads.
	HazardReadRaw = "hazard:read:raw"
)
