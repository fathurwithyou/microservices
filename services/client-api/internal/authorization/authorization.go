package authorization

import "slices"

const (
	SummaryScope = "hazard:read:summary"
	RawScope     = "hazard:read:raw"
)

func Has(scopes []string, required string) bool { return slices.Contains(scopes, required) }
