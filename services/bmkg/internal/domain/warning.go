package domain

type TsunamiWarning struct {
	WarningID        string   `json:"warning_id"`
	RelatedEventID   string   `json:"related_event_id"`
	ThreatLevel      string   `json:"threat_level"`
	AffectedZones    []string `json:"affected_zones"`
	EstimatedArrival string   `json:"estimated_arrival"`
}
