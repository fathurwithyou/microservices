// Package domain defines the PVMBG volcanic report shape. Field names and
// JSON tags are part of the mock contract and must not change: the
// aggregator maps these exact names.
package domain

// SchemaV2ConfidenceLevel is the confidence value injected into reports
// when schema version 2 is enabled via the admin endpoint.
const SchemaV2ConfidenceLevel = 0.82

// VolcanicReport is one volcanic activity report. ConfidenceLevel is only
// present when schemaV2 is enabled.
type VolcanicReport struct {
	ReportID         string   `json:"report_id"`
	VolcanoID        string   `json:"volcano_id"`
	AlertLevel       string   `json:"alert_level"`
	EruptionCount24H float64  `json:"eruption_count_24h"`
	AshColumnHeightM float64  `json:"ash_column_height_m"`
	ReportedAt       string   `json:"reported_at"`
	ConfidenceLevel  *float64 `json:"confidence_level,omitempty"`
}
