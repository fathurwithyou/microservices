package mapping

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/google/uuid"
)

func Volcanic(raw map[string]json.RawMessage, volcanoes map[string]domain.VolcanoReference, now time.Time) (domain.HazardEvent, error) {
	reportID, err := stringField(raw, "report_id")
	if err != nil {
		return domain.HazardEvent{}, err
	}
	volcanoID, err := stringField(raw, "volcano_id")
	if err != nil {
		return domain.HazardEvent{}, err
	}
	alertLevel, err := stringField(raw, "alert_level")
	if err != nil {
		return domain.HazardEvent{}, err
	}
	severity, ok := domain.SeverityFromAlert(alertLevel)
	if !ok {
		return domain.HazardEvent{}, fmt.Errorf("unknown alert_level %q: %w", alertLevel, domain.ErrInvalidSourcePayload)
	}
	eruptionCount, err := numberField(raw, "eruption_count_24h")
	if err != nil {
		return domain.HazardEvent{}, err
	}
	ashColumnHeight, err := numberField(raw, "ash_column_height_m")
	if err != nil {
		return domain.HazardEvent{}, err
	}
	reportedAt, err := timeField(raw, "reported_at")
	if err != nil {
		return domain.HazardEvent{}, err
	}

	reference := volcanoes[volcanoID]
	attributes := additionalAttributes(raw, volcanicCanonicalFields)
	attributes["eruption_count_24h"] = eruptionCount
	attributes["ash_column_height_m"] = ashColumnHeight
	return domain.HazardEvent{HazardID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("PVMBG:"+reportID)).String(), Source: domain.SourcePVMBG, SourceRefID: reportID, HazardType: domain.HazardVolcanic, Severity: severity, AreaName: reference.Name, Latitude: reference.Latitude, Longitude: reference.Longitude, OccurredAt: reportedAt, IngestedAt: now, Attributes: attributes}, nil
}

var volcanicCanonicalFields = fieldSet("report_id", "volcano_id", "alert_level", "reported_at")
