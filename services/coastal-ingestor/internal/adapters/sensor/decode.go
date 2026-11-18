package sensor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
)

// looksCritical matches the raw bytes of an item that could not be decoded but
// still spells out a truthy critical flag (possibly inside an escaped string).
// Such an item is quarantined like any other undecodable one, but flagged, so
// the ingestor counts and logs it as a critical reading that did not get
// through instead of burying it among ordinary rejects.
var looksCritical = regexp.MustCompile(`(?i)critical\\?"\s*:\s*(\\?")?(true|yes|1)\b`)

type fieldState int

const (
	fieldAbsent fieldState = iota // missing or null
	fieldOK
	fieldWrongType
)

// decodeItem turns one element of "items" into a Reading. It never fails:
// whatever is wrong with the element is recorded on the reading as an
// IdentityViolation (cannot tell which observation this is) or a
// ContentViolation (identity fine, another contract field unusable), and the
// validator decides. Unknown extra fields are ignored. The raw element is kept
// as the payload so a quarantine record shows exactly what the sensor sent.
func (c *Client) decodeItem(raw rawItem, from page) domain.Reading {
	reading := domain.Reading{
		ReceivedAt:  from.receivedAt,
		GeneratedAt: from.generated,
		Severity:    domain.SeverityNormal,
		Payload:     raw.data,
	}
	var fields map[string]json.RawMessage
	switch {
	case raw.violation != "":
		reading.IdentityViolation = raw.violation
	// JSON is UTF-8 by definition. Go would silently replace invalid bytes,
	// which could turn two different broken IDs into the same one.
	case len(raw.data) == 0 || raw.data[0] != '{' || !utf8.Valid(raw.data) || json.Unmarshal(raw.data, &fields) != nil:
		reading.IdentityViolation = domain.ReasonMalformedItem
	}
	if reading.IdentityViolation != "" {
		if looksCritical.Match(raw.data) {
			reading.Critical = true
			reading.Severity = domain.SeverityAwas
		}
		return reading
	}

	content := func(reason domain.QuarantineReason) {
		if reading.ContentViolation == "" {
			reading.ContentViolation = reason
		}
	}

	var state fieldState
	if reading.ID, state = stringField(fields, "observation_id"); state == fieldWrongType {
		reading.IdentityViolation = domain.ReasonInvalidFieldType
	}
	if reading.SensorID, state = stringField(fields, "station_id"); state == fieldWrongType && reading.IdentityViolation == "" {
		reading.IdentityViolation = domain.ReasonInvalidFieldType
	}
	observedAt, state := stringField(fields, "observed_at")
	switch {
	// A missing ID or station is reported as such by the validator; the
	// timestamp of an unidentifiable item is not worth a second opinion.
	case reading.IdentityViolation != "" || strings.TrimSpace(reading.ID) == "" || strings.TrimSpace(reading.SensorID) == "":
	case state == fieldWrongType:
		reading.IdentityViolation = domain.ReasonInvalidFieldType
	case state == fieldAbsent || strings.TrimSpace(observedAt) == "":
		reading.IdentityViolation = domain.ReasonMissingObservedAt
	default:
		parsed, err := time.Parse(time.RFC3339, observedAt)
		if err != nil {
			reading.IdentityViolation = domain.ReasonInvalidObservedAt
		} else {
			reading.OccurredAt = parsed
		}
	}

	if reading.Quality, state = stringField(fields, "quality"); state == fieldWrongType {
		reading.Quality = ""
		content(domain.ReasonInvalidFieldType)
	}

	wave, waveViolation := numberField(fields, "wave_height_m")
	anomaly, anomalyViolation := numberField(fields, "sea_level_anomaly_m")
	switch {
	case waveViolation != "":
		content(waveViolation)
	case anomalyViolation != "":
		content(anomalyViolation)
	default:
		reading.WaveHeightM, reading.SeaLevelAnomalyM, reading.HasMeasurements = wave, anomaly, true
	}

	critical, state := criticalField(fields)
	if state == fieldWrongType {
		content(domain.ReasonInvalidFieldType)
	}
	if critical {
		reading.Critical = true
		reading.Severity = domain.SeverityAwas
	}

	c.locate(&reading)
	return reading
}

// locate fills area and coordinates from the station registry. An unknown
// station leaves AreaName empty, which the validator reports.
func (c *Client) locate(reading *domain.Reading) {
	located, ok := c.stations[reading.SensorID]
	if !ok {
		return
	}
	reading.AreaName = located.Province
	reading.Latitude = located.Latitude
	reading.Longitude = located.Longitude
}

// dedupeKey identifies a reading within the tail page: its observation ID, or
// a digest of its bytes when it has none (otherwise an ID-less broken item
// sitting on the tail would be quarantined again on every poll).
func dedupeKey(reading domain.Reading) string {
	if reading.ID != "" && reading.IdentityViolation == "" {
		return "id:" + reading.ID
	}
	return "raw:" + digest(reading.Payload)
}

// stringField reads a contract string. For a scalar of the wrong type the raw
// text is returned (shortened) so the quarantine record can still be found by
// what the sensor put there.
func stringField(fields map[string]json.RawMessage, name string) (string, fieldState) {
	raw, ok := fields[name]
	if !ok || string(raw) == "null" {
		return "", fieldAbsent
	}
	if raw[0] == '"' {
		var value string
		if json.Unmarshal(raw, &value) != nil {
			return "", fieldWrongType
		}
		return value, fieldOK
	}
	if raw[0] == '{' || raw[0] == '[' {
		return "", fieldWrongType
	}
	const limit = 64
	if len(raw) > limit {
		raw = raw[:limit]
	}
	return string(raw), fieldWrongType
}

// numberField reads a contract measurement and reports what is wrong with it.
func numberField(fields map[string]json.RawMessage, name string) (float64, domain.QuarantineReason) {
	raw, ok := fields[name]
	if !ok || string(raw) == "null" {
		return 0, domain.ReasonMissingMeasurement
	}
	if raw[0] != '-' && (raw[0] < '0' || raw[0] > '9') {
		return 0, domain.ReasonInvalidFieldType
	}
	var value float64
	if json.Unmarshal(raw, &value) != nil {
		// A JSON number float64 cannot hold (1e999).
		return 0, domain.ReasonInvalidMeasurement
	}
	return value, ""
}

// criticalField reads the critical flag. The contract says boolean; a missing
// flag means not critical. A flag of the wrong type is a contract violation,
// but it is read in the direction that cannot lose a warning: any string or
// number that is not plainly "false" counts as critical.
func criticalField(fields map[string]json.RawMessage) (bool, fieldState) {
	raw, ok := fields["critical"]
	if !ok || string(raw) == "null" {
		return false, fieldAbsent
	}
	switch string(raw) {
	case "true":
		return true, fieldOK
	case "false":
		return false, fieldOK
	}
	switch {
	case raw[0] == '"':
		var value string
		_ = json.Unmarshal(raw, &value)
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "", "false", "no", "n", "0":
			return false, fieldWrongType
		}
		return true, fieldWrongType
	case raw[0] == '-' || (raw[0] >= '0' && raw[0] <= '9'):
		var value float64
		if json.Unmarshal(raw, &value) == nil && value == 0 {
			return false, fieldWrongType
		}
		return true, fieldWrongType
	}
	return false, fieldWrongType
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:8])
}
