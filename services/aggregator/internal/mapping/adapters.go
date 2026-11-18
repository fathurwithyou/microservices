package mapping

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// SchemaAdapters are operator-enabled translations for partner schema
// changes that the default policy refuses. Messages that arrived before an
// adapter was enabled stay parked until an Ops replay maps them again.
//
// Default policy (M2 P3.1):
//   - accepted automatically: new optional fields (kept in attributes),
//     field order, unknown extra fields;
//   - refused (parked, never guessed): renamed fields, changed types, and
//     missing required fields.
type SchemaAdapters struct {
	// PVMBGBreakingV3 understands the PVMBG "breaking" schema: alert_level
	// renamed to level, eruption_count_24h sent as a numeric string, and
	// reported_at omitted. The missing time is taken from when BNPB received
	// the message and the event is marked with reported_at_source.
	PVMBGBreakingV3 bool
}

var adapters atomic.Value

func init() { adapters.Store(SchemaAdapters{}) }

// SetSchemaAdapters replaces the enabled adapters.
func SetSchemaAdapters(value SchemaAdapters) { adapters.Store(value) }

// CurrentSchemaAdapters returns the enabled adapters.
func CurrentSchemaAdapters() SchemaAdapters { return adapters.Load().(SchemaAdapters) }

// ParseSchemaAdapters reads a comma-separated list, e.g. "pvmbg-breaking-v3".
func ParseSchemaAdapters(value string) (SchemaAdapters, []string) {
	var result SchemaAdapters
	var unknown []string
	for _, name := range strings.Split(value, ",") {
		switch name = strings.TrimSpace(name); name {
		case "":
		case "pvmbg-breaking-v3":
			result.PVMBGBreakingV3 = true
		default:
			unknown = append(unknown, name)
		}
	}
	return result, unknown
}

// adaptPVMBGBreakingV3 returns a copy of raw in the canonical v1 shape, or raw
// itself when the record is not in the breaking shape. It only fills what
// the breaking schema removed or renamed; anything else still has to pass
// the normal mapping checks.
func adaptPVMBGBreakingV3(raw map[string]json.RawMessage, receivedAt time.Time) (map[string]json.RawMessage, bool) {
	_, hasAlert := raw["alert_level"]
	level, hasLevel := raw["level"]
	count := raw["eruption_count_24h"]
	_, hasReported := raw["reported_at"]
	countIsString := len(count) > 0 && count[0] == '"'
	if hasAlert && hasReported && !countIsString {
		return raw, false
	}
	adapted := make(map[string]json.RawMessage, len(raw)+2)
	for key, value := range raw {
		adapted[key] = value
	}
	if !hasAlert && hasLevel {
		adapted["alert_level"] = level
		delete(adapted, "level")
	}
	if countIsString {
		var text string
		if json.Unmarshal(count, &text) == nil {
			if number, err := strconv.ParseFloat(strings.TrimSpace(text), 64); err == nil {
				adapted["eruption_count_24h"] = json.RawMessage(strconv.FormatFloat(number, 'f', -1, 64))
			}
		}
	}
	if !hasReported && !receivedAt.IsZero() {
		stamp, _ := json.Marshal(receivedAt.UTC().Format(time.RFC3339))
		adapted["reported_at"] = stamp
		adapted["reported_at_source"] = json.RawMessage(`"received_at"`)
	}
	adapted["schema_adapter"] = json.RawMessage(`"pvmbg-breaking-v3"`)
	return adapted, true
}
