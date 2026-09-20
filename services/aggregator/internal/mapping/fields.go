package mapping

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type fields map[string]struct{}

func fieldSet(names ...string) fields {
	result := make(fields, len(names))
	for _, name := range names {
		result[name] = struct{}{}
	}
	return result
}
func additionalAttributes(raw map[string]json.RawMessage, known fields) map[string]any {
	result := make(map[string]any)
	for key, value := range raw {
		if _, ok := known[key]; !ok {
			var decoded any
			if json.Unmarshal(value, &decoded) == nil {
				result[key] = decoded
			}
		}
	}
	return result
}
func stringField(raw map[string]json.RawMessage, name string) (string, error) {
	var value string
	if data, ok := raw[name]; !ok || json.Unmarshal(data, &value) != nil || value == "" {
		return "", fmt.Errorf("missing or invalid %s: %w", name, domain.ErrInvalidSourcePayload)
	}
	return value, nil
}
func numberField(raw map[string]json.RawMessage, name string) (float64, error) {
	var value float64
	if data, ok := raw[name]; !ok || json.Unmarshal(data, &value) != nil {
		return 0, fmt.Errorf("missing or invalid %s: %w", name, domain.ErrInvalidSourcePayload)
	}
	return value, nil
}
func boolField(raw map[string]json.RawMessage, name string) (bool, error) {
	var value bool
	if data, ok := raw[name]; !ok || json.Unmarshal(data, &value) != nil {
		return false, fmt.Errorf("missing or invalid %s: %w", name, domain.ErrInvalidSourcePayload)
	}
	return value, nil
}
func timeField(raw map[string]json.RawMessage, name string) (time.Time, error) {
	value, err := stringField(raw, name)
	if err != nil {
		return time.Time{}, err
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid %s: %w", name, domain.ErrInvalidSourcePayload)
	}
	return parsed, nil
}
func stringSliceField(raw map[string]json.RawMessage, name string) ([]string, error) {
	var value []string
	if data, ok := raw[name]; !ok || json.Unmarshal(data, &value) != nil {
		return nil, domain.ErrInvalidSourcePayload
	}
	return value, nil
}
