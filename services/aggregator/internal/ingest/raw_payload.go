package ingest

import "encoding/json"

func rawString(raw map[string]json.RawMessage, key string) string {
	var value string
	data, ok := raw[key]
	if !ok || json.Unmarshal(data, &value) != nil {
		return ""
	}
	return value
}
