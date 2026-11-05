// Package importer reads a historical earthquake catalog and maps it through
// the same seismic rules used by live BMKG ingestion.
package importer

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/mapping"
)

// maxCatalogBytes bounds one import file.
const maxCatalogBytes = 512 << 20

// Read accepts a JSON array or newline-delimited JSON objects. Each object
// uses the BMKG field names consumed by mapping.Seismic.
func Read(reader io.Reader) ([]map[string]json.RawMessage, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxCatalogBytes))
	if err != nil {
		return nil, err
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, nil
	}
	if trimmed[0] == '[' {
		var items []map[string]json.RawMessage
		if err := json.Unmarshal(trimmed, &items); err != nil {
			return nil, fmt.Errorf("decode catalog array: %w", err)
		}
		return items, nil
	}

	var items []map[string]json.RawMessage
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 64*1024), 16<<20)
	line := 0
	for scanner.Scan() {
		line++
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			continue
		}
		var item map[string]json.RawMessage
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, fmt.Errorf("decode catalog line %d: %w", line, err)
		}
		items = append(items, item)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read catalog: %w", err)
	}
	return items, nil
}

// Events maps catalog rows through the live BMKG rules. Rows that cannot be
// mapped are returned separately so the command can report them and continue.
func Events(rows []map[string]json.RawMessage, now time.Time) ([]domain.HazardEvent, []error) {
	events := make([]domain.HazardEvent, 0, len(rows))
	var failures []error
	for index, row := range rows {
		event, err := mapping.Seismic(row, nil, now)
		if err != nil {
			failures = append(failures, fmt.Errorf("row %d: %w", index+1, err))
			continue
		}
		events = append(events, event)
	}
	return events, failures
}
