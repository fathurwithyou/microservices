package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/pvmbg/internal/domain"
	"github.com/fathurwithyou/microservices/services/pvmbg/internal/seed"
	"github.com/google/uuid"
)

// Runtime is the in-memory store for volcanic reports and simulation flags.
type Runtime struct {
	mu       sync.RWMutex
	outage   bool
	schemaV2 bool
	breaking bool
	// malformed is the transport corruption served for the next
	// malformedLeft responses; empty means well-formed JSON.
	malformed     MalformedKind
	malformedLeft int
	minDelay      time.Duration
	maxDelay      time.Duration
	now           func() time.Time
	reports       []domain.VolcanicReport
}

// Config configures a Runtime.
type Config struct {
	// MinDelay and MaxDelay bound the artificial response delay.
	MinDelay time.Duration
	MaxDelay time.Duration
	// SeedCount is the number of historical reports seeded at startup.
	SeedCount int
	// Now supplies the current time; defaults to UTC now. Injectable for
	// deterministic tests.
	Now func() time.Time
}

// New validates cfg and returns a Runtime seeded with historical reports.
func New(cfg Config) (*Runtime, error) {
	if cfg.SeedCount < 0 {
		return nil, errors.New("state: seed count must not be negative")
	}
	now := cfg.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Runtime{
		minDelay: cfg.MinDelay,
		maxDelay: cfg.MaxDelay,
		now:      now,
		reports:  seed.InitialReports(now(), cfg.SeedCount),
	}, nil
}

// AppendReport appends one synthetic runtime report.
func (r *Runtime) AppendReport() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.reports = append(r.reports, domain.VolcanicReport{
		ReportID:         "pvmbg-report-" + uuid.NewString(),
		VolcanoID:        "merapi",
		AlertLevel:       "Waspada",
		EruptionCount24H: 1,
		AshColumnHeightM: 350,
		ReportedAt:       r.now().Format(time.RFC3339),
	})
}

// ReportsSince returns copies of reports observed at or after since,
// injecting the schemaV2 confidence level when enabled.
func (r *Runtime) ReportsSince(since time.Time) []domain.VolcanicReport {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]domain.VolcanicReport, 0)
	for _, report := range r.reports {
		reportedAt, err := time.Parse(time.RFC3339, report.ReportedAt)
		if err != nil || reportedAt.Before(since) {
			continue
		}
		if r.schemaV2 {
			confidence := domain.SchemaV2ConfidenceLevel
			report.ConfidenceLevel = &confidence
		}
		result = append(result, report)
	}
	return result
}

// IsOutage reports whether outage simulation is active.
func (r *Runtime) IsOutage() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.outage
}

// SetOutage toggles outage simulation.
func (r *Runtime) SetOutage(value bool) {
	r.mu.Lock()
	r.outage = value
	r.mu.Unlock()
}

// SetSchemaV2 toggles schema version 2 simulation. Turning it off returns the
// feed to the baseline v1 schema, clearing breaking and malformed modes too.
func (r *Runtime) SetSchemaV2(value bool) {
	r.mu.Lock()
	r.schemaV2 = value
	r.breaking = false
	r.malformed = ""
	r.malformedLeft = 0
	r.mu.Unlock()
}

// Reset restores the baseline v1 schema with well-formed responses.
func (r *Runtime) Reset() {
	r.SetSchemaV2(false)
}

// SetBreaking toggles an incompatible schema simulation.
func (r *Runtime) SetBreaking(value bool) {
	r.mu.Lock()
	r.breaking = value
	if value {
		r.schemaV2 = false
		r.malformed = ""
		r.malformedLeft = 0
	}
	r.mu.Unlock()
}

// IsBreaking reports whether incompatible schema mode is active.
func (r *Runtime) IsBreaking() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.breaking
}

// BreakingReportsSince returns reports with renamed, retyped, and missing
// fields so the aggregator must park them.
func (r *Runtime) BreakingReportsSince(since time.Time) []map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]map[string]any, 0)
	for _, report := range r.reports {
		reportedAt, err := time.Parse(time.RFC3339, report.ReportedAt)
		if err != nil || reportedAt.Before(since) {
			continue
		}
		result = append(result, map[string]any{
			"report_id":              report.ReportID,
			"volcano_id":             report.VolcanoID,
			"level":                  report.AlertLevel,
			"eruption_count_24h":     fmt.Sprintf("%v", report.EruptionCount24H),
			"ash_column_height_m":    report.AshColumnHeightM,
			"missing_required_field": true,
		})
	}
	return result
}

// MalformedKind names one transport-level corruption of the response body.
type MalformedKind string

const (
	// MalformedTruncated cuts the JSON document in the middle.
	MalformedTruncated MalformedKind = "truncated"
	// MalformedDeepNesting nests arrays far beyond any sane decoder depth.
	MalformedDeepNesting MalformedKind = "deep_nesting"
	// MalformedInvalidUTF8 embeds bytes that are not valid UTF-8.
	MalformedInvalidUTF8 MalformedKind = "invalid_utf8"
	// MalformedOversized pads a valid document past consumer size limits.
	MalformedOversized MalformedKind = "oversized"
)

// deepNestingLevels exceeds encoding/json's 10000-level limit.
const deepNestingLevels = 20000

// oversizedPadding exceeds the aggregator's 8 MiB response limit.
const oversizedPadding = 9 << 20

// ParseMalformedKind validates an admin-supplied kind.
func ParseMalformedKind(value string) (MalformedKind, bool) {
	switch kind := MalformedKind(value); kind {
	case MalformedTruncated, MalformedDeepNesting, MalformedInvalidUTF8, MalformedOversized:
		return kind, true
	default:
		return "", false
	}
}

// SetMalformed serves kind for the next count responses (minimum 1), after
// which the feed returns to well-formed JSON on its own. Counted delivery
// mimics a partner that sends one broken message among healthy ones.
func (r *Runtime) SetMalformed(kind MalformedKind, count int) {
	if count < 1 {
		count = 1
	}
	r.mu.Lock()
	r.malformed = kind
	r.malformedLeft = count
	r.mu.Unlock()
}

// TakeMalformed consumes one pending malformed response, if any.
func (r *Runtime) TakeMalformed() (MalformedKind, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.malformed == "" || r.malformedLeft <= 0 {
		return "", false
	}
	kind := r.malformed
	r.malformedLeft--
	if r.malformedLeft == 0 {
		r.malformed = ""
	}
	return kind, true
}

// MalformedBody renders the reports since the cursor with the requested
// corruption. The bytes are deterministic for one cursor so repeated polls
// produce the same message.
func (r *Runtime) MalformedBody(kind MalformedKind, since time.Time) []byte {
	reports := r.ReportsSince(since)
	switch kind {
	case MalformedTruncated:
		body, _ := json.Marshal(reports)
		if len(body) < 4 {
			return []byte(`[{"report_id":`)
		}
		return body[:len(body)/2]
	case MalformedDeepNesting:
		return []byte(`[{"report_id":"pvmbg-deep","nested":` + strings.Repeat("[", deepNestingLevels) + strings.Repeat("]", deepNestingLevels) + `}]`)
	case MalformedInvalidUTF8:
		body, _ := json.Marshal(reports)
		if len(body) < 2 {
			body = []byte(`[]`)
		}
		// Put raw 0xFF 0xFE bytes inside a string value so the JSON grammar
		// stays valid but the encoding is not UTF-8.
		broken := []byte(`[{"report_id":"pvmbg-bad-utf8","volcano_id":"merapi` + "\xff\xfe" + `"}`)
		if len(body) > 2 {
			broken = append(broken, ',')
			broken = append(broken, body[1:]...)
			return broken
		}
		return append(broken, ']')
	case MalformedOversized:
		body, _ := json.Marshal(map[string]any{"report_id": "pvmbg-oversized", "padding": strings.Repeat("x", oversizedPadding)})
		return append(append([]byte{'['}, body...), ']')
	default:
		body, _ := json.Marshal(reports)
		return body
	}
}

// DelayRange returns the configured artificial delay bounds.
func (r *Runtime) DelayRange() (time.Duration, time.Duration) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.minDelay, r.maxDelay
}
