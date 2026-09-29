package state

import (
	"errors"
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
	minDelay time.Duration
	maxDelay time.Duration
	now      func() time.Time
	reports  []domain.VolcanicReport
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

// SetSchemaV2 toggles schema version 2 simulation.
func (r *Runtime) SetSchemaV2(value bool) {
	r.mu.Lock()
	r.schemaV2 = value
	r.mu.Unlock()
}

// DelayRange returns the configured artificial delay bounds.
func (r *Runtime) DelayRange() (time.Duration, time.Duration) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.minDelay, r.maxDelay
}
