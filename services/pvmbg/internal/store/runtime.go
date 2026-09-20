package store

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/pvmbg/internal/domain"
	"github.com/fathurwithyou/microservices/services/pvmbg/internal/seed"
)

type Runtime struct {
	mu       sync.RWMutex
	outage   bool
	schemaV2 bool
	minDelay time.Duration
	maxDelay time.Duration
	reports  []domain.VolcanicReport
}

func New(minDelay, maxDelay time.Duration, count int) *Runtime {
	return &Runtime{minDelay: minDelay, maxDelay: maxDelay, reports: seed.InitialReports(time.Now().UTC(), count)}
}

func (r *Runtime) AppendReport() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	r.reports = append(r.reports, domain.VolcanicReport{
		"report_id":           "pvmbg-report-" + strconv.Itoa(len(r.reports)+1),
		"volcano_id":          "merapi",
		"alert_level":         "Waspada",
		"eruption_count_24h":  1,
		"ash_column_height_m": float64(350),
		"reported_at":         now.Format(time.RFC3339),
	})
}

func (r *Runtime) ReportsSince(since time.Time) []domain.VolcanicReport {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]domain.VolcanicReport, 0)
	for _, original := range r.reports {
		report := original.Clone()
		reportedAt, err := time.Parse(time.RFC3339, fmt.Sprint(report["reported_at"]))
		if err != nil || reportedAt.Before(since) {
			continue
		}
		if r.schemaV2 {
			report["confidence_level"] = 0.82
		}
		result = append(result, report)
	}
	return result
}

func (r *Runtime) IsOutage() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.outage
}

func (r *Runtime) SetOutage(value bool) {
	r.mu.Lock()
	r.outage = value
	r.mu.Unlock()
}

func (r *Runtime) SetSchemaV2(value bool) {
	r.mu.Lock()
	r.schemaV2 = value
	r.mu.Unlock()
}

func (r *Runtime) DelayRange() (time.Duration, time.Duration) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.minDelay, r.maxDelay
}
