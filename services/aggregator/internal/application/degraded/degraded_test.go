package degraded

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func lookupOf(units []string, err error) func(context.Context) ([]string, error) {
	return func(context.Context) ([]string, error) { return units, err }
}

func TestCompleteness(t *testing.T) {
	storeDown := errors.New("store down")
	stale := Info{Cause: storeDown, Since: time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)}
	tests := []struct {
		name     string
		lookup   func(context.Context) ([]string, error)
		answer   Info
		partial  bool
		units    []string
		degraded bool
	}{
		{name: "live and complete", lookup: lookupOf(nil, nil)},
		{name: "live, a node is down", lookup: lookupOf([]string{"node-c"}, nil), partial: true, units: []string{"node-c"}},
		{name: "no lookup configured", lookup: nil},
		{name: "stale answer, no node known down", lookup: lookupOf(nil, nil), answer: stale, partial: true, units: []string{StoreUnit}, degraded: true},
		{name: "stale answer, a node known down", lookup: lookupOf([]string{"node-c"}, nil), answer: stale, partial: true, units: []string{"node-c"}, degraded: true},
		{name: "live answer, lookup failed", lookup: lookupOf(nil, storeDown), partial: true, units: []string{StoreUnit}, degraded: true},
		{name: "stale answer, lookup failed", lookup: lookupOf(nil, storeDown), answer: stale, partial: true, units: []string{StoreUnit}, degraded: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partial, units, info := Completeness(context.Background(), tt.lookup, tt.answer)
			if partial != tt.partial || strings.Join(units, ",") != strings.Join(tt.units, ",") || info.Degraded() != tt.degraded {
				t.Fatalf("partial=%v units=%v degraded=%v, want %v %v %v", partial, units, info.Degraded(), tt.partial, tt.units, tt.degraded)
			}
			if !info.Since.Equal(tt.answer.Since) {
				t.Fatalf("since = %s, want the answer's own %s", info.Since, tt.answer.Since)
			}
		})
	}
}

func TestRecordCountsEveryAnswerButWarnsOncePerInterval(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(previous)

	const endpoint = "test_endpoint"
	before := testutil.ToFloat64(answers.WithLabelValues(endpoint))
	Record(endpoint, Info{}) // a live answer is not counted
	for i := 0; i < 50; i++ {
		Record(endpoint, Info{Cause: errors.New("store down"), Since: time.Now()})
	}
	if got := testutil.ToFloat64(answers.WithLabelValues(endpoint)) - before; got != 50 {
		t.Fatalf("counted %v degraded answers, want 50", got)
	}
	if lines := strings.Count(logs.String(), "level=WARN"); lines != 1 {
		t.Fatalf("%d warnings for 50 answers within the interval, want 1:\n%s", lines, logs.String())
	}
	if !strings.Contains(logs.String(), "endpoint="+endpoint) || !strings.Contains(logs.String(), "store down") {
		t.Fatalf("warning lacks the endpoint or the cause: %s", logs.String())
	}
}
