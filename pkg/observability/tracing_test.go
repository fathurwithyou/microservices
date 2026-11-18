package observability

import (
	"strings"
	"testing"
)

func TestSamplerDefaultsToEveryTrace(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")
	if got := sampler().Description(); !strings.Contains(got, "AlwaysOnSampler") {
		t.Fatalf("sampler = %s, want every trace kept by default", got)
	}
}

func TestSamplerUsesConfiguredRatioForNewTraces(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.1")
	got := sampler().Description()
	if !strings.Contains(got, "TraceIDRatioBased{0.1}") || !strings.HasPrefix(got, "ParentBased") {
		t.Fatalf("sampler = %s, want ParentBased with a 0.1 ratio for root spans", got)
	}
}
