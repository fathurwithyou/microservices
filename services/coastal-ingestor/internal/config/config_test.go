package config

import (
	"testing"
	"time"
)

func clearEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"SENSOR_URL",
		"SENSOR_TOKEN",
		"COASTAL_INGESTOR_HEALTH_PORT",
		"COASTAL_DATABASE_URL",
		"COASTAL_INGESTOR_POLL_INTERVAL",
		"COASTAL_INGESTOR_REQUEST_TIMEOUT",
		"COASTAL_STALE_AFTER",
		"COASTAL_SUBMIT_CONCURRENCY",
		"COASTAL_SUBMIT_BUDGET",
		"COASTAL_INGESTOR_CIRCUIT_FAILURES",
		"COASTAL_INGESTOR_CIRCUIT_WINDOW",
		"COASTAL_INGESTOR_CIRCUIT_OPEN_TIMEOUT",
		"INTERNAL_SERVICE_TOKEN",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("INTERNAL_SERVICE_TOKEN", "test-token")
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.HealthPort != "8087" || cfg.PollInterval != 30*time.Second || cfg.CircuitFailureThreshold != 5 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.StaleAfter != 30*time.Minute || cfg.SubmitConcurrency != 8 || cfg.SubmitBudget != time.Second {
		t.Fatalf("unexpected critical-lane defaults: %+v", cfg)
	}
	if cfg.SensorURL != "" || cfg.SensorToken != "" {
		t.Fatalf("fixture mode expected, got url=%q token=%q", cfg.SensorURL, cfg.SensorToken)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		check bool
	}{
		{name: "url without token", env: map[string]string{"SENSOR_URL": "http://sensor"}, check: true},
		{name: "zero poll interval", env: map[string]string{"COASTAL_INGESTOR_POLL_INTERVAL": "0s"}, check: true},
		{name: "negative timeout", env: map[string]string{"COASTAL_INGESTOR_REQUEST_TIMEOUT": "-1s"}, check: true},
		{name: "bad health port", env: map[string]string{"COASTAL_INGESTOR_HEALTH_PORT": "http"}, check: true},
		{name: "zero submit concurrency", env: map[string]string{"COASTAL_SUBMIT_CONCURRENCY": "0"}, check: true},
		{name: "huge submit concurrency", env: map[string]string{"COASTAL_SUBMIT_CONCURRENCY": "1000"}, check: true},
		{name: "zero stale after", env: map[string]string{"COASTAL_STALE_AFTER": "0s"}, check: true},
		{name: "url with token", env: map[string]string{"SENSOR_URL": "http://sensor", "SENSOR_TOKEN": "t"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			_, err := Load()
			if tc.check && err == nil {
				t.Fatal("Load must reject invalid configuration")
			}
			if !tc.check && err != nil {
				t.Fatalf("Load returned error: %v", err)
			}
		})
	}
}
