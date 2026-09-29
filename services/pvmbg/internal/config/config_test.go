package config

import (
	"testing"
	"time"
)

// setEnv pins every variable Load reads so tests are hermetic; empty values
// behave as unset.
func setEnv(t *testing.T, values map[string]string) {
	t.Helper()
	for name, value := range values {
		t.Setenv(name, value)
	}
}

func unsetAll(t *testing.T) {
	t.Helper()
	setEnv(t, map[string]string{
		"PVMBG_TOKEN":         "",
		"PVMBG_ADMIN_KEY":     "",
		"PVMBG_PORT":          "",
		"PVMBG_MIN_DELAY":     "",
		"PVMBG_MAX_DELAY":     "",
		"NEW_REPORT_INTERVAL": "",
		"SEED_COUNT":          "",
	})
}

func setSecrets(t *testing.T) {
	t.Helper()
	setEnv(t, map[string]string{
		"PVMBG_TOKEN":     "test-token",
		"PVMBG_ADMIN_KEY": "test-admin-key",
	})
}

func TestLoadDefaults(t *testing.T) {
	unsetAll(t)
	setSecrets(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Port != "8082" {
		t.Errorf("Port = %q, want 8082", cfg.Port)
	}
	if cfg.MinDelay != 500*time.Millisecond {
		t.Errorf("MinDelay = %s, want 500ms", cfg.MinDelay)
	}
	if cfg.MaxDelay != 3*time.Second {
		t.Errorf("MaxDelay = %s, want 3s", cfg.MaxDelay)
	}
	if cfg.NewReportInterval != 10*time.Second {
		t.Errorf("NewReportInterval = %s, want 10s", cfg.NewReportInterval)
	}
	if cfg.SeedCount != 20 {
		t.Errorf("SeedCount = %d, want 20", cfg.SeedCount)
	}
}

func TestLoadValidOverrides(t *testing.T) {
	unsetAll(t)
	setSecrets(t)
	setEnv(t, map[string]string{
		"PVMBG_PORT":          "9092",
		"PVMBG_MIN_DELAY":     "200ms",
		"PVMBG_MAX_DELAY":     "1s",
		"NEW_REPORT_INTERVAL": "45s",
		"SEED_COUNT":          "3",
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Port != "9092" {
		t.Errorf("Port = %q, want 9092", cfg.Port)
	}
	if cfg.MinDelay != 200*time.Millisecond || cfg.MaxDelay != time.Second {
		t.Errorf("delays = %s/%s, want 200ms/1s", cfg.MinDelay, cfg.MaxDelay)
	}
	if cfg.NewReportInterval != 45*time.Second {
		t.Errorf("NewReportInterval = %s, want 45s", cfg.NewReportInterval)
	}
	if cfg.SeedCount != 3 {
		t.Errorf("SeedCount = %d, want 3", cfg.SeedCount)
	}
}

func TestLoadClampsMaxDelayBelowMin(t *testing.T) {
	unsetAll(t)
	setSecrets(t)
	setEnv(t, map[string]string{
		"PVMBG_MIN_DELAY": "2s",
		"PVMBG_MAX_DELAY": "1s",
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.MaxDelay != cfg.MinDelay {
		t.Errorf("MaxDelay = %s, want clamped to MinDelay %s", cfg.MaxDelay, cfg.MinDelay)
	}
}

func TestLoadMissingSecrets(t *testing.T) {
	for _, missing := range []string{"PVMBG_TOKEN", "PVMBG_ADMIN_KEY"} {
		t.Run(missing, func(t *testing.T) {
			unsetAll(t)
			setSecrets(t)
			t.Setenv(missing, "")

			if _, err := Load(); err == nil {
				t.Fatalf("Load() with missing %s must fail", missing)
			}
		})
	}
}

func TestLoadMalformedValues(t *testing.T) {
	cases := map[string]string{
		"PVMBG_MIN_DELAY":     "soon",
		"PVMBG_MAX_DELAY":     "3 seconds",
		"NEW_REPORT_INTERVAL": "10s-ish",
		"SEED_COUNT":          "many",
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			unsetAll(t)
			setSecrets(t)
			t.Setenv(name, value)

			if _, err := Load(); err == nil {
				t.Fatalf("Load() with %s=%q must fail", name, value)
			}
		})
	}
}

func TestLoadNonPositiveReportInterval(t *testing.T) {
	for _, value := range []string{"0s", "-1s"} {
		t.Run(value, func(t *testing.T) {
			unsetAll(t)
			setSecrets(t)
			t.Setenv("NEW_REPORT_INTERVAL", value)

			if _, err := Load(); err == nil {
				t.Fatalf("Load() with NEW_REPORT_INTERVAL=%s must fail (ticker would panic)", value)
			}
		})
	}
}

func TestLoadIgnoresLegacyNewEventInterval(t *testing.T) {
	unsetAll(t)
	setSecrets(t)
	t.Setenv("NEW_EVENT_INTERVAL", "99s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.NewReportInterval != 10*time.Second {
		t.Errorf("NewReportInterval = %s, want default 10s (legacy NEW_EVENT_INTERVAL must be ignored)", cfg.NewReportInterval)
	}
}
