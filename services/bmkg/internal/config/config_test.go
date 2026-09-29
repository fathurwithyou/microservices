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
		"BMKG_API_KEY":       "",
		"BMKG_PORT":          "",
		"BMKG_DELAY":         "",
		"NEW_EVENT_INTERVAL": "",
		"SEED_COUNT":         "",
	})
}

func TestLoadDefaults(t *testing.T) {
	unsetAll(t)
	t.Setenv("BMKG_API_KEY", "test-key")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Port != "8081" {
		t.Errorf("Port = %q, want 8081", cfg.Port)
	}
	if cfg.APIKey != "test-key" {
		t.Errorf("APIKey = %q, want test-key", cfg.APIKey)
	}
	if cfg.ResponseDelay != 100*time.Millisecond {
		t.Errorf("ResponseDelay = %s, want 100ms", cfg.ResponseDelay)
	}
	if cfg.NewEventInterval != 10*time.Second {
		t.Errorf("NewEventInterval = %s, want 10s", cfg.NewEventInterval)
	}
	if cfg.SeedCount != 20 {
		t.Errorf("SeedCount = %d, want 20", cfg.SeedCount)
	}
}

func TestLoadValidOverrides(t *testing.T) {
	unsetAll(t)
	setEnv(t, map[string]string{
		"BMKG_API_KEY":       "override-key",
		"BMKG_PORT":          "9091",
		"BMKG_DELAY":         "150ms",
		"NEW_EVENT_INTERVAL": "30s",
		"SEED_COUNT":         "7",
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Port != "9091" || cfg.APIKey != "override-key" {
		t.Errorf("Port/APIKey = %q/%q", cfg.Port, cfg.APIKey)
	}
	if cfg.ResponseDelay != 150*time.Millisecond {
		t.Errorf("ResponseDelay = %s, want 150ms", cfg.ResponseDelay)
	}
	if cfg.NewEventInterval != 30*time.Second {
		t.Errorf("NewEventInterval = %s, want 30s", cfg.NewEventInterval)
	}
	if cfg.SeedCount != 7 {
		t.Errorf("SeedCount = %d, want 7", cfg.SeedCount)
	}
}

func TestLoadMissingAPIKey(t *testing.T) {
	unsetAll(t)

	if _, err := Load(); err == nil {
		t.Fatal("Load() with missing BMKG_API_KEY must fail")
	}
}

func TestLoadMalformedValues(t *testing.T) {
	cases := map[string]string{
		"BMKG_DELAY":         "not-a-duration",
		"NEW_EVENT_INTERVAL": "10 seconds",
		"SEED_COUNT":         "many",
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			unsetAll(t)
			t.Setenv("BMKG_API_KEY", "test-key")
			t.Setenv(name, value)

			if _, err := Load(); err == nil {
				t.Fatalf("Load() with %s=%q must fail", name, value)
			}
		})
	}
}

func TestLoadNonPositiveEventInterval(t *testing.T) {
	for _, value := range []string{"0s", "-1s"} {
		t.Run(value, func(t *testing.T) {
			unsetAll(t)
			t.Setenv("BMKG_API_KEY", "test-key")
			t.Setenv("NEW_EVENT_INTERVAL", value)

			if _, err := Load(); err == nil {
				t.Fatalf("Load() with NEW_EVENT_INTERVAL=%s must fail (ticker would panic)", value)
			}
		})
	}
}
