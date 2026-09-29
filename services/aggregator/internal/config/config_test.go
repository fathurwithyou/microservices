package config

import (
	"testing"
	"time"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("INTERNAL_SERVICE_TOKEN", "test-token")
	t.Setenv("RABBITMQ_URL", "amqp://localhost/")
}

func TestLoadRejectsMalformedDuration(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("POLL_INTERVAL", "notaduration")
	if _, err := Load(); err == nil {
		t.Fatal("malformed POLL_INTERVAL must fail, not fall back")
	}
}

func TestLoadRejectsMissingSecrets(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "amqp://localhost/")
	if _, err := Load(); err == nil {
		t.Fatal("missing INTERNAL_SERVICE_TOKEN must fail")
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("POLL_INTERVAL", "")
	t.Setenv("OUTBOX_POLL_INTERVAL", "")
	t.Setenv("REQUEST_TIMEOUT", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PollInterval != 3*time.Second {
		t.Fatalf("PollInterval = %s, want 3s", cfg.PollInterval)
	}
	if cfg.StoreDriver != "postgres" {
		t.Fatalf("StoreDriver = %q, want postgres", cfg.StoreDriver)
	}
}

func TestValidateRejectsNonPositiveIntervals(t *testing.T) {
	cfg := Config{PollInterval: 0, OutboxPollInterval: time.Second, RequestTimeout: time.Second, StoreDriver: "memory"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("zero POLL_INTERVAL must fail validation")
	}
	cfg = Config{PollInterval: time.Second, OutboxPollInterval: time.Second, RequestTimeout: time.Second, StoreDriver: "bogus"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("unknown STORE_DRIVER must fail validation")
	}
}
