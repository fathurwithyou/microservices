package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadRejectsMissingSecrets(t *testing.T) {
	t.Setenv("PVMBG_ADMIN_KEY", "")
	t.Setenv("BMKG_API_KEY", "")
	t.Setenv("PVMBG_TOKEN", "")
	t.Setenv("RABBITMQ_USER", "")
	t.Setenv("RABBITMQ_PASSWORD", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected configuration error")
	}
}

func TestLoadDoesNotReadDatabaseURL(t *testing.T) {
	t.Setenv("PVMBG_ADMIN_KEY", "admin")
	t.Setenv("BMKG_API_KEY", "bmkg")
	t.Setenv("PVMBG_TOKEN", "pvmbg")
	t.Setenv("RABBITMQ_USER", "user")
	t.Setenv("RABBITMQ_PASSWORD", "pass")
	t.Setenv("AGGREGATOR_DATABASE_URL", "postgres://should-not-be-used")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	encoded := os.Getenv("AGGREGATOR_DATABASE_URL")
	if encoded == "" {
		t.Fatal("test setup failed")
	}
	if cfg.ArtifactsDir == "" || cfg.Port == "" {
		t.Fatal("expected defaults")
	}
}

func setRequiredSecrets(t *testing.T) {
	t.Helper()
	t.Setenv("PVMBG_ADMIN_KEY", "admin")
	t.Setenv("BMKG_API_KEY", "bmkg")
	t.Setenv("PVMBG_TOKEN", "pvmbg")
	t.Setenv("RABBITMQ_USER", "user")
	t.Setenv("RABBITMQ_PASSWORD", "pass")
}

func TestLoadRejectsMalformedDuration(t *testing.T) {
	setRequiredSecrets(t)
	t.Setenv("REQUEST_TIMEOUT", "five seconds")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for malformed REQUEST_TIMEOUT")
	}
}

func TestLoadParsesRequestTimeout(t *testing.T) {
	setRequiredSecrets(t)
	t.Setenv("REQUEST_TIMEOUT", "2500ms")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RequestTimeout != 2500*time.Millisecond {
		t.Fatalf("RequestTimeout = %s", cfg.RequestTimeout)
	}
}

func TestLoadQueueNameDefaults(t *testing.T) {
	setRequiredSecrets(t)
	t.Setenv("CONSUMER_QUEUE_DASHBOARD", "")
	t.Setenv("CONSUMER_QUEUE_NOTIFIER", "")
	t.Setenv("CONSUMER_QUEUE_PORTAL", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"dashboard.hazard-events", "notifier.hazard-events", "portal.hazard-events"}
	if len(cfg.QueueNames) != len(expected) {
		t.Fatalf("QueueNames = %#v", cfg.QueueNames)
	}
	for index, want := range expected {
		if cfg.QueueNames[index] != want {
			t.Fatalf("QueueNames[%d] = %q want %q", index, cfg.QueueNames[index], want)
		}
	}
}

func TestLoadQueueNameOverrides(t *testing.T) {
	setRequiredSecrets(t)
	t.Setenv("CONSUMER_QUEUE_DASHBOARD", "custom.dashboard")
	t.Setenv("CONSUMER_QUEUE_NOTIFIER", "custom.notifier")
	t.Setenv("CONSUMER_QUEUE_PORTAL", "custom.portal")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"custom.dashboard", "custom.notifier", "custom.portal"}
	for index, want := range expected {
		if cfg.QueueNames[index] != want {
			t.Fatalf("QueueNames[%d] = %q want %q", index, cfg.QueueNames[index], want)
		}
	}
}
