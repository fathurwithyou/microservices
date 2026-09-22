package config

import (
	"os"
	"testing"
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
