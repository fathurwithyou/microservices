package config

import (
	"strings"
	"testing"
)

func TestLoadMissingRabbitURLFails(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with missing RABBITMQ_URL: err = nil, want error")
	}
	if !strings.Contains(err.Error(), "RABBITMQ_URL") {
		t.Fatalf("Load() error = %q, want it to mention RABBITMQ_URL", err)
	}
}

func TestLoadDefaultsHealthPort(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	t.Setenv("CONSUMER_HEALTH_PORT", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() = error %v, want success", err)
	}
	if cfg.HealthPort != "8083" {
		t.Errorf("HealthPort = %q, want %q", cfg.HealthPort, "8083")
	}
}

func TestLoadOverridesFromEnvironment(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "amqp://user:pass@rabbit:5672/vhost")
	t.Setenv("CONSUMER_HEALTH_PORT", "9999")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() = error %v, want success", err)
	}
	if cfg.RabbitURL != "amqp://user:pass@rabbit:5672/vhost" {
		t.Errorf("RabbitURL = %q, want environment value", cfg.RabbitURL)
	}
	if cfg.HealthPort != "9999" {
		t.Errorf("HealthPort = %q, want %q", cfg.HealthPort, "9999")
	}
}
