// Package config loads BMKG runtime configuration from the environment.
package config

import (
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/pkg/env"
)

// Config holds the runtime configuration for the BMKG simulation service.
type Config struct {
	// Port is the TCP port the HTTP server listens on.
	Port string
	// APIKey is the credential clients send in the X-BMKG-Key header.
	APIKey string
	// ResponseDelay is the fixed artificial delay injected into
	// /seismic-events responses.
	ResponseDelay time.Duration
	// NewEventInterval is how often the producer appends a new event.
	NewEventInterval time.Duration
	// SeedCount is the number of historical events seeded at startup.
	SeedCount int
}

// Load reads configuration from environment variables. Malformed integer or
// duration values are errors instead of silent fallbacks so the service
// fails fast at startup. A non-positive NEW_EVENT_INTERVAL is rejected
// because time.NewTicker panics on it.
func Load() (Config, error) {
	apiKey, err := env.Required("BMKG_API_KEY")
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	responseDelay, err := env.Duration("BMKG_DELAY", 100*time.Millisecond)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	newEventInterval, err := env.Duration("NEW_EVENT_INTERVAL", 10*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	if newEventInterval <= 0 {
		return Config{}, fmt.Errorf("load config: environment variable NEW_EVENT_INTERVAL must be positive, got %s", newEventInterval)
	}
	seedCount, err := env.Int("SEED_COUNT", 20)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	return Config{
		Port:             env.String("BMKG_PORT", "8081"),
		APIKey:           apiKey,
		ResponseDelay:    responseDelay,
		NewEventInterval: newEventInterval,
		SeedCount:        seedCount,
	}, nil
}
