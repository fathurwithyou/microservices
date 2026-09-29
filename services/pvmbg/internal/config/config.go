// Package config loads PVMBG runtime configuration from the environment.
package config

import (
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/pkg/env"
)

// Config holds the runtime configuration for the PVMBG simulation service.
type Config struct {
	// Port is the TCP port the HTTP server listens on.
	Port string
	// Token is the bearer credential clients send in the Authorization
	// header for data routes.
	Token string
	// AdminKey is the credential sent in the admin key header for
	// simulation-control routes.
	AdminKey string
	// MinDelay and MaxDelay bound the random response delay injected into
	// /volcanic-reports.
	MinDelay time.Duration
	MaxDelay time.Duration
	// NewReportInterval is how often the producer appends a new report.
	NewReportInterval time.Duration
	// SeedCount is the number of historical reports seeded at startup.
	SeedCount int
}

// Load reads configuration from environment variables. Malformed integer or
// duration values are errors instead of silent fallbacks so the service
// fails fast at startup. A non-positive NEW_REPORT_INTERVAL is rejected
// because time.NewTicker panics on it.
func Load() (Config, error) {
	token, err := env.Required("PVMBG_TOKEN")
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	adminKey, err := env.Required("PVMBG_ADMIN_KEY")
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	minDelay, err := env.Duration("PVMBG_MIN_DELAY", 500*time.Millisecond)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	maxDelay, err := env.Duration("PVMBG_MAX_DELAY", 3*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	if maxDelay < minDelay {
		maxDelay = minDelay
	}
	newReportInterval, err := env.Duration("NEW_REPORT_INTERVAL", 10*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	if newReportInterval <= 0 {
		return Config{}, fmt.Errorf("load config: environment variable NEW_REPORT_INTERVAL must be positive, got %s", newReportInterval)
	}
	seedCount, err := env.Int("SEED_COUNT", 20)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	return Config{
		Port:              env.String("PVMBG_PORT", "8082"),
		Token:             token,
		AdminKey:          adminKey,
		MinDelay:          minDelay,
		MaxDelay:          maxDelay,
		NewReportInterval: newReportInterval,
		SeedCount:         seedCount,
	}, nil
}
