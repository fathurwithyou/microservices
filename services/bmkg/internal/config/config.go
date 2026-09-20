package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port             string
	APIKey           string
	ResponseDelay    time.Duration
	NewEventInterval time.Duration
	SeedCount        int
}

func Load() (Config, error) {
	apiKey := os.Getenv("BMKG_API_KEY")
	if apiKey == "" {
		return Config{}, errors.New("BMKG_API_KEY is required")
	}
	return Config{
		Port:             valueOr("BMKG_PORT", "8081"),
		APIKey:           apiKey,
		ResponseDelay:    durationOr("BMKG_DELAY", 100*time.Millisecond),
		NewEventInterval: durationOr("NEW_EVENT_INTERVAL", 10*time.Second),
		SeedCount:        intOr("SEED_COUNT", 20),
	}, nil
}

func intOr(name string, fallback int) int {
	if value := os.Getenv(name); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed >= 0 {
			return parsed
		}
	}
	return fallback
}

func valueOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func durationOr(name string, fallback time.Duration) time.Duration {
	if value := os.Getenv(name); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return fallback
}
