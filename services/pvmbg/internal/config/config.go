package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port              string
	Token             string
	AdminKey          string
	MinDelay          time.Duration
	MaxDelay          time.Duration
	NewReportInterval time.Duration
	SeedCount         int
}

func Load() (Config, error) {
	token := os.Getenv("PVMBG_TOKEN")
	adminKey := os.Getenv("PVMBG_ADMIN_KEY")
	if token == "" || adminKey == "" {
		return Config{}, errors.New("PVMBG_TOKEN and PVMBG_ADMIN_KEY are required")
	}

	minDelay := durationOr("PVMBG_MIN_DELAY", 500*time.Millisecond)
	maxDelay := durationOr("PVMBG_MAX_DELAY", 3*time.Second)
	if maxDelay < minDelay {
		maxDelay = minDelay
	}
	return Config{
		Port:              valueOr("PVMBG_PORT", "8082"),
		Token:             token,
		AdminKey:          adminKey,
		MinDelay:          minDelay,
		MaxDelay:          maxDelay,
		NewReportInterval: durationOr("NEW_EVENT_INTERVAL", 10*time.Second),
		SeedCount:         intOr("SEED_COUNT", 20),
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
