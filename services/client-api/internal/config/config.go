package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port                 string
	InternalServiceToken string
	AggregatorAddress    string
	AuthAddress          string
	RequestTimeout       time.Duration
	MaxInflightRequests  int
}

func Load() (Config, error) {
	internalServiceToken := os.Getenv("INTERNAL_SERVICE_TOKEN")
	if internalServiceToken == "" {
		return Config{}, errors.New("INTERNAL_SERVICE_TOKEN is required")
	}
	return Config{
		Port:                 valueOr("CLIENT_API_PORT", "8080"),
		InternalServiceToken: internalServiceToken,
		AggregatorAddress:    valueOr("AGGREGATOR_GRPC_ADDR", "aggregator:9090"),
		AuthAddress:          valueOr("AUTH_GRPC_ADDR", "auth:9091"),
		RequestTimeout:       durationOr("REQUEST_TIMEOUT", 5*time.Second),
		MaxInflightRequests:  intOr("MAX_INFLIGHT_REQUESTS", 64),
	}, nil
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

func intOr(name string, fallback int) int {
	if value := os.Getenv(name); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}
