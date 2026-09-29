// Package env reads typed configuration values from environment variables.
//
// Unlike ad hoc getenv helpers, malformed values are returned as errors
// instead of silently falling back to defaults, so services fail fast at
// startup when configuration is invalid.
package env

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// String returns the value of the environment variable name, or fallback
// when name is unset or empty.
func String(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// Required returns the value of the environment variable name, or an error
// when it is unset or empty.
func Required(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("environment variable %s is required", name)
	}
	return value, nil
}

// Int returns the environment variable name parsed as an integer, or
// fallback when name is unset or empty. A malformed value is an error.
func Int(name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("environment variable %s must be an integer: %w", name, err)
	}
	return value, nil
}

// Duration returns the environment variable name parsed as a
// time.Duration, or fallback when name is unset or empty. A malformed
// value is an error.
func Duration(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("environment variable %s must be a duration: %w", name, err)
	}
	return value, nil
}

// Bool returns the environment variable name parsed as a boolean, or
// fallback when name is unset or empty. A malformed value is an error.
func Bool(name string, fallback bool) (bool, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("environment variable %s must be a boolean: %w", name, err)
	}
	return value, nil
}
