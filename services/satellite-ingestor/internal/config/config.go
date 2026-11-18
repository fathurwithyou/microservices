// Package config loads satellite-ingestor runtime configuration from the
// environment.
package config

import (
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/pkg/env"
)

// Config holds the runtime configuration for the satellite ingestor.
type Config struct {
	// FeedURL is the base URL of the hotspot feed. Required.
	FeedURL string
	// FeedToken is the bearer credential sent to the feed. Required.
	FeedToken string
	// AggregatorAddr is the internal gRPC target. Required for M2 ingestion.
	AggregatorAddr string
	// InternalToken authenticates to the aggregator. Required.
	InternalToken string
	// HealthPort is the TCP port the health endpoint listens on.
	HealthPort string
	// DatabaseURL enables durable state when set. Empty keeps the in-memory
	// adapter, which is valid for tests and local development.
	DatabaseURL string
	// LateWindow is the accepted lateness: a detection whose acquisition time
	// is more than this behind the latest acquisition time seen is counted as
	// late instead of clustered.
	LateWindow time.Duration
	// IdleTimeout ends a feed connection that delivered nothing, not even a
	// heartbeat, for this long.
	IdleTimeout time.Duration
	// MinBackoff and MaxBackoff bound the reconnect delay.
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// LeaderTTL and LeaderRenew control the single-writer lease.
	LeaderTTL   time.Duration
	LeaderRenew time.Duration
}

// Load reads configuration from environment variables. Missing required
// values and malformed or non-positive durations are errors so the service
// fails fast at startup instead of silently reconnecting in a tight loop.
func Load() (Config, error) {
	feedURL, err := env.Required("FEED_URL")
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	feedToken, err := env.Required("FEED_TOKEN")
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	internalToken, err := env.Required("INTERNAL_SERVICE_TOKEN")
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	// 12 h covers a morning pass whose data only arrives at noon, twice over.
	lateWindow, err := env.Duration("SATELLITE_LATE_WINDOW", 12*time.Hour)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	idleTimeout, err := env.Duration("SATELLITE_IDLE_TIMEOUT", 45*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	minBackoff, err := env.Duration("SATELLITE_MIN_BACKOFF", time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	maxBackoff, err := env.Duration("SATELLITE_MAX_BACKOFF", 30*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	leaderTTL, err := env.Duration("LEADER_TTL", 15*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	leaderRenew, err := env.Duration("LEADER_RENEW", 5*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}

	cfg := Config{
		FeedURL:        feedURL,
		FeedToken:      feedToken,
		AggregatorAddr: env.String("AGGREGATOR_GRPC_ADDR", "aggregator:9090"),
		InternalToken:  internalToken,
		HealthPort:     env.String("SATELIT_INGESTOR_HEALTH_PORT", "8086"),
		DatabaseURL:    env.String("SATELLITE_DATABASE_URL", ""),
		LateWindow:     lateWindow,
		IdleTimeout:    idleTimeout,
		MinBackoff:     minBackoff,
		MaxBackoff:     maxBackoff,
		LeaderTTL:      leaderTTL,
		LeaderRenew:    leaderRenew,
	}
	return cfg, cfg.Validate()
}

// Validate rejects durations that would make the stream loop misbehave.
func (c Config) Validate() error {
	durations := []struct {
		name  string
		value time.Duration
	}{
		{"SATELLITE_LATE_WINDOW", c.LateWindow},
		{"SATELLITE_IDLE_TIMEOUT", c.IdleTimeout},
		{"SATELLITE_MIN_BACKOFF", c.MinBackoff},
		{"SATELLITE_MAX_BACKOFF", c.MaxBackoff},
		{"LEADER_TTL", c.LeaderTTL},
		{"LEADER_RENEW", c.LeaderRenew},
	}
	for _, duration := range durations {
		if duration.value <= 0 {
			return fmt.Errorf("config: environment variable %s must be positive, got %s", duration.name, duration.value)
		}
	}
	if c.MaxBackoff < c.MinBackoff {
		return fmt.Errorf("config: SATELLITE_MAX_BACKOFF (%s) must be at least SATELLITE_MIN_BACKOFF (%s)", c.MaxBackoff, c.MinBackoff)
	}
	return nil
}
