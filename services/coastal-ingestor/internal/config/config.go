// Package config loads coastal-ingestor runtime configuration from the
// environment.
package config

import (
	"fmt"
	"strconv"
	"time"

	"github.com/fathurwithyou/microservices/pkg/env"
)

// Config holds the runtime configuration for the coastal ingestor.
type Config struct {
	// SensorURL is the coastal sensor feed. Empty selects fixture mode.
	SensorURL string
	// SensorToken is the bearer token for the sensor feed. Optional in
	// fixture mode.
	SensorToken string
	// AggregatorAddr is the internal gRPC target.
	AggregatorAddr string
	// InternalToken authenticates to the aggregator.
	InternalToken string
	// HealthPort is the TCP port for the health endpoint.
	HealthPort string
	// DatabaseURL is the coastal-ingestor Postgres/CockroachDB DSN. Empty
	// keeps quarantine and last-good state in memory.
	DatabaseURL string
	// PollInterval is the gap between ingest cycles.
	PollInterval time.Duration
	// RequestTimeout bounds one source fetch.
	RequestTimeout time.Duration
	// StaleAfter is the feed freshness (sensor clock minus newest
	// observed_at, in sensor time) above which the feed is reported stale.
	// It replaces the per-reading COASTAL_INGESTOR_MAX_AGE, which compared
	// the sensor's virtual clock with ours and is no longer read.
	StaleAfter time.Duration
	// SubmitConcurrency is the number of concurrent Aggregator submits.
	SubmitConcurrency int
	// SubmitBudget is how long one cycle submits non-critical readings
	// before polling the sensor again.
	SubmitBudget time.Duration
	// CircuitFailureThreshold is the consecutive failure count that trips
	// the breaker.
	CircuitFailureThreshold int
	// CircuitFailureWindow resets the failure streak after a quiet period.
	CircuitFailureWindow time.Duration
	// CircuitOpenTimeout is how long the breaker stays open before probing.
	CircuitOpenTimeout time.Duration
	// LeaderTTL is how long a silent leader keeps the polling lease. It is
	// short because a critical reading must reach Tim Lapangan within 5 s.
	LeaderTTL time.Duration
	// LeaderRenew is the lease renewal interval; must be below LeaderTTL.
	LeaderRenew time.Duration
}

// Load reads configuration from environment variables. Malformed values are
// errors instead of silent fallbacks so the service fails fast at startup.
func Load() (Config, error) {
	pollInterval, err := env.Duration("COASTAL_INGESTOR_POLL_INTERVAL", 30*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	requestTimeout, err := env.Duration("COASTAL_INGESTOR_REQUEST_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	staleAfter, err := env.Duration("COASTAL_STALE_AFTER", 30*time.Minute)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	submitConcurrency, err := env.Int("COASTAL_SUBMIT_CONCURRENCY", 8)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	submitBudget, err := env.Duration("COASTAL_SUBMIT_BUDGET", time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	failureThreshold, err := env.Int("COASTAL_INGESTOR_CIRCUIT_FAILURES", 5)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	failureWindow, err := env.Duration("COASTAL_INGESTOR_CIRCUIT_WINDOW", 30*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	openTimeout, err := env.Duration("COASTAL_INGESTOR_CIRCUIT_OPEN_TIMEOUT", 30*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	leaderTTL, err := env.Duration("COASTAL_LEADER_TTL", 6*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	leaderRenew, err := env.Duration("COASTAL_LEADER_RENEW", 2*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	internalToken, err := env.Required("INTERNAL_SERVICE_TOKEN")
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	cfg := Config{
		SensorURL:               env.String("SENSOR_URL", ""),
		SensorToken:             env.String("SENSOR_TOKEN", ""),
		AggregatorAddr:          env.String("AGGREGATOR_GRPC_ADDR", "aggregator:9090"),
		InternalToken:           internalToken,
		HealthPort:              env.String("COASTAL_INGESTOR_HEALTH_PORT", "8087"),
		DatabaseURL:             env.String("COASTAL_DATABASE_URL", ""),
		PollInterval:            pollInterval,
		RequestTimeout:          requestTimeout,
		StaleAfter:              staleAfter,
		SubmitConcurrency:       submitConcurrency,
		SubmitBudget:            submitBudget,
		CircuitFailureThreshold: failureThreshold,
		CircuitFailureWindow:    failureWindow,
		CircuitOpenTimeout:      openTimeout,
		LeaderTTL:               leaderTTL,
		LeaderRenew:             leaderRenew,
	}
	return cfg, cfg.Validate()
}

// Validate checks cross-field constraints. Durations and thresholds must be
// positive; fixture mode (empty SENSOR_URL) needs no token.
func (c Config) Validate() error {
	port, err := strconv.Atoi(c.HealthPort)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("COASTAL_INGESTOR_HEALTH_PORT must be a TCP port, got %q", c.HealthPort)
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("COASTAL_INGESTOR_POLL_INTERVAL must be positive, got %s", c.PollInterval)
	}
	if c.RequestTimeout <= 0 {
		return fmt.Errorf("COASTAL_INGESTOR_REQUEST_TIMEOUT must be positive, got %s", c.RequestTimeout)
	}
	if c.StaleAfter <= 0 {
		return fmt.Errorf("COASTAL_STALE_AFTER must be positive, got %s", c.StaleAfter)
	}
	// The upper bound keeps a typo from opening hundreds of streams against
	// the Aggregator.
	if c.SubmitConcurrency < 1 || c.SubmitConcurrency > 64 {
		return fmt.Errorf("COASTAL_SUBMIT_CONCURRENCY must be between 1 and 64, got %d", c.SubmitConcurrency)
	}
	if c.SubmitBudget <= 0 {
		return fmt.Errorf("COASTAL_SUBMIT_BUDGET must be positive, got %s", c.SubmitBudget)
	}
	if c.CircuitFailureThreshold <= 0 {
		return fmt.Errorf("COASTAL_INGESTOR_CIRCUIT_FAILURES must be positive, got %d", c.CircuitFailureThreshold)
	}
	if c.CircuitFailureWindow <= 0 {
		return fmt.Errorf("COASTAL_INGESTOR_CIRCUIT_WINDOW must be positive, got %s", c.CircuitFailureWindow)
	}
	if c.CircuitOpenTimeout <= 0 {
		return fmt.Errorf("COASTAL_INGESTOR_CIRCUIT_OPEN_TIMEOUT must be positive, got %s", c.CircuitOpenTimeout)
	}
	if c.LeaderRenew <= 0 || c.LeaderRenew >= c.LeaderTTL {
		return fmt.Errorf("COASTAL_LEADER_RENEW must be positive and below COASTAL_LEADER_TTL, got %s/%s", c.LeaderRenew, c.LeaderTTL)
	}
	if c.SensorURL != "" && c.SensorToken == "" {
		return fmt.Errorf("SENSOR_TOKEN is required when SENSOR_URL is set")
	}
	return nil
}
