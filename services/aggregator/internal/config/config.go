package config

import (
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/pkg/env"
)

// Config holds every runtime knob for the aggregator service, loaded from
// environment variables at startup.
type Config struct {
	GRPCPort             string
	HTTPPort             string
	InternalServiceToken string
	BMKGURL              string
	PVMBGURL             string
	BMKGKey              string
	PVMBGToken           string
	DatabaseURL          string
	StoreDriver          string
	RabbitURL            string
	AuditToken           string
	OpsToken             string
	PollInterval         time.Duration
	// SchemaAdapters lists operator-enabled partner schema adapters
	// (PVMBG_SCHEMA_ADAPTERS, e.g. "pvmbg-breaking-v3").
	SchemaAdapters     string
	OutboxPollInterval time.Duration
	RequestTimeout     time.Duration
	LeaderTTL          time.Duration
	LeaderRenew        time.Duration
	// FailureRetryInterval spaces automatic retries of transient failures.
	FailureRetryInterval time.Duration
	// FailureRetryMaxAttempts parks a transient failure after this many tries.
	FailureRetryMaxAttempts int
}

// Load reads configuration from the environment. Malformed values are
// errors: the service fails fast at startup instead of silently running on
// defaults.
func Load() (Config, error) {
	internalServiceToken, err := env.Required("INTERNAL_SERVICE_TOKEN")
	if err != nil {
		return Config{}, err
	}
	rabbitURL, err := env.Required("RABBITMQ_URL")
	if err != nil {
		return Config{}, err
	}
	pollInterval, err := env.Duration("POLL_INTERVAL", 3*time.Second)
	if err != nil {
		return Config{}, err
	}
	outboxPollInterval, err := env.Duration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond)
	if err != nil {
		return Config{}, err
	}
	requestTimeout, err := env.Duration("REQUEST_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	leaderTTL, err := env.Duration("LEADER_TTL", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	leaderRenew, err := env.Duration("LEADER_RENEW", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	failureRetryInterval, err := env.Duration("FAILURE_RETRY_INTERVAL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	failureRetryMaxAttempts, err := env.Int("FAILURE_RETRY_MAX_ATTEMPTS", 5)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		GRPCPort:             env.String("AGGREGATOR_GRPC_PORT", "9090"),
		HTTPPort:             env.String("AGGREGATOR_HTTP_PORT", "8090"),
		InternalServiceToken: internalServiceToken,
		BMKGURL:              env.String("BMKG_URL", "http://bmkg:8081"),
		PVMBGURL:             env.String("PVMBG_URL", "http://pvmbg:8082"),
		BMKGKey:              env.String("BMKG_API_KEY", ""),
		PVMBGToken:           env.String("PVMBG_TOKEN", ""),
		DatabaseURL:          env.String("AGGREGATOR_DATABASE_URL", ""),
		StoreDriver:          env.String("STORE_DRIVER", "postgres"),
		RabbitURL:            rabbitURL,
		AuditToken:           env.String("AUDIT_TOKEN", ""),
		OpsToken:             env.String("OPS_TOKEN", ""),
		PollInterval:         pollInterval,
		SchemaAdapters:       env.String("PVMBG_SCHEMA_ADAPTERS", ""),
		OutboxPollInterval:   outboxPollInterval,
		RequestTimeout:       requestTimeout,
		LeaderTTL:            leaderTTL,
		LeaderRenew:          leaderRenew,

		FailureRetryInterval:    failureRetryInterval,
		FailureRetryMaxAttempts: failureRetryMaxAttempts,
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks cross-field constraints that env parsing cannot express.
func (cfg Config) Validate() error {
	if cfg.PollInterval <= 0 {
		return fmt.Errorf("POLL_INTERVAL must be positive, got %s", cfg.PollInterval)
	}
	if cfg.OutboxPollInterval <= 0 {
		return fmt.Errorf("OUTBOX_POLL_INTERVAL must be positive, got %s", cfg.OutboxPollInterval)
	}
	if cfg.RequestTimeout <= 0 {
		return fmt.Errorf("REQUEST_TIMEOUT must be positive, got %s", cfg.RequestTimeout)
	}
	if cfg.LeaderTTL <= 0 {
		return fmt.Errorf("LEADER_TTL must be positive, got %s", cfg.LeaderTTL)
	}
	if cfg.LeaderRenew <= 0 || cfg.LeaderRenew >= cfg.LeaderTTL {
		return fmt.Errorf("LEADER_RENEW must be positive and less than LEADER_TTL, got %s/%s", cfg.LeaderRenew, cfg.LeaderTTL)
	}
	if cfg.FailureRetryInterval <= 0 {
		return fmt.Errorf("FAILURE_RETRY_INTERVAL must be positive, got %s", cfg.FailureRetryInterval)
	}
	if cfg.FailureRetryMaxAttempts < 1 {
		return fmt.Errorf("FAILURE_RETRY_MAX_ATTEMPTS must be at least 1, got %d", cfg.FailureRetryMaxAttempts)
	}
	switch cfg.StoreDriver {
	case "postgres", "cockroach", "memory":
	default:
		return fmt.Errorf("unsupported STORE_DRIVER %q", cfg.StoreDriver)
	}
	return nil
}
