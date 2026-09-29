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
	PollInterval         time.Duration
	OutboxPollInterval   time.Duration
	RequestTimeout       time.Duration
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
		PollInterval:         pollInterval,
		OutboxPollInterval:   outboxPollInterval,
		RequestTimeout:       requestTimeout,
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
	switch cfg.StoreDriver {
	case "postgres", "memory":
	default:
		return fmt.Errorf("unsupported STORE_DRIVER %q", cfg.StoreDriver)
	}
	return nil
}
