package config

import (
	"errors"
	"os"
	"time"
)

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

func Load() (Config, error) {
	internalServiceToken := os.Getenv("INTERNAL_SERVICE_TOKEN")
	if internalServiceToken == "" {
		return Config{}, errors.New("INTERNAL_SERVICE_TOKEN is required")
	}
	return Config{
		GRPCPort:             valueOr("AGGREGATOR_GRPC_PORT", "9090"),
		HTTPPort:             valueOr("AGGREGATOR_HTTP_PORT", "8090"),
		InternalServiceToken: internalServiceToken,
		BMKGURL:              valueOr("BMKG_URL", "http://bmkg:8081"),
		PVMBGURL:             valueOr("PVMBG_URL", "http://pvmbg:8082"),
		BMKGKey:              os.Getenv("BMKG_API_KEY"),
		PVMBGToken:           os.Getenv("PVMBG_TOKEN"),
		DatabaseURL:          os.Getenv("AGGREGATOR_DATABASE_URL"),
		StoreDriver:          valueOr("STORE_DRIVER", "postgres"),
		RabbitURL:            os.Getenv("RABBITMQ_URL"),
		PollInterval:         durationOr("POLL_INTERVAL", 3*time.Second),
		OutboxPollInterval:   durationOr("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		RequestTimeout:       durationOr("REQUEST_TIMEOUT", 5*time.Second),
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
