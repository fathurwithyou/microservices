package config

import (
	"time"

	"github.com/fathurwithyou/microservices/pkg/env"
)

// Config holds the runtime configuration for the client-api gateway.
type Config struct {
	Port                 string
	InternalServiceToken string
	AggregatorAddress    string
	AuthAddress          string
	RequestTimeout       time.Duration
	MaxInflightRequests  int
}

// Load reads configuration from the environment. Malformed values are
// errors so the service fails fast at startup.
func Load() (Config, error) {
	internalServiceToken, err := env.Required("INTERNAL_SERVICE_TOKEN")
	if err != nil {
		return Config{}, err
	}
	requestTimeout, err := env.Duration("REQUEST_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	maxInflight, err := env.Int("MAX_INFLIGHT_REQUESTS", 64)
	if err != nil {
		return Config{}, err
	}
	if maxInflight < 1 {
		maxInflight = 1
	}
	return Config{
		Port:                 env.String("CLIENT_API_PORT", "8080"),
		InternalServiceToken: internalServiceToken,
		AggregatorAddress:    env.String("AGGREGATOR_GRPC_ADDR", "aggregator:9090"),
		AuthAddress:          env.String("AUTH_GRPC_ADDR", "auth:9091"),
		RequestTimeout:       requestTimeout,
		MaxInflightRequests:  maxInflight,
	}, nil
}
