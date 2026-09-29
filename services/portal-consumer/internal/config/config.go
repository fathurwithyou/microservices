package config

import (
	"fmt"

	"github.com/fathurwithyou/microservices/pkg/env"
)

// Config holds the runtime configuration for the consumer.
type Config struct {
	// RabbitURL is the AMQP connection string. Required.
	RabbitURL string
	// HealthPort is the TCP port the health endpoint listens on.
	HealthPort string
}

// Load reads configuration from the environment. A missing RABBITMQ_URL is
// an error so the service fails fast at startup instead of silently running
// without a broker.
func Load() (Config, error) {
	rabbitURL, err := env.Required("RABBITMQ_URL")
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	return Config{
		RabbitURL:  rabbitURL,
		HealthPort: env.String("CONSUMER_HEALTH_PORT", "8085"),
	}, nil
}
