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
	// QueueName is the service-owned queue. Defaults to notifier.hazard-events.
	QueueName string
	// ServiceName is used for logs, health, and the AMQP consumer tag.
	ServiceName string
	// Exchange is the fanout exchange this consumer binds to.
	Exchange string
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
		RabbitURL:   rabbitURL,
		HealthPort:  env.String("CONSUMER_HEALTH_PORT", "8084"),
		QueueName:   env.String("NOTIFIER_QUEUE", "notifier.hazard-events"),
		ServiceName: env.String("NOTIFIER_SERVICE_NAME", "notifier-consumer"),
		Exchange:    env.String("NOTIFIER_EXCHANGE", "hazard.events"),
	}, nil
}
