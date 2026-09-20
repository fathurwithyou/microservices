package config

import "os"

type Config struct{ RabbitURL, HealthPort string }

func Load() Config {
	return Config{RabbitURL: os.Getenv("RABBITMQ_URL"), HealthPort: valueOr("CONSUMER_HEALTH_PORT", "8085")}
}
func valueOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
