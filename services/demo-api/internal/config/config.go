package config

import (
	"errors"
	"os"
	"time"
)

type Config struct {
	Port                 string
	PVMBGURL             string
	PVMBGAdminKey        string
	PVMBGToken           string
	BMKGURL              string
	BMKGAPIKey           string
	RabbitManagementURL  string
	RabbitUser           string
	RabbitPassword       string
	ArtifactsDir         string
	ClientAPIHealthURL   string
	AggregatorHealthURL  string
	BMKGHealthURL        string
	PVMBGHealthURL       string
	AuthAddr             string
	DashboardConsumerURL string
	NotifierConsumerURL  string
	PortalConsumerURL    string
	RequestTimeout       time.Duration
}

func Load() (Config, error) {
	pvmbgAdminKey := os.Getenv("PVMBG_ADMIN_KEY")
	if pvmbgAdminKey == "" {
		return Config{}, errors.New("PVMBG_ADMIN_KEY is required")
	}
	bmkgAPIKey := os.Getenv("BMKG_API_KEY")
	if bmkgAPIKey == "" {
		return Config{}, errors.New("BMKG_API_KEY is required")
	}
	pvmbgToken := os.Getenv("PVMBG_TOKEN")
	if pvmbgToken == "" {
		return Config{}, errors.New("PVMBG_TOKEN is required")
	}
	rabbitUser := os.Getenv("RABBITMQ_USER")
	if rabbitUser == "" {
		return Config{}, errors.New("RABBITMQ_USER is required")
	}
	rabbitPassword := os.Getenv("RABBITMQ_PASSWORD")
	if rabbitPassword == "" {
		return Config{}, errors.New("RABBITMQ_PASSWORD is required")
	}
	return Config{
		Port:                 valueOr("DEMO_API_PORT", "8086"),
		PVMBGURL:             valueOr("PVMBG_URL", "http://pvmbg:8082"),
		PVMBGAdminKey:        pvmbgAdminKey,
		PVMBGToken:           pvmbgToken,
		BMKGURL:              valueOr("BMKG_URL", "http://bmkg:8081"),
		BMKGAPIKey:           bmkgAPIKey,
		RabbitManagementURL:  valueOr("RABBITMQ_MANAGEMENT_URL", "http://rabbitmq:15672"),
		RabbitUser:           rabbitUser,
		RabbitPassword:       rabbitPassword,
		ArtifactsDir:         valueOr("ARTIFACTS_DIR", "/artifacts"),
		ClientAPIHealthURL:   valueOr("CLIENT_API_HEALTH_URL", "http://client-api:8080/health"),
		AggregatorHealthURL:  valueOr("AGGREGATOR_HEALTH_URL", "http://aggregator:8090/health"),
		BMKGHealthURL:        valueOr("BMKG_HEALTH_URL", "http://bmkg:8081/health"),
		PVMBGHealthURL:       valueOr("PVMBG_HEALTH_URL", "http://pvmbg:8082/health"),
		AuthAddr:             valueOr("AUTH_ADDR", "auth:9091"),
		DashboardConsumerURL: valueOr("DASHBOARD_CONSUMER_HEALTH_URL", "http://dashboard-consumer:8083/health"),
		NotifierConsumerURL:  valueOr("NOTIFIER_CONSUMER_HEALTH_URL", "http://notifier-consumer:8084/health"),
		PortalConsumerURL:    valueOr("PORTAL_CONSUMER_HEALTH_URL", "http://portal-consumer:8085/health"),
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
