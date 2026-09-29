package config

import (
	"time"

	"github.com/fathurwithyou/microservices/pkg/env"
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
	QueueNames           []string
}

func Load() (Config, error) {
	pvmbgAdminKey, err := env.Required("PVMBG_ADMIN_KEY")
	if err != nil {
		return Config{}, err
	}
	bmkgAPIKey, err := env.Required("BMKG_API_KEY")
	if err != nil {
		return Config{}, err
	}
	pvmbgToken, err := env.Required("PVMBG_TOKEN")
	if err != nil {
		return Config{}, err
	}
	rabbitUser, err := env.Required("RABBITMQ_USER")
	if err != nil {
		return Config{}, err
	}
	rabbitPassword, err := env.Required("RABBITMQ_PASSWORD")
	if err != nil {
		return Config{}, err
	}
	requestTimeout, err := env.Duration("REQUEST_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	return Config{
		Port:                 env.String("DEMO_API_PORT", "8086"),
		PVMBGURL:             env.String("PVMBG_URL", "http://pvmbg:8082"),
		PVMBGAdminKey:        pvmbgAdminKey,
		PVMBGToken:           pvmbgToken,
		BMKGURL:              env.String("BMKG_URL", "http://bmkg:8081"),
		BMKGAPIKey:           bmkgAPIKey,
		RabbitManagementURL:  env.String("RABBITMQ_MANAGEMENT_URL", "http://rabbitmq:15672"),
		RabbitUser:           rabbitUser,
		RabbitPassword:       rabbitPassword,
		ArtifactsDir:         env.String("ARTIFACTS_DIR", "/artifacts"),
		ClientAPIHealthURL:   env.String("CLIENT_API_HEALTH_URL", "http://client-api:8080/health"),
		AggregatorHealthURL:  env.String("AGGREGATOR_HEALTH_URL", "http://aggregator:8090/health"),
		BMKGHealthURL:        env.String("BMKG_HEALTH_URL", "http://bmkg:8081/health"),
		PVMBGHealthURL:       env.String("PVMBG_HEALTH_URL", "http://pvmbg:8082/health"),
		AuthAddr:             env.String("AUTH_ADDR", "auth:9091"),
		DashboardConsumerURL: env.String("DASHBOARD_CONSUMER_HEALTH_URL", "http://dashboard-consumer:8083/health"),
		NotifierConsumerURL:  env.String("NOTIFIER_CONSUMER_HEALTH_URL", "http://notifier-consumer:8084/health"),
		PortalConsumerURL:    env.String("PORTAL_CONSUMER_HEALTH_URL", "http://portal-consumer:8085/health"),
		RequestTimeout:       requestTimeout,
		QueueNames: []string{
			env.String("CONSUMER_QUEUE_DASHBOARD", "dashboard.hazard-events"),
			env.String("CONSUMER_QUEUE_NOTIFIER", "notifier.hazard-events"),
			env.String("CONSUMER_QUEUE_PORTAL", "portal.hazard-events"),
		},
	}, nil
}
