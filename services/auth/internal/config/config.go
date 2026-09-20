package config

import (
	"errors"
	"os"
	"time"

	"github.com/fathurwithyou/microservices/services/auth/internal/domain"
)

type Config struct {
	GRPCPort             string
	InternalServiceToken string
	AccessTokenTTL       time.Duration
	DatabaseURL          string
	DatabaseWait         time.Duration
	Clients              map[string]domain.Client
}

func Load() (Config, error) {
	internalServiceToken := os.Getenv("INTERNAL_SERVICE_TOKEN")
	if internalServiceToken == "" {
		return Config{}, errors.New("INTERNAL_SERVICE_TOKEN is required")
	}
	databaseURL := os.Getenv("AUTH_DATABASE_URL")
	if databaseURL == "" {
		return Config{}, errors.New("AUTH_DATABASE_URL is required")
	}

	clients := map[string]domain.Client{
		"media-client":      {Secret: os.Getenv("MEDIA_CLIENT_SECRET"), Scopes: []string{"hazard:read:summary"}},
		"field-team-client": {Secret: os.Getenv("FIELD_TEAM_CLIENT_SECRET"), Scopes: []string{"hazard:read:summary", "hazard:read:raw"}},
		"bnpb-ops-client":   {Secret: os.Getenv("BNPB_OPS_CLIENT_SECRET"), Scopes: []string{"hazard:read:summary", "hazard:read:raw"}},
	}
	for clientID, client := range clients {
		if client.Secret == "" {
			return Config{}, errors.New(clientID + " secret is required")
		}
	}

	return Config{
		GRPCPort:             valueOr("AUTH_GRPC_PORT", "9091"),
		InternalServiceToken: internalServiceToken,
		AccessTokenTTL:       durationOr("ACCESS_TOKEN_TTL", 60*time.Second),
		DatabaseURL:          databaseURL,
		DatabaseWait:         durationOr("AUTH_DATABASE_WAIT", 60*time.Second),
		Clients:              clients,
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
