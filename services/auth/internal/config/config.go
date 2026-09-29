package config

import (
	"time"

	"github.com/fathurwithyou/microservices/contracts/scopes"
	"github.com/fathurwithyou/microservices/pkg/env"
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

// Load reads the auth service configuration from the environment. Malformed
// values are errors, so a bad ACCESS_TOKEN_TTL or AUTH_DATABASE_WAIT fails
// startup instead of silently falling back to a default.
func Load() (Config, error) {
	internalServiceToken, err := env.Required("INTERNAL_SERVICE_TOKEN")
	if err != nil {
		return Config{}, err
	}
	databaseURL, err := env.Required("AUTH_DATABASE_URL")
	if err != nil {
		return Config{}, err
	}
	grpcPort := env.String("AUTH_GRPC_PORT", "9091")
	accessTokenTTL, err := env.Duration("ACCESS_TOKEN_TTL", 60*time.Second)
	if err != nil {
		return Config{}, err
	}
	databaseWait, err := env.Duration("AUTH_DATABASE_WAIT", 60*time.Second)
	if err != nil {
		return Config{}, err
	}

	mediaSecret, err := env.Required("MEDIA_CLIENT_SECRET")
	if err != nil {
		return Config{}, err
	}
	fieldTeamSecret, err := env.Required("FIELD_TEAM_CLIENT_SECRET")
	if err != nil {
		return Config{}, err
	}
	opsSecret, err := env.Required("BNPB_OPS_CLIENT_SECRET")
	if err != nil {
		return Config{}, err
	}
	clients := map[string]domain.Client{
		"media-client":      {Secret: mediaSecret, Scopes: []string{scopes.HazardReadSummary}},
		"field-team-client": {Secret: fieldTeamSecret, Scopes: []string{scopes.HazardReadSummary, scopes.HazardReadRaw}},
		"bnpb-ops-client":   {Secret: opsSecret, Scopes: []string{scopes.HazardReadSummary, scopes.HazardReadRaw}},
	}

	return Config{
		GRPCPort:             grpcPort,
		InternalServiceToken: internalServiceToken,
		AccessTokenTTL:       accessTokenTTL,
		DatabaseURL:          databaseURL,
		DatabaseWait:         databaseWait,
		Clients:              clients,
	}, nil
}
