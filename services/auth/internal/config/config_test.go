package config

import (
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/contracts/scopes"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("INTERNAL_SERVICE_TOKEN", "internal")
	t.Setenv("MEDIA_CLIENT_SECRET", "media")
	t.Setenv("FIELD_TEAM_CLIENT_SECRET", "field")
	t.Setenv("BNPB_OPS_CLIENT_SECRET", "ops")
}

func TestLoadRequiresAuthDatabaseURL(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("AUTH_DATABASE_URL", "")

	if _, err := Load(); err == nil {
		t.Fatal("Load succeeded without AUTH_DATABASE_URL")
	}
}

func TestLoadReadsAuthDatabaseConfiguration(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("AUTH_DATABASE_URL", "postgres://auth-db")
	t.Setenv("AUTH_DATABASE_WAIT", "7s")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURL != "postgres://auth-db" {
		t.Fatalf("database URL = %q", cfg.DatabaseURL)
	}
	if cfg.DatabaseWait.String() != "7s" {
		t.Fatalf("database wait = %s", cfg.DatabaseWait)
	}
}

func TestLoadRejectsMalformedDurations(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("AUTH_DATABASE_URL", "postgres://auth-db")
	t.Setenv("ACCESS_TOKEN_TTL", "one minute")

	if _, err := Load(); err == nil {
		t.Fatal("Load accepted a malformed ACCESS_TOKEN_TTL")
	}

	t.Setenv("ACCESS_TOKEN_TTL", "30s")
	t.Setenv("AUTH_DATABASE_WAIT", "soon")
	if _, err := Load(); err == nil {
		t.Fatal("Load accepted a malformed AUTH_DATABASE_WAIT")
	}
}

func TestLoadAssignsContractScopes(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("AUTH_DATABASE_URL", "postgres://auth-db")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	media := cfg.Clients["media-client"]
	if len(media.Scopes) != 1 || media.Scopes[0] != scopes.HazardReadSummary {
		t.Fatalf("media-client scopes = %v, want [%s]", media.Scopes, scopes.HazardReadSummary)
	}
	field := cfg.Clients["field-team-client"]
	if len(field.Scopes) != 2 || field.Scopes[0] != scopes.HazardReadSummary || field.Scopes[1] != scopes.HazardReadRaw {
		t.Fatalf("field-team-client scopes = %v", field.Scopes)
	}
	if cfg.GRPCPort != "9091" {
		t.Fatalf("default gRPC port = %q, want 9091", cfg.GRPCPort)
	}
	if cfg.AccessTokenTTL != 60*time.Second {
		t.Fatalf("default access token TTL = %s, want 1m0s", cfg.AccessTokenTTL)
	}
}
