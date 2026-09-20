package config

import "testing"

func TestLoadRequiresAuthDatabaseURL(t *testing.T) {
	t.Setenv("INTERNAL_SERVICE_TOKEN", "internal")
	t.Setenv("MEDIA_CLIENT_SECRET", "media")
	t.Setenv("FIELD_TEAM_CLIENT_SECRET", "field")
	t.Setenv("BNPB_OPS_CLIENT_SECRET", "ops")
	t.Setenv("AUTH_DATABASE_URL", "")

	if _, err := Load(); err == nil {
		t.Fatal("Load succeeded without AUTH_DATABASE_URL")
	}
}

func TestLoadReadsAuthDatabaseConfiguration(t *testing.T) {
	t.Setenv("INTERNAL_SERVICE_TOKEN", "internal")
	t.Setenv("MEDIA_CLIENT_SECRET", "media")
	t.Setenv("FIELD_TEAM_CLIENT_SECRET", "field")
	t.Setenv("BNPB_OPS_CLIENT_SECRET", "ops")
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
