package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/config"
)

func TestNewStoreRequiresDatabaseURLForPostgres(t *testing.T) {
	store, closeStore, err := newStore(context.Background(), config.Config{StoreDriver: "postgres"}, time.Minute)
	if err == nil {
		t.Fatal("expected missing database URL to fail")
	}
	if store != nil || closeStore != nil {
		t.Fatal("failed store initialization must not return a store or cleanup function")
	}
}

func TestNewStoreAllowsExplicitMemoryDriver(t *testing.T) {
	store, closeStore, err := newStore(context.Background(), config.Config{StoreDriver: "memory"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if store == nil || closeStore == nil {
		t.Fatal("explicit memory driver must return a usable store")
	}
	closeStore()
}

func TestRunRejectsUnknownCommand(t *testing.T) {
	if code := run(context.Background(), []string{"frobnicate"}, slog.Default()); code != 2 {
		t.Fatalf("unknown command exit = %d, want 2", code)
	}
}

func TestRunMigrateRequiresSubcommand(t *testing.T) {
	if code := run(context.Background(), []string{"migrate"}, slog.Default()); code != 2 {
		t.Fatalf("bare migrate exit = %d, want 2", code)
	}
	if code := run(context.Background(), []string{"migrate", "sideways"}, slog.Default()); code != 2 {
		t.Fatalf("unknown migrate subcommand exit = %d, want 2", code)
	}
}

func TestRunDownWithoutYesRefusesOnMemoryDriver(t *testing.T) {
	t.Setenv("STORE_DRIVER", "memory")
	if code := run(context.Background(), []string{"migrate", "down"}, slog.Default()); code != 0 {
		t.Fatalf("memory down exit = %d, want 0", code)
	}
}
