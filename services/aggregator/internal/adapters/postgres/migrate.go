package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.SetDialect("postgres")
	goose.SetBaseFS(migrations.FS)
}

func Connect(ctx context.Context, databaseURL string, wait time.Duration) (*pgxpool.Pool, error) {
	deadline := time.Now().Add(wait)
	var lastErr error
	for {
		pool, err := pgxpool.New(ctx, databaseURL)
		if err == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				return pool, nil
			} else {
				lastErr = pingErr
				pool.Close()
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("PostgreSQL not ready after %s: %w", wait, lastErr)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func Up(ctx context.Context, databaseURL string, wait time.Duration) error {
	db, err := openMigrationDB(ctx, databaseURL, wait)
	if err != nil {
		return err
	}
	defer db.Close()
	return goose.UpContext(ctx, db, migrations.Dir)
}

func Down(ctx context.Context, databaseURL string, wait time.Duration, confirm bool) error {
	if !confirm {
		return errors.New("refusing to roll back without confirmation; re-run with --yes")
	}
	db, err := openMigrationDB(ctx, databaseURL, wait)
	if err != nil {
		return err
	}
	defer db.Close()
	return goose.DownContext(ctx, db, migrations.Dir)
}

func Status(ctx context.Context, databaseURL string, wait time.Duration) error {
	db, err := openMigrationDB(ctx, databaseURL, wait)
	if err != nil {
		return err
	}
	defer db.Close()
	return goose.StatusContext(ctx, db, migrations.Dir)
}

func openMigrationDB(ctx context.Context, databaseURL string, wait time.Duration) (*sql.DB, error) {
	deadline := time.Now().Add(wait)
	var lastErr error
	for {
		db, err := sql.Open("pgx", databaseURL)
		if err == nil {
			if pingErr := db.PingContext(ctx); pingErr == nil {
				return db, nil
			} else {
				lastErr = pingErr
				db.Close()
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("PostgreSQL not ready after %s: %w", wait, lastErr)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
