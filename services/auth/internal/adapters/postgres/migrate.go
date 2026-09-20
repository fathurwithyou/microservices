package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/services/auth/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	schema, err := migrations.FS.ReadFile("001_init.sql")
	if err != nil {
		return fmt.Errorf("read auth migration: %w", err)
	}
	if _, err := pool.Exec(ctx, string(schema)); err != nil {
		return fmt.Errorf("apply auth migration: %w", err)
	}
	return nil
}
