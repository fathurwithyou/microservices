package postgres

import (
	"context"
	"fmt"

	"github.com/fathurwithyou/microservices/services/auth/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
