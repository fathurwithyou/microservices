package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.SetDialect("postgres")
	goose.SetBaseFS(migrations.FS)
	// Own version table: aggregator, satellite, and coastal migrations
	// have overlapping version numbers, and a shared goose_db_version made
	// later services believe their tables already existed.
	goose.SetTableName("coastal_goose_db_version")
}

// Migrate applies every embedded migration that has not run yet.
func Migrate(ctx context.Context, databaseURL string) error {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open migration connection: %w", err)
	}
	defer db.Close()
	if err := goose.UpContext(ctx, db, migrations.Dir); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
