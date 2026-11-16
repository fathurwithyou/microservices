package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
		pool, err := newPool(ctx, databaseURL)
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

// newPool opens a pool whose connections may read crdb_internal. The Audit
// API and the partial-result flags depend on crdb_internal.gossip_nodes, which
// newer CockroachDB releases hide behind allow_unsafe_internals. Servers that
// do not know the variable (PostgreSQL, older CockroachDB) answer 42704 and
// are left as they are.
// defaultPoolMaxConns is the per-instance connection cap when the DSN sets none.
const defaultPoolMaxConns = 16

func newPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	// pgxpool defaults to max(4, NumCPU) connections: on a 2-vCPU node that is
	// 4 concurrent queries per instance, which caps reads long before the
	// instance or the store is busy (M2 P2.7). An explicit pool_max_conns in
	// the DSN still wins.
	if !strings.Contains(databaseURL, "pool_max_conns") {
		config.MaxConns = defaultPoolMaxConns
	}
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET allow_unsafe_internals = true")
		var pgErr *pgconn.PgError
		if err != nil && errors.As(err, &pgErr) && pgErr.Code == "42704" {
			return nil
		}
		return err
	}
	return pgxpool.NewWithConfig(ctx, config)
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
