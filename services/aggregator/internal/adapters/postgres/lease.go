package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/pkg/lease"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LeaseStore persists one named lease using the database clock. It is safe for
// multiple aggregator replicas because acquire/renew are single statements.
type LeaseStore struct {
	pool *pgxpool.Pool
	name string
}

// LeaseStore returns a lease store for name backed by this repository's pool.
func (s *Repository) LeaseStore(name string) *LeaseStore {
	return &LeaseStore{pool: s.pool, name: name}
}

// Acquire takes the lease when it is free or expired. The database clock, not
// the caller's clock, decides expiry.
func (s *LeaseStore) Acquire(ctx context.Context, holder string, ttl time.Duration) (lease.Token, bool, error) {
	row := s.pool.QueryRow(ctx, `
INSERT INTO leader_lease (name, holder, fence, expires_at)
VALUES ($1, $2, 1, clock_timestamp() + $3::interval)
ON CONFLICT (name) DO UPDATE
SET holder = EXCLUDED.holder,
    fence = leader_lease.fence + 1,
    expires_at = EXCLUDED.expires_at
WHERE leader_lease.expires_at <= clock_timestamp()
RETURNING holder, fence, expires_at`, s.name, holder, intervalLiteral(ttl))
	token, err := scanLease(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return lease.Token{}, false, nil
	}
	if err != nil {
		return lease.Token{}, false, fmt.Errorf("acquire leader lease: %w", err)
	}
	return token, true, nil
}

// Renew extends a live lease owned by token.
func (s *LeaseStore) Renew(ctx context.Context, token lease.Token, ttl time.Duration) (lease.Token, bool, error) {
	row := s.pool.QueryRow(ctx, `
UPDATE leader_lease
SET expires_at = clock_timestamp() + $4::interval
WHERE name = $1 AND holder = $2 AND fence = $3 AND expires_at > clock_timestamp()
RETURNING holder, fence, expires_at`, s.name, token.Holder, token.Fence, intervalLiteral(ttl))
	renewed, err := scanLease(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return lease.Token{}, false, nil
	}
	if err != nil {
		return lease.Token{}, false, fmt.Errorf("renew leader lease: %w", err)
	}
	return renewed, true, nil
}

// Release removes the lease only when token still owns it.
func (s *LeaseStore) Release(ctx context.Context, token lease.Token) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM leader_lease WHERE name = $1 AND holder = $2 AND fence = $3`, s.name, token.Holder, token.Fence)
	if err != nil {
		return fmt.Errorf("release leader lease: %w", err)
	}
	return nil
}

func intervalLiteral(ttl time.Duration) string {
	return fmt.Sprintf("%.6f seconds", ttl.Seconds())
}

func scanLease(row pgx.Row) (lease.Token, error) {
	var token lease.Token
	err := row.Scan(&token.Holder, &token.Fence, &token.Expiry)
	return token, err
}
