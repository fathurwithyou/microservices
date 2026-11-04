// Package postgres provides a durable State adapter on pgx.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// State persists quarantine records and the last accepted batch.
type State struct {
	pool *pgxpool.Pool
}

// New applies pending migrations, then opens the query pool.
func New(ctx context.Context, databaseURL string) (*State, error) {
	if err := Migrate(ctx, databaseURL); err != nil {
		return nil, err
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open database pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &State{pool: pool}, nil
}

// Close releases the connection pool.
func (s *State) Close() { s.pool.Close() }

// SaveQuarantine inserts one row per rejected reading.
func (s *State) SaveQuarantine(ctx context.Context, records []domain.QuarantineRecord) error {
	for _, record := range records {
		if _, err := s.pool.Exec(ctx,
			`INSERT INTO coastal_quarantine (message_id, source, received_at, reason, retryable, payload)
VALUES ($1,$2,$3,$4,$5,$6)`,
			record.MessageID, record.Source, record.ReceivedAt, record.Reason, record.Retryable, record.Payload,
		); err != nil {
			return fmt.Errorf("insert quarantine record %s: %w", record.MessageID, err)
		}
	}
	return nil
}

// SaveLastGood upserts the accepted batch as JSONB, keyed by source.
func (s *State) SaveLastGood(ctx context.Context, readings []domain.Reading, capturedAt time.Time) error {
	readingsJSON, err := json.Marshal(readings)
	if err != nil {
		return fmt.Errorf("marshal last good readings: %w", err)
	}
	// shortcut: domain.Reading has no Source field, and the coastal ingestor
	// only ingests one source; group by source once that field exists.
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO coastal_last_good (source, captured_at, readings)
VALUES ($1,$2,$3)
ON CONFLICT (source) DO UPDATE
SET captured_at = EXCLUDED.captured_at, readings = EXCLUDED.readings, updated_at = now()`,
		domain.SourceCoastalSensor, capturedAt, readingsJSON,
	); err != nil {
		return fmt.Errorf("upsert last good readings: %w", err)
	}
	return nil
}

// LastGood returns the most recent accepted batch, or zero values when the
// table is empty.
func (s *State) LastGood(ctx context.Context) ([]domain.Reading, time.Time, error) {
	var readingsJSON []byte
	var capturedAt time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT readings, captured_at FROM coastal_last_good ORDER BY captured_at DESC LIMIT 1`,
	).Scan(&readingsJSON, &capturedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, time.Time{}, nil
	}
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("load last good readings: %w", err)
	}
	var readings []domain.Reading
	if err := json.Unmarshal(readingsJSON, &readings); err != nil {
		return nil, time.Time{}, fmt.Errorf("decode last good readings: %w", err)
	}
	return readings, capturedAt, nil
}
