// Package postgres provides a durable State adapter on pgx.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

// quarantineChunk bounds one multi-row INSERT (6 parameters per row, far
// below the 65535-parameter limit).
const quarantineChunk = 500

// SaveQuarantine stores one row per rejected reading. Rows go in multi-row
// INSERTs of quarantineChunk: a stale sensor replay quarantines thousands of
// readings per cycle, and one statement per row kept the poll loop busy for
// ~40 s per cycle while loading the store with ~160 statements per second.
func (s *State) SaveQuarantine(ctx context.Context, records []domain.QuarantineRecord) error {
	for start := 0; start < len(records); start += quarantineChunk {
		end := min(start+quarantineChunk, len(records))
		chunk := records[start:end]
		var query strings.Builder
		query.WriteString(`INSERT INTO coastal_quarantine (message_id, source, received_at, reason, retryable, payload) VALUES `)
		args := make([]any, 0, len(chunk)*6)
		for i, record := range chunk {
			if i > 0 {
				query.WriteString(",")
			}
			n := i * 6
			fmt.Fprintf(&query, "($%d,$%d,$%d,$%d,$%d,$%d)", n+1, n+2, n+3, n+4, n+5, n+6)
			args = append(args, record.MessageID, record.Source, record.ReceivedAt, record.Reason, record.Retryable, record.Payload)
		}
		if _, err := s.pool.Exec(ctx, query.String(), args...); err != nil {
			return fmt.Errorf("insert quarantine records %s..%s: %w", chunk[0].MessageID, chunk[len(chunk)-1].MessageID, err)
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

// Cursor returns the last committed sensor page cursor.
func (s *State) Cursor(ctx context.Context) (string, error) {
	var cursor string
	err := s.pool.QueryRow(ctx, `SELECT cursor FROM coastal_cursor WHERE source = $1`, domain.SourceCoastalSensor).Scan(&cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load sensor cursor: %w", err)
	}
	return cursor, nil
}

// SaveCursor commits the sensor page cursor.
func (s *State) SaveCursor(ctx context.Context, cursor string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO coastal_cursor (source, cursor)
VALUES ($1,$2)
ON CONFLICT (source) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()`, domain.SourceCoastalSensor, cursor)
	if err != nil {
		return fmt.Errorf("save sensor cursor: %w", err)
	}
	return nil
}
