package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/hazards"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultWait = 60 * time.Second

type Store struct{ pool *pgxpool.Pool }

func New(ctx context.Context, databaseURL string) (*Store, error) {
	return NewWithWait(ctx, databaseURL, defaultWait)
}

func NewFromPool(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func NewWithWait(ctx context.Context, databaseURL string, wait time.Duration) (*Store, error) {
	if err := Up(ctx, databaseURL, wait); err != nil {
		return nil, err
	}
	pool, err := Connect(ctx, databaseURL, wait)
	if err != nil {
		return nil, err
	}
	if err := Seed(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("seed canonical reference data: %w", err)
	}
	return &Store{pool: pool}, nil
}
func (s *Store) Close() { s.pool.Close() }

func (s *Store) PersistHazardAndEnqueue(ctx context.Context, event domain.HazardEvent, message domain.OutboxMessage) error {
	attributes, err := json.Marshal(event.Attributes)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var persistedID string
	err = tx.QueryRow(ctx, `INSERT INTO hazard_events (hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, ingested_at, attributes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (source, source_ref_id) DO UPDATE SET severity=EXCLUDED.severity, attributes=EXCLUDED.attributes, ingested_at=EXCLUDED.ingested_at WHERE hazard_events.severity IS DISTINCT FROM EXCLUDED.severity OR hazard_events.attributes IS DISTINCT FROM EXCLUDED.attributes RETURNING hazard_id`, event.HazardID, event.Source, event.SourceRefID, event.HazardType, event.Severity, event.AreaName, event.Latitude, event.Longitude, event.OccurredAt, event.IngestedAt, attributes).Scan(&persistedID)
	if err == pgx.ErrNoRows {
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	message.HazardID = persistedID
	if _, err := tx.Exec(ctx, `INSERT INTO outbox_messages (hazard_id, correlation_id, routing_key, payload) VALUES ($1,$2,$3,$4)`, message.HazardID, message.CorrelationID, message.RoutingKey, message.Payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) GetHazard(ctx context.Context, id string) (domain.HazardEvent, error) {
	row := s.pool.QueryRow(ctx, `SELECT hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, ingested_at, attributes FROM hazard_events WHERE hazard_id=$1`, id)
	return scan(row)
}

func (s *Store) ListHazards(ctx context.Context, filter hazards.ListFilter) ([]domain.HazardEvent, error) {
	query := `SELECT hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, ingested_at, attributes FROM hazard_events WHERE 1=1`
	args := make([]any, 0, 4)
	if filter.Source != "" {
		args = append(args, filter.Source)
		query += fmt.Sprintf(" AND source=$%d", len(args))
	}
	if filter.HazardType != "" {
		args = append(args, filter.HazardType)
		query += fmt.Sprintf(" AND hazard_type=$%d", len(args))
	}
	if filter.Severity != "" {
		args = append(args, filter.Severity)
		query += fmt.Sprintf(" AND severity=$%d", len(args))
	}
	query += " ORDER BY occurred_at DESC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, limit(filter.PageSize))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domain.HazardEvent, 0)
	for rows.Next() {
		item, scanErr := scan(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) SetSourceStatus(ctx context.Context, status domain.SourceStatus) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO source_status (source,status,last_success_at,stale_since,last_error) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (source) DO UPDATE SET status=EXCLUDED.status,last_success_at=EXCLUDED.last_success_at,stale_since=EXCLUDED.stale_since,last_error=EXCLUDED.last_error`, status.Source, status.Status, status.LastSuccessAt, status.StaleSince, status.LastError)
	return err
}
func (s *Store) GetSourceStatus(ctx context.Context, source domain.Source) (domain.SourceStatus, error) {
	var result domain.SourceStatus
	err := s.pool.QueryRow(ctx, `SELECT source,status,last_success_at,stale_since,last_error FROM source_status WHERE source=$1`, source).Scan(&result.Source, &result.Status, &result.LastSuccessAt, &result.StaleSince, &result.LastError)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.SourceStatus{Source: source, Status: "unknown"}, nil
	}
	if err != nil {
		return domain.SourceStatus{}, err
	}
	return result, nil
}

func (s *Store) GetPollCursor(ctx context.Context, source domain.Source) (time.Time, error) {
	var cursor time.Time
	err := s.pool.QueryRow(ctx, `SELECT cursor_at FROM poll_cursors WHERE source=$1`, source).Scan(&cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	return cursor, err
}

func (s *Store) AdvancePollCursor(ctx context.Context, source domain.Source, cursor time.Time) error {
	_, err := s.pool.Exec(ctx, `
INSERT INTO poll_cursors (source, cursor_at)
VALUES ($1, $2)
ON CONFLICT (source) DO UPDATE
SET cursor_at = EXCLUDED.cursor_at
WHERE poll_cursors.cursor_at < EXCLUDED.cursor_at`, source, cursor)
	return err
}

func (s *Store) LoadVolcanoReferences(ctx context.Context) (map[string]domain.VolcanoReference, error) {
	rows, err := s.pool.Query(ctx, `SELECT volcano_id, name, latitude, longitude FROM volcano_reference ORDER BY volcano_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	references := make(map[string]domain.VolcanoReference)
	for rows.Next() {
		var id string
		var reference domain.VolcanoReference
		if err := rows.Scan(&id, &reference.Name, &reference.Latitude, &reference.Longitude); err != nil {
			return nil, err
		}
		references[id] = reference
	}
	return references, rows.Err()
}

func (s *Store) ClaimPendingOutbox(ctx context.Context, limit int, leaseDuration time.Duration) ([]domain.OutboxMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	leaseToken := uuid.NewString()
	leaseUntil := time.Now().UTC().Add(leaseDuration)
	rows, err := s.pool.Query(ctx, `
WITH candidates AS (
  SELECT message_id
  FROM outbox_messages
  WHERE published_at IS NULL
    AND available_at <= now()
    AND (lease_until IS NULL OR lease_until <= now())
  ORDER BY message_id
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE outbox_messages AS messages
SET lease_until = $2, lease_token = $3, attempts = messages.attempts + 1
FROM candidates
WHERE messages.message_id = candidates.message_id
RETURNING messages.message_id, messages.hazard_id, messages.correlation_id, messages.routing_key, messages.payload, messages.attempts`, limit, leaseUntil, leaseToken)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := make([]domain.OutboxMessage, 0, limit)
	for rows.Next() {
		message := domain.OutboxMessage{LeaseToken: leaseToken}
		if err := rows.Scan(&message.ID, &message.HazardID, &message.CorrelationID, &message.RoutingKey, &message.Payload, &message.Attempts); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

func (s *Store) MarkOutboxPublished(ctx context.Context, id int64, leaseToken string) error {
	result, err := s.pool.Exec(ctx, `UPDATE outbox_messages SET published_at = now(), lease_until = NULL, lease_token = NULL WHERE message_id = $1 AND published_at IS NULL AND lease_token = $2`, id, leaseToken)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("outbox message %d lease lost", id)
	}
	return nil
}

func (s *Store) ReleaseOutbox(ctx context.Context, id int64, leaseToken string, nextAttempt time.Time) error {
	result, err := s.pool.Exec(ctx, `UPDATE outbox_messages SET available_at = $3, lease_until = NULL, lease_token = NULL WHERE message_id = $1 AND published_at IS NULL AND lease_token = $2`, id, leaseToken, nextAttempt)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("outbox message %d lease lost", id)
	}
	return nil
}

type scanner interface{ Scan(...any) error }

func scan(row scanner) (domain.HazardEvent, error) {
	var item domain.HazardEvent
	var attributes []byte
	err := row.Scan(&item.HazardID, &item.Source, &item.SourceRefID, &item.HazardType, &item.Severity, &item.AreaName, &item.Latitude, &item.Longitude, &item.OccurredAt, &item.IngestedAt, &attributes)
	if err != nil {
		return item, err
	}
	if json.Unmarshal(attributes, &item.Attributes) != nil {
		item.Attributes = map[string]any{}
	}
	return item, nil
}
func limit(value int) int {
	if value <= 0 || value > 100 {
		return 100
	}
	return value
}
