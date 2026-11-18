package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/analytics"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/audit"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/hazards"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultWait = 60 * time.Second

type Repository struct {
	pool *pgxpool.Pool
	// health caches UnavailableUnits: every listing, analytics and audit
	// answer reports completeness, and the cluster metadata behind it is far
	// more expensive than the answer itself.
	health unitsCache
	// AnalyticsStaleness is how far behind now the analytics scan reads (see
	// Aggregate). Zero reads at present time; tests that write then read at
	// once need that.
	AnalyticsStaleness time.Duration
}

// defaultAnalyticsStaleness matches the analytics cache ttl: a dashboard that
// already accepts 5 s old counts loses nothing by reading a snapshot 5 s old,
// and the snapshot is older than every in-flight ingest transaction.
const defaultAnalyticsStaleness = 5 * time.Second

// unitsCacheTTL bounds how stale a completeness marker can be. A node that
// dies is reported within this time (plus one refresh) after CockroachDB
// marks it not live.
const unitsCacheTTL = 2 * time.Second

// unitsRefreshTimeout bounds one background refresh of the marker.
const unitsRefreshTimeout = 10 * time.Second

type unitsCache struct {
	mu         sync.Mutex
	at         time.Time
	units      []string
	known      bool
	refreshing bool
	// first is closed when the first answer (or failure) is in, for callers
	// that arrive before any value exists.
	first chan struct{}
	err   error
	// load replaces the store lookup in tests.
	load func(context.Context) ([]string, error)
}

func NewRepository(ctx context.Context, databaseURL string) (*Repository, error) {
	return NewRepositoryWithWait(ctx, databaseURL, defaultWait)
}

func NewRepositoryFromPool(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool, AnalyticsStaleness: defaultAnalyticsStaleness}
}

func NewRepositoryWithWait(ctx context.Context, databaseURL string, wait time.Duration) (*Repository, error) {
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
	return &Repository{pool: pool, AnalyticsStaleness: defaultAnalyticsStaleness}, nil
}
func (s *Repository) Close() { s.pool.Close() }

func (s *Repository) PersistHazardAndEnqueue(ctx context.Context, event domain.HazardEvent, message domain.OutboxMessage) error {
	attributes, err := json.Marshal(event.Attributes)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := checkWriteFence(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	defer tx.Rollback(ctx)
	var persistedID string
	err = tx.QueryRow(ctx, `INSERT INTO hazard_events (hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, ingested_at, attributes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (source, source_ref_id) DO UPDATE SET severity=EXCLUDED.severity, area_name=EXCLUDED.area_name, latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude, occurred_at=EXCLUDED.occurred_at, attributes=EXCLUDED.attributes, ingested_at=EXCLUDED.ingested_at WHERE hazard_events.severity IS DISTINCT FROM EXCLUDED.severity OR hazard_events.attributes IS DISTINCT FROM EXCLUDED.attributes OR hazard_events.area_name IS DISTINCT FROM EXCLUDED.area_name OR hazard_events.latitude IS DISTINCT FROM EXCLUDED.latitude OR hazard_events.longitude IS DISTINCT FROM EXCLUDED.longitude OR hazard_events.occurred_at IS DISTINCT FROM EXCLUDED.occurred_at RETURNING hazard_id`, event.HazardID, event.Source, event.SourceRefID, event.HazardType, event.Severity, event.AreaName, event.Latitude, event.Longitude, event.OccurredAt, event.IngestedAt, attributes).Scan(&persistedID)
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

func (s *Repository) GetHazard(ctx context.Context, id string) (domain.HazardEvent, error) {
	row := s.pool.QueryRow(ctx, `SELECT hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, ingested_at, attributes FROM hazard_events WHERE hazard_id=$1`, id)
	item, err := scan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		// Told apart from a store failure so the transport can answer 404
		// for the one and "unavailable" for the other.
		return domain.HazardEvent{}, domain.ErrNotFound
	}
	return item, err
}

// ListAuditEvents returns events ordered by ingested_at and hazard_id. The
// cursor is the last hazard_id on the previous page.
func (s *Repository) ListAuditEvents(ctx context.Context, filter audit.Filter) ([]domain.HazardEvent, error) {
	// AS OF SYSTEM TIME takes no placeholder; the literal is a formatted
	// time, never caller text.
	asOf := ""
	if !filter.AsOf.IsZero() {
		asOf = fmt.Sprintf(" AS OF SYSTEM TIME '%s'", filter.AsOf.UTC().Format(time.RFC3339Nano))
	}
	rows, err := s.pool.Query(ctx, `SELECT hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, ingested_at, attributes
FROM hazard_events`+asOf+`
WHERE ($1 = '' OR source = $1)
  AND ingested_at >= $2
  AND ($3 = '' OR ingested_at > $2 OR (ingested_at = $2 AND hazard_id > $3))
ORDER BY ingested_at ASC, hazard_id ASC
LIMIT $4`, string(filter.Source), filter.Since, filter.Cursor, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domain.HazardEvent, 0, filter.Limit)
	for rows.Next() {
		item, scanErr := scan(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// SourceRefIDs returns the source_ref_id values for checksum computation.
func (s *Repository) SourceRefIDs(ctx context.Context, source domain.Source) ([]string, error) {
	// The scan covers every event of the source; under concurrent writes it
	// can be asked to restart (SQLSTATE 40001), see retryRead.
	return retryRead(ctx, func(ctx context.Context) ([]string, error) {
		rows, err := s.pool.Query(ctx, `SELECT source_ref_id FROM hazard_events WHERE ($1 = '' OR source = $1) ORDER BY source_ref_id`, string(source))
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		ids := make([]string, 0)
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
		return ids, rows.Err()
	})
}

// PurgeSource deletes the events of one source in bounded chunks, so the
// delete never becomes one transaction large enough to be refused or to hold
// write intents across the whole table.
func (s *Repository) PurgeSource(ctx context.Context, source domain.Source, refPrefix string) (int64, error) {
	const chunk = 2000
	var total int64
	for {
		tag, err := s.pool.Exec(ctx,
			`DELETE FROM hazard_events WHERE source = $1 AND ($2 = '' OR source_ref_id LIKE $2 || '%') LIMIT $3`,
			string(source), refPrefix, chunk)
		if err != nil {
			return total, err
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < chunk {
			return total, nil
		}
	}
}

// SaveIngestFailure stores a raw rejected payload idempotently.
func (s *Repository) SaveIngestFailure(ctx context.Context, failure domain.IngestFailure) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO ingest_failures (message_id, source, received_at, reason, retryable, attempts, status, payload)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (message_id) DO NOTHING`, failure.MessageID, failure.Source, failure.ReceivedAt, failure.Reason, failure.Retryable, failure.Attempts, failure.Status, failure.Payload)
	return err
}

// ListIngestFailures returns failure metadata, oldest first. Payloads can be
// up to the 8 MiB response limit, so listings report only their size; use
// GetIngestFailure to read one message's bytes.
func (s *Repository) ListIngestFailures(ctx context.Context, filter domain.FailureFilter) ([]domain.IngestFailure, error) {
	rows, err := s.pool.Query(ctx, `SELECT message_id, source, received_at, reason, last_error, retryable, attempts, status, octet_length(payload)
FROM ingest_failures
WHERE ($1 = '' OR source = $1)
  AND (($5 = '' AND status IN ('parked','retrying')) OR status = $5)
  AND ($3 = '' OR received_at > $4 OR (received_at = $4 AND message_id > $3))
ORDER BY received_at ASC, message_id ASC LIMIT $2`, string(filter.Source), filter.Limit, filter.AfterID, filter.After, string(filter.Status))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domain.IngestFailure, 0, filter.Limit)
	for rows.Next() {
		var item domain.IngestFailure
		if err := rows.Scan(&item.MessageID, &item.Source, &item.ReceivedAt, &item.Reason, &item.LastError, &item.Retryable, &item.Attempts, &item.Status, &item.PayloadBytes); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// MarkIngestFailureRecovered removes a replayed payload from the active queue.
func (s *Repository) MarkIngestFailureRecovered(ctx context.Context, messageID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE ingest_failures SET status = 'recovered', attempts = attempts + 1 WHERE message_id = $1`, messageID)
	return err
}

// RecordIngestFailureAttempt counts one failed automatic retry. The original
// reason is kept; lastError records this attempt. Only rows still in the
// retry queue change, so a concurrent replay that already recovered the
// message wins.
func (s *Repository) RecordIngestFailureAttempt(ctx context.Context, messageID, lastError string, retryable, park bool) error {
	_, err := s.pool.Exec(ctx, `UPDATE ingest_failures
SET attempts = attempts + 1, last_error = $2, retryable = $3,
    status = CASE WHEN $4 THEN 'parked' ELSE status END
WHERE message_id = $1 AND status = 'retrying'`, messageID, lastError, retryable, park)
	return err
}

// GetIngestFailure returns one stored failure, including recovered ones.
func (s *Repository) GetIngestFailure(ctx context.Context, messageID string) (domain.IngestFailure, error) {
	var item domain.IngestFailure
	err := s.pool.QueryRow(ctx, `SELECT message_id, source, received_at, reason, last_error, retryable, attempts, status, payload
FROM ingest_failures WHERE message_id = $1`, messageID).Scan(&item.MessageID, &item.Source, &item.ReceivedAt, &item.Reason, &item.LastError, &item.Retryable, &item.Attempts, &item.Status, &item.Payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.IngestFailure{}, domain.ErrNotFound
	}
	item.PayloadBytes = len(item.Payload)
	return item, err
}

// HazardExists reports whether the canonical store contains hazard_id.
func (s *Repository) HazardExists(ctx context.Context, hazardID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM hazard_events WHERE hazard_id = $1)`, hazardID).Scan(&exists)
	return exists, err
}

// Aggregate counts one window per (bucket, category, severity, area) in one
// scan, inside a read-only transaction fixed AnalyticsStaleness behind now.
//
// A read at a timestamp older than every in-flight ingest transaction meets
// no write intents, so it never waits in the lock table. At present time the
// analytics scans spent up to 0.5 s there and 2-4 s in total under steady
// ingest (statement statistics, 18 Nov); a historical read costs the scan
// alone. The snapshot timestamp is the transaction's own and is returned as
// Snapshot.AsOf, so the answer states how old it is.
func (s *Repository) Aggregate(ctx context.Context, filter analytics.Filter) (analytics.Snapshot, error) {
	bucket := "day"
	switch filter.Bucket {
	case analytics.BucketHour:
		bucket = "hour"
	case analytics.BucketWeek:
		bucket = "week"
	}
	return retryRead(ctx, func(ctx context.Context) (analytics.Snapshot, error) {
		tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			return analytics.Snapshot{}, err
		}
		defer tx.Rollback(ctx) //nolint:errcheck // a failed rollback after commit is expected
		if s.AnalyticsStaleness > 0 {
			if _, err := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION AS OF SYSTEM TIME '-%dms'", s.AnalyticsStaleness.Milliseconds())); err != nil {
				return analytics.Snapshot{}, fmt.Errorf("historical read: %w", err)
			}
		}
		var hlc string
		if err := tx.QueryRow(ctx, `SELECT cluster_logical_timestamp()::STRING`).Scan(&hlc); err != nil {
			return analytics.Snapshot{}, fmt.Errorf("snapshot timestamp: %w", err)
		}
		asOf, err := hlcTime(hlc)
		if err != nil {
			return analytics.Snapshot{}, err
		}
		rows, err := tx.Query(ctx, `SELECT date_trunc($3, occurred_at) AS bucket, `+analytics.CategorySQL+` AS category, severity, area_name, count(*) FROM hazard_events WHERE occurred_at >= $1 AND occurred_at < $2 GROUP BY bucket, category, severity, area_name ORDER BY bucket, category, severity, area_name`, filter.Since, filter.Until, bucket)
		if err != nil {
			return analytics.Snapshot{}, err
		}
		defer rows.Close()
		snapshot := analytics.Snapshot{Rows: make([]analytics.Row, 0), AsOf: asOf}
		for rows.Next() {
			var row analytics.Row
			if err := rows.Scan(&row.Bucket, &row.Category, &row.Severity, &row.AreaName, &row.Count); err != nil {
				return analytics.Snapshot{}, err
			}
			snapshot.Rows = append(snapshot.Rows, row)
		}
		if err := rows.Err(); err != nil {
			return analytics.Snapshot{}, err
		}
		return snapshot, tx.Commit(ctx)
	})
}

// hlcTime converts a CockroachDB hybrid logical clock timestamp, as
// cluster_logical_timestamp() prints it ("<wall nanoseconds>.<logical>"),
// to a wall-clock time.
func hlcTime(value string) (time.Time, error) {
	wall, _, _ := strings.Cut(value, ".")
	nanos, err := strconv.ParseInt(wall, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse hlc timestamp %q: %w", value, err)
	}
	return time.Unix(0, nanos).UTC(), nil
}

// SaveFieldReport inserts a field report once. The bool reports whether this
// call created the record; a repeated report_id is an idempotent no-op.
func (s *Repository) SaveFieldReport(ctx context.Context, report domain.FieldReport) (bool, error) {
	tag, err := s.pool.Exec(ctx, `INSERT INTO field_reports (report_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, received_at, description, status)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (report_id) DO NOTHING`, report.ReportID, report.HazardType, report.Severity, report.AreaName, report.Latitude, report.Longitude, report.OccurredAt, report.ReceivedAt, report.Description, report.Status)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// FieldReportsByID returns the stored rows for ids, in any order.
func (s *Repository) FieldReportsByID(ctx context.Context, ids []string) ([]domain.FieldReport, error) {
	rows, err := s.pool.Query(ctx, `SELECT report_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, received_at, description, status, attempts, processed_at
FROM field_reports WHERE report_id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	reports := make([]domain.FieldReport, 0, len(ids))
	for rows.Next() {
		report, scanErr := scanFieldReport(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		reports = append(reports, report)
	}
	return reports, rows.Err()
}

// ListAcceptedFieldReports returns accepted reports in arrival order.
func (s *Repository) ListAcceptedFieldReports(ctx context.Context, limit int) ([]domain.FieldReport, error) {
	rows, err := s.pool.Query(ctx, `SELECT report_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, received_at, description, status, attempts, processed_at
FROM field_reports WHERE status = 'accepted' ORDER BY received_at ASC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	reports := make([]domain.FieldReport, 0, limit)
	for rows.Next() {
		report, scanErr := scanFieldReport(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		reports = append(reports, report)
	}
	return reports, rows.Err()
}

// MarkFieldReport moves one accepted report to its terminal state and reports
// whether this call made the transition. Every Aggregator instance runs the
// worker; the conditional UPDATE lets exactly one of them win each report.
func (s *Repository) MarkFieldReport(ctx context.Context, reportID string, status domain.FieldReportStatus, processedAt time.Time) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE field_reports SET status = $2, attempts = attempts + 1, processed_at = $3 WHERE report_id = $1 AND status = 'accepted'`, reportID, status, processedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func scanFieldReport(row scanner) (domain.FieldReport, error) {
	var report domain.FieldReport
	var processedAt *time.Time
	err := row.Scan(&report.ReportID, &report.HazardType, &report.Severity, &report.AreaName, &report.Latitude, &report.Longitude, &report.OccurredAt, &report.ReceivedAt, &report.Description, &report.Status, &report.Attempts, &processedAt)
	if err != nil {
		return domain.FieldReport{}, err
	}
	report.ProcessedAt = processedAt
	return report, nil
}

func (s *Repository) ListHazards(ctx context.Context, filter hazards.ListFilter) ([]domain.HazardEvent, error) {
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
	cursor, after, err := hazards.DecodePageToken(filter.PageToken)
	if err != nil {
		return nil, err
	}
	if after {
		args = append(args, cursor.OccurredAt, cursor.HazardID)
		query += fmt.Sprintf(" AND (occurred_at, hazard_id) < ($%d, $%d)", len(args)-1, len(args))
	}
	// hazard_id breaks occurred_at ties so keyset pages form a total order.
	query += " ORDER BY occurred_at DESC, hazard_id DESC LIMIT $" + fmt.Sprint(len(args)+1)
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

func (s *Repository) SetSourceStatus(ctx context.Context, status domain.SourceStatus) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO source_status (source,status,last_success_at,stale_since,last_error) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (source) DO UPDATE SET status=EXCLUDED.status,last_success_at=EXCLUDED.last_success_at,stale_since=EXCLUDED.stale_since,last_error=EXCLUDED.last_error`, status.Source, status.Status, status.LastSuccessAt, status.StaleSince, status.LastError)
	return err
}
func (s *Repository) GetSourceStatus(ctx context.Context, source domain.Source) (domain.SourceStatus, error) {
	var result domain.SourceStatus
	err := s.pool.QueryRow(ctx, `SELECT source,status,last_success_at,stale_since,last_error FROM source_status WHERE source=$1`, source).Scan(&result.Source, &result.Status, &result.LastSuccessAt, &result.StaleSince, &result.LastError)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.SourceStatus{Source: source, Status: domain.SourceStateUnknown}, nil
	}
	if err != nil {
		return domain.SourceStatus{}, err
	}
	return result, nil
}

func (s *Repository) GetPollCursor(ctx context.Context, source domain.Source) (time.Time, error) {
	var cursor time.Time
	err := s.pool.QueryRow(ctx, `SELECT cursor_at FROM poll_cursors WHERE source=$1`, source).Scan(&cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	return cursor, err
}

func (s *Repository) AdvancePollCursor(ctx context.Context, source domain.Source, cursor time.Time) error {
	_, err := s.pool.Exec(ctx, `
INSERT INTO poll_cursors (source, cursor_at)
VALUES ($1, $2)
ON CONFLICT (source) DO UPDATE
SET cursor_at = EXCLUDED.cursor_at
WHERE poll_cursors.cursor_at < EXCLUDED.cursor_at`, source, cursor)
	return err
}

func (s *Repository) LoadVolcanoReferences(ctx context.Context) (map[string]domain.VolcanoReference, error) {
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

func (s *Repository) ClaimPendingOutbox(ctx context.Context, limit int, leaseDuration time.Duration) ([]domain.OutboxMessage, error) {
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

func (s *Repository) MarkOutboxPublished(ctx context.Context, id int64, leaseToken string) error {
	result, err := s.pool.Exec(ctx, `UPDATE outbox_messages SET published_at = now(), lease_until = NULL, lease_token = NULL WHERE message_id = $1 AND published_at IS NULL AND lease_token = $2`, id, leaseToken)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("outbox message %d lease lost", id)
	}
	return nil
}

func (s *Repository) ReleaseOutbox(ctx context.Context, id int64, leaseToken string, nextAttempt time.Time) error {
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

// HazardLocations reads the range metadata for one hazard row and maps replica
// store IDs to the node names declared in the CockroachDB localities.
func (s *Repository) HazardLocations(ctx context.Context, hazardID string) ([]audit.Location, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT range_id, replicas, lease_holder
		 FROM [SHOW RANGE FROM TABLE hazard_events FOR ROW ($1::STRING)]`, hazardID)
	if err != nil {
		return nil, fmt.Errorf("show range: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	var (
		rangeID int64
		replica []int64
		lease   int64
	)
	if err := rows.Scan(&rangeID, &replica, &lease); err != nil {
		return nil, fmt.Errorf("scan range: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	names, err := s.nodeNames(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(replica, func(i, j int) bool { return replica[i] < replica[j] })
	locations := make([]audit.Location, 0, len(replica))
	replicaIndex := 0
	for _, nodeID := range replica {
		name := names[nodeID]
		if name == "" {
			name = fmt.Sprintf("node-%d", nodeID)
		}
		if nodeID == lease {
			locations = append(locations, audit.Location{Node: name, Unit: fmt.Sprintf("shard-%d", rangeID), Role: "primary"})
			continue
		}
		replicaIndex++
		locations = append(locations, audit.Location{Node: name, Unit: fmt.Sprintf("shard-%d-r%d", rangeID, replicaIndex), Role: "replica"})
	}
	// Keep the lease holder first so callers see the writable copy at index 0.
	sort.SliceStable(locations, func(i, j int) bool { return locations[i].Role == "primary" && locations[j].Role != "primary" })
	return locations, nil
}

func (s *Repository) nodeNames(ctx context.Context) (map[int64]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT node_id, locality, address FROM crdb_internal.gossip_nodes`)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	defer rows.Close()
	names := make(map[int64]string)
	for rows.Next() {
		var (
			nodeID   int64
			locality string
			address  string
		)
		if err := rows.Scan(&nodeID, &locality, &address); err != nil {
			return nil, err
		}
		names[nodeID] = nodeName(locality, address)
	}
	return names, rows.Err()
}

func nodeName(locality, address string) string {
	for _, part := range strings.Split(locality, ",") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(part), "cloud="); ok {
			return value
		}
	}
	return address
}

type storeNode struct {
	name string
	live bool
}

func (s *Repository) storeNodes(ctx context.Context) (map[int64]storeNode, error) {
	rows, err := s.pool.Query(ctx, `SELECT node_id, locality, address, is_live FROM crdb_internal.gossip_nodes`)
	if err != nil {
		return nil, fmt.Errorf("list store nodes: %w", err)
	}
	defer rows.Close()
	nodes := make(map[int64]storeNode)
	for rows.Next() {
		var (
			nodeID   int64
			locality string
			address  string
			live     bool
		)
		if err := rows.Scan(&nodeID, &locality, &address, &live); err != nil {
			return nil, err
		}
		nodes[nodeID] = storeNode{name: nodeName(locality, address), live: live}
	}
	return nodes, rows.Err()
}

// UnavailableUnits returns the store nodes that are not live and still hold
// a replica of this database. A dead node whose replicas have all been
// re-created elsewhere no longer affects completeness, so answers stop being
// marked partial once the cluster has healed, even though CockroachDB keeps
// reporting the dead node ID until it is decommissioned. When the range
// metadata cannot be read, every dead node counts (fail towards partial).
// UnavailableUnits answers from a cache refreshed in the background
// (stale-while-revalidate). With dead store nodes the lookup reads range
// placement, which takes seconds under load; computing it inline under a
// lock stalled every listing behind it each time the cache expired. Now at
// most one refresh runs, callers get the last known answer immediately, and
// only callers that arrive before any answer exists wait for the first one.
func (s *Repository) UnavailableUnits(ctx context.Context) ([]string, error) {
	h := &s.health
	h.mu.Lock()
	if h.known {
		units := append([]string(nil), h.units...)
		if time.Since(h.at) >= unitsCacheTTL && !h.refreshing {
			h.refreshing = true
			go s.refreshUnits()
		}
		h.mu.Unlock()
		return units, nil
	}
	if h.first == nil {
		h.first = make(chan struct{})
		h.refreshing = true
		go s.refreshUnits()
	}
	first := h.first
	h.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-first:
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.known {
		return nil, h.err
	}
	return append([]string(nil), h.units...), nil
}

func (s *Repository) refreshUnits() {
	ctx, cancel := context.WithTimeout(context.Background(), unitsRefreshTimeout)
	defer cancel()
	load := s.health.load
	if load == nil {
		load = s.unavailableUnits
	}
	units, err := load(ctx)
	h := &s.health
	h.mu.Lock()
	defer h.mu.Unlock()
	h.refreshing = false
	if err == nil {
		h.units, h.at, h.known = units, time.Now(), true
	}
	h.err = err
	if h.first != nil && (h.known || err != nil) {
		select {
		case <-h.first:
		default:
			close(h.first)
		}
		if !h.known {
			// Let the next caller start a fresh first attempt.
			h.first = nil
		}
	}
}

func (s *Repository) unavailableUnits(ctx context.Context) ([]string, error) {
	nodes, err := s.storeNodes(ctx)
	if err != nil {
		return nil, err
	}
	dead := make(map[int64]string)
	for id, node := range nodes {
		if !node.live {
			dead[id] = node.name
		}
	}
	if len(dead) == 0 {
		return nil, nil
	}
	holding, err := s.replicaHolders(ctx)
	names := make(map[string]bool)
	for id, name := range dead {
		if err != nil || holding[id] {
			names[name] = true
		}
	}
	units := make([]string, 0, len(names))
	for name := range names {
		units = append(units, name)
	}
	sort.Strings(units)
	return units, nil
}

// replicaHolders returns the node IDs holding any replica of the current
// database's ranges.
func (s *Repository) replicaHolders(ctx context.Context) (map[int64]bool, error) {
	var database string
	if err := s.pool.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT unnest(replicas) FROM [SHOW RANGES FROM DATABASE `+pgx.Identifier{database}.Sanitize()+`]`)
	if err != nil {
		return nil, fmt.Errorf("show ranges: %w", err)
	}
	defer rows.Close()
	holders := make(map[int64]bool)
	for rows.Next() {
		var nodeID int64
		if err := rows.Scan(&nodeID); err != nil {
			return nil, err
		}
		holders[nodeID] = true
	}
	return holders, rows.Err()
}

// HazardDistribution counts, per store node, the hazard_events range
// replicas and leases it holds and the bytes of those replicas.
func (s *Repository) HazardDistribution(ctx context.Context) (audit.Distribution, error) {
	nodes, err := s.storeNodes(ctx)
	if err != nil {
		return audit.Distribution{}, err
	}
	rows, err := s.pool.Query(ctx, `SELECT lease_holder, replicas, range_size FROM [SHOW RANGES FROM TABLE hazard_events WITH DETAILS]`)
	if err != nil {
		return audit.Distribution{}, fmt.Errorf("show ranges: %w", err)
	}
	defer rows.Close()
	shares := make(map[int64]*audit.NodeShare)
	share := func(id int64) *audit.NodeShare {
		if existing, ok := shares[id]; ok {
			return existing
		}
		node := nodes[id]
		name := node.name
		if name == "" {
			name = fmt.Sprintf("node-%d", id)
		}
		created := &audit.NodeShare{Node: name, NodeID: id, Live: node.live}
		shares[id] = created
		return created
	}
	for id := range nodes {
		share(id)
	}
	var result audit.Distribution
	for rows.Next() {
		var (
			lease    int64
			replicas []int64
			size     int64
		)
		if err := rows.Scan(&lease, &replicas, &size); err != nil {
			return audit.Distribution{}, err
		}
		result.Ranges++
		result.LogicalBytes += size
		for _, id := range replicas {
			holder := share(id)
			holder.Replicas++
			holder.ReplicaBytes += size
		}
		if lease != 0 {
			share(lease).Leaseholders++
		}
	}
	if err := rows.Err(); err != nil {
		return audit.Distribution{}, err
	}
	for _, item := range shares {
		result.Nodes = append(result.Nodes, *item)
	}
	sort.Slice(result.Nodes, func(i, j int) bool { return result.Nodes[i].NodeID < result.Nodes[j].NodeID })
	return result, nil
}

// checkWriteFence verifies, inside the write transaction, that the lease term
// attached to ctx is still current by the database clock. Under SERIALIZABLE
// a concurrent takeover that rewrites the lease row conflicts with this read,
// so the write either commits under a current term or not at all.
func checkWriteFence(ctx context.Context, tx pgx.Tx) error {
	fence, ok := domain.WriteFenceFrom(ctx)
	if !ok {
		return nil
	}
	var held bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM leader_lease WHERE name = $1 AND holder = $2 AND fence = $3 AND expires_at > clock_timestamp())`,
		fence.Lease, fence.Holder, fence.Fence).Scan(&held)
	if err != nil {
		return fmt.Errorf("check write fence: %w", err)
	}
	if !held {
		return domain.ErrFenced
	}
	return nil
}

// BulkPersistHazards inserts a historical catalog without emitting outbox
// messages. Existing rows are left untouched.
func (s *Repository) BulkPersistHazards(ctx context.Context, events []domain.HazardEvent) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}
	// One multi-row INSERT per chunk, each committed on its own. A single
	// transaction over the whole catalog holds write intents on every row
	// until commit, so concurrent reads of those keys block and a 100k-row
	// import takes minutes. Chunks keep each transaction small; ON CONFLICT
	// DO NOTHING makes a re-run after a partial failure safe.
	inserted := 0
	for start := 0; start < len(events); start += bulkInsertChunk {
		end := min(start+bulkInsertChunk, len(events))
		count, err := s.insertHazardChunk(ctx, events[start:end])
		if err != nil {
			return inserted, fmt.Errorf("insert rows %d-%d: %w", start+1, end, err)
		}
		inserted += count
	}
	return inserted, nil
}

// bulkInsertChunk is the row count per catalog INSERT. 11 parameters per row
// keeps a chunk well under the 65535 placeholder limit.
const bulkInsertChunk = 500

func (s *Repository) insertHazardChunk(ctx context.Context, events []domain.HazardEvent) (int, error) {
	const columns = 11
	var query strings.Builder
	query.WriteString(`INSERT INTO hazard_events (hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, ingested_at, attributes) VALUES `)
	args := make([]any, 0, len(events)*columns)
	for index, event := range events {
		attributes, err := json.Marshal(event.Attributes)
		if err != nil {
			return 0, err
		}
		if index > 0 {
			query.WriteByte(',')
		}
		base := index * columns
		query.WriteByte('(')
		for column := 1; column <= columns; column++ {
			if column > 1 {
				query.WriteByte(',')
			}
			fmt.Fprintf(&query, "$%d", base+column)
		}
		query.WriteByte(')')
		args = append(args, event.HazardID, event.Source, event.SourceRefID, event.HazardType, event.Severity,
			event.AreaName, event.Latitude, event.Longitude, event.OccurredAt, event.IngestedAt, attributes)
	}
	query.WriteString(` ON CONFLICT (source, source_ref_id) DO NOTHING`)
	tag, err := s.pool.Exec(ctx, query.String(), args...)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}
