// Package postgres provides the durable pgx State adapter.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/ports"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

const consumerName = "satellite-ingestor"

func init() {
	goose.SetDialect("postgres")
	goose.SetBaseFS(migrations.FS)
	// Own version table: aggregator, satellite, and coastal migrations
	// have overlapping version numbers, and a shared goose_db_version made
	// later services believe their tables already existed.
	goose.SetTableName("satellite_goose_db_version")
}

// State implements ports.State on a pgx pool.
type State struct{ pool *pgxpool.Pool }

// New applies pending migrations, connects, and returns a State.
func New(ctx context.Context, databaseURL string) (*State, error) {
	if err := Up(ctx, databaseURL); err != nil {
		return nil, err
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect database: %w", err)
	}
	return &State{pool: pool}, nil
}

// NewFromPool wraps an existing pool without migrating.
func NewFromPool(pool *pgxpool.Pool) *State { return &State{pool: pool} }

// Close releases the connection pool.
func (s *State) Close() { s.pool.Close() }

// Up applies all pending goose migrations.
func Up(ctx context.Context, databaseURL string) error {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open migration connection: %w", err)
	}
	defer db.Close()
	if err := goose.UpContext(ctx, db, migrations.Dir); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	return nil
}

const (
	metaSession   = "session"
	metaWatermark = "watermark"
	counterPrefix = "count:"
)

// Load returns the committed offset, working set, active fires and meta.
func (s *State) Load(ctx context.Context) (ports.Snapshot, error) {
	snapshot := ports.Snapshot{Counters: map[string]int64{}}
	err := s.pool.QueryRow(ctx,
		`SELECT offset_value FROM satellite_feed_offset WHERE consumer = $1`, consumerName).Scan(&snapshot.Offset)
	if err != nil && err != pgx.ErrNoRows {
		return ports.Snapshot{}, fmt.Errorf("load feed offset: %w", err)
	}

	rows, err := s.pool.Query(ctx, `SELECT name, value FROM satellite_meta`)
	if err != nil {
		return ports.Snapshot{}, fmt.Errorf("query meta: %w", err)
	}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			rows.Close()
			return ports.Snapshot{}, fmt.Errorf("scan meta: %w", err)
		}
		switch {
		case name == metaSession:
			snapshot.Session = value
		case name == metaWatermark:
			if at, err := time.Parse(time.RFC3339, value); err == nil {
				snapshot.Watermark = at.UTC()
			}
		case len(name) > len(counterPrefix) && name[:len(counterPrefix)] == counterPrefix:
			if number, err := strconv.ParseInt(value, 10, 64); err == nil {
				snapshot.Counters[name[len(counterPrefix):]] = number
			}
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return ports.Snapshot{}, fmt.Errorf("read meta: %w", err)
	}

	rows, err = s.pool.Query(ctx,
		`SELECT satellite, acq_date, acq_time, latitude, longitude, raw_latitude, raw_longitude, confidence, frp, first_offset
		 FROM satellite_detections ORDER BY first_offset, detection_key`)
	if err != nil {
		return ports.Snapshot{}, fmt.Errorf("query detections: %w", err)
	}
	for rows.Next() {
		var (
			detection  domain.Detection
			satellite  string
			confidence string
		)
		if err := rows.Scan(&satellite, &detection.AcqDate, &detection.AcqTime,
			&detection.Latitude, &detection.Longitude, &detection.RawLatitude, &detection.RawLongitude,
			&confidence, &detection.FRP, &detection.FirstOffset); err != nil {
			rows.Close()
			return ports.Snapshot{}, fmt.Errorf("scan detection: %w", err)
		}
		detection.Satellite = domain.Satellite(satellite)
		detection.Confidence = domain.Confidence(confidence)
		snapshot.Detections = append(snapshot.Detections, detection)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return ports.Snapshot{}, fmt.Errorf("read detections: %w", err)
	}

	rows, err = s.pool.Query(ctx,
		`SELECT source_ref_id, occurred_at, area_name, latitude, longitude, detection_count,
		        satellites, max_confidence, total_frp, severity, detection_keys
		 FROM satellite_fires WHERE status = $1 ORDER BY source_ref_id`, domain.StatusActive)
	if err != nil {
		return ports.Snapshot{}, fmt.Errorf("query fires: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			fire          domain.Fire
			satellites    []string
			maxConfidence string
			severity      string
		)
		if err := rows.Scan(&fire.SourceRefID, &fire.OccurredAt, &fire.AreaName, &fire.Latitude,
			&fire.Longitude, &fire.DetectionCount, &satellites, &maxConfidence,
			&fire.TotalFRP, &severity, &fire.DetectionKeys); err != nil {
			return ports.Snapshot{}, fmt.Errorf("scan fire: %w", err)
		}
		fire.Status = domain.StatusActive
		fire.OccurredAt = fire.OccurredAt.UTC()
		for _, satellite := range satellites {
			fire.Satellites = append(fire.Satellites, domain.Satellite(satellite))
		}
		fire.MaxConfidence = domain.Confidence(maxConfidence)
		fire.Severity = domain.Severity(severity)
		snapshot.Fires = append(snapshot.Fires, fire)
	}
	if err := rows.Err(); err != nil {
		return ports.Snapshot{}, fmt.Errorf("read fires: %w", err)
	}
	return snapshot, nil
}

// Commit writes the batch and the offset in one transaction. Statements are
// pipelined, so a burst of several hundred detections costs one round trip.
func (s *State) Commit(ctx context.Context, change ports.Batch) error {
	batch := &pgx.Batch{}
	for _, detection := range change.UpsertDetections {
		observedAt, err := detection.Time()
		if err != nil {
			return fmt.Errorf("save detection %q: %w", detection.Key(), err)
		}
		batch.Queue(
			`INSERT INTO satellite_detections
			 (detection_key, satellite, acq_date, acq_time, latitude, longitude, raw_latitude, raw_longitude, confidence, frp, observed_at, first_offset)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			 ON CONFLICT (detection_key) DO UPDATE SET confidence = EXCLUDED.confidence, frp = EXCLUDED.frp`,
			detection.Key(), string(detection.Satellite), detection.AcqDate, detection.AcqTime,
			detection.Latitude, detection.Longitude, detection.RawLatitude, detection.RawLongitude,
			string(detection.Confidence), detection.FRP, observedAt, detection.FirstOffset)
	}
	if len(change.DeleteDetections) > 0 {
		batch.Queue(`DELETE FROM satellite_detections WHERE detection_key = ANY($1)`, change.DeleteDetections)
	}
	for _, fire := range change.Fires {
		satellites := make([]string, len(fire.Satellites))
		for i, satellite := range fire.Satellites {
			satellites[i] = string(satellite)
		}
		keys := fire.DetectionKeys
		if keys == nil {
			keys = []string{}
		}
		batch.Queue(
			`INSERT INTO satellite_fires
			 (source_ref_id, occurred_at, area_name, latitude, longitude, detection_count,
			  satellites, max_confidence, total_frp, severity, status, detection_keys, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, now())
			 ON CONFLICT (source_ref_id) DO UPDATE SET
			   occurred_at = EXCLUDED.occurred_at, area_name = EXCLUDED.area_name,
			   latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
			   detection_count = EXCLUDED.detection_count, satellites = EXCLUDED.satellites,
			   max_confidence = EXCLUDED.max_confidence, total_frp = EXCLUDED.total_frp,
			   severity = EXCLUDED.severity, status = EXCLUDED.status,
			   detection_keys = EXCLUDED.detection_keys, updated_at = now()`,
			fire.SourceRefID, fire.OccurredAt, fire.AreaName, fire.Latitude, fire.Longitude,
			fire.DetectionCount, satellites, string(fire.MaxConfidence), fire.TotalFRP,
			string(fire.Severity), fire.Status, keys)
	}
	for _, late := range change.Late {
		batch.Queue(
			`INSERT INTO satellite_late_detections (detection_key, feed_offset, acquired_at, watermark)
			 VALUES ($1,$2,$3,$4) ON CONFLICT (feed_offset, detection_key) DO NOTHING`,
			late.Key, late.Offset, late.AcquiredAt, late.Watermark)
	}
	meta := func(name, value string) {
		batch.Queue(`INSERT INTO satellite_meta (name, value) VALUES ($1,$2)
		             ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value`, name, value)
	}
	if !change.Watermark.IsZero() {
		meta(metaWatermark, change.Watermark.UTC().Format(time.RFC3339))
	}
	for name, value := range change.Counters {
		meta(counterPrefix+name, strconv.FormatInt(value, 10))
	}
	batch.Queue(
		`INSERT INTO satellite_feed_offset (consumer, offset_value) VALUES ($1, $2)
		 ON CONFLICT (consumer) DO UPDATE SET offset_value = EXCLUDED.offset_value`,
		consumerName, change.Offset)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin commit: %w", err)
	}
	defer tx.Rollback(ctx)
	results := tx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("commit statement %d: %w", i, err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("close commit batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Reset starts a new feed session: the working set, fires, late records,
// watermark and counters of the previous session are dropped and the offset
// returns to 0.
func (s *State) Reset(ctx context.Context, session string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin reset: %w", err)
	}
	defer tx.Rollback(ctx)
	for _, statement := range []string{
		`DELETE FROM satellite_detections WHERE true`,
		`DELETE FROM satellite_fire_detections WHERE true`,
		`DELETE FROM satellite_fires WHERE true`,
		`DELETE FROM satellite_late_detections WHERE true`,
		`DELETE FROM satellite_meta WHERE true`,
		`DELETE FROM satellite_feed_offset WHERE true`,
	} {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("reset state: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO satellite_meta (name, value) VALUES ($1,$2)`, metaSession, session); err != nil {
		return fmt.Errorf("save session: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit reset: %w", err)
	}
	return nil
}
