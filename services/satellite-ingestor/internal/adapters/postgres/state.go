// Package postgres provides the durable pgx State adapter.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
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

func (s *State) Offset(ctx context.Context) (int64, error) {
	var offset int64
	err := s.pool.QueryRow(ctx,
		`SELECT offset_value FROM satellite_feed_offset WHERE consumer = $1`, consumerName).Scan(&offset)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("load feed offset: %w", err)
	}
	return offset, nil
}

func (s *State) SaveOffset(ctx context.Context, offset int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO satellite_feed_offset (consumer, offset_value) VALUES ($1, $2)
		 ON CONFLICT (consumer) DO UPDATE SET offset_value = EXCLUDED.offset_value`,
		consumerName, offset)
	if err != nil {
		return fmt.Errorf("save feed offset: %w", err)
	}
	return nil
}

func (s *State) Detections(ctx context.Context) ([]domain.Detection, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT satellite, acq_date, acq_time, latitude, longitude, raw_latitude, raw_longitude, confidence, frp
		 FROM satellite_detections ORDER BY detection_key`)
	if err != nil {
		return nil, fmt.Errorf("query detections: %w", err)
	}
	defer rows.Close()

	var detections []domain.Detection
	for rows.Next() {
		var (
			detection  domain.Detection
			satellite  string
			confidence string
		)
		if err := rows.Scan(&satellite, &detection.AcqDate, &detection.AcqTime,
			&detection.Latitude, &detection.Longitude, &detection.RawLatitude, &detection.RawLongitude,
			&confidence, &detection.FRP); err != nil {
			return nil, fmt.Errorf("scan detection: %w", err)
		}
		detection.Satellite = domain.Satellite(satellite)
		detection.Confidence = domain.Confidence(confidence)
		detections = append(detections, detection)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read detections: %w", err)
	}
	return detections, nil
}

func (s *State) SaveDetections(ctx context.Context, detections []domain.Detection) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin detections transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM satellite_detections`); err != nil {
		return fmt.Errorf("clear detections: %w", err)
	}
	for _, detection := range detections {
		observedAt, err := detection.Time()
		if err != nil {
			return fmt.Errorf("save detection %q: %w", detection.Key(), err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO satellite_detections
			 (detection_key, satellite, acq_date, acq_time, latitude, longitude, raw_latitude, raw_longitude, confidence, frp, observed_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			detection.Key(), string(detection.Satellite), detection.AcqDate, detection.AcqTime,
			detection.Latitude, detection.Longitude, detection.RawLatitude, detection.RawLongitude,
			string(detection.Confidence), detection.FRP, observedAt); err != nil {
			return fmt.Errorf("insert detection %q: %w", detection.Key(), err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit detections: %w", err)
	}
	return nil
}

func (s *State) Fires(ctx context.Context) ([]domain.Fire, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT source_ref_id, occurred_at, area_name, latitude, longitude,
		        detection_count, satellites, max_confidence, total_frp, severity
		 FROM satellite_fires ORDER BY source_ref_id`)
	if err != nil {
		return nil, fmt.Errorf("query fires: %w", err)
	}
	defer rows.Close()

	var fires []domain.Fire
	for rows.Next() {
		var (
			fire          domain.Fire
			satellites    []string
			maxConfidence string
			severity      string
		)
		if err := rows.Scan(&fire.SourceRefID, &fire.OccurredAt, &fire.AreaName, &fire.Latitude,
			&fire.Longitude, &fire.DetectionCount, &satellites, &maxConfidence,
			&fire.TotalFRP, &severity); err != nil {
			return nil, fmt.Errorf("scan fire: %w", err)
		}
		fire.Satellites = make([]domain.Satellite, len(satellites))
		for i, satellite := range satellites {
			fire.Satellites[i] = domain.Satellite(satellite)
		}
		fire.MaxConfidence = domain.Confidence(maxConfidence)
		fire.Severity = domain.Severity(severity)
		fires = append(fires, fire)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read fires: %w", err)
	}
	if len(fires) == 0 {
		return nil, nil
	}

	rows, err = s.pool.Query(ctx,
		`SELECT source_ref_id, detection_key FROM satellite_fire_detections
		 ORDER BY source_ref_id, detection_key`)
	if err != nil {
		return nil, fmt.Errorf("query fire detections: %w", err)
	}
	defer rows.Close()

	keys := make(map[string][]string, len(fires))
	for rows.Next() {
		var sourceRefID, detectionKey string
		if err := rows.Scan(&sourceRefID, &detectionKey); err != nil {
			return nil, fmt.Errorf("scan fire detection: %w", err)
		}
		keys[sourceRefID] = append(keys[sourceRefID], detectionKey)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read fire detections: %w", err)
	}
	for i := range fires {
		fires[i].DetectionKeys = keys[fires[i].SourceRefID]
	}
	return fires, nil
}

func (s *State) SaveFires(ctx context.Context, fires []domain.Fire) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin fires transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM satellite_fire_detections`); err != nil {
		return fmt.Errorf("clear fire detections: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM satellite_fires`); err != nil {
		return fmt.Errorf("clear fires: %w", err)
	}
	for _, fire := range fires {
		satellites := make([]string, len(fire.Satellites))
		for i, satellite := range fire.Satellites {
			satellites[i] = string(satellite)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO satellite_fires
			 (source_ref_id, occurred_at, area_name, latitude, longitude, detection_count,
			  satellites, max_confidence, total_frp, severity)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			fire.SourceRefID, fire.OccurredAt, fire.AreaName, fire.Latitude, fire.Longitude,
			fire.DetectionCount, satellites, string(fire.MaxConfidence), fire.TotalFRP,
			string(fire.Severity)); err != nil {
			return fmt.Errorf("insert fire %q: %w", fire.SourceRefID, err)
		}
		for _, detectionKey := range fire.DetectionKeys {
			if _, err := tx.Exec(ctx,
				`INSERT INTO satellite_fire_detections (source_ref_id, detection_key) VALUES ($1,$2)`,
				fire.SourceRefID, detectionKey); err != nil {
				return fmt.Errorf("insert fire %q detection %q: %w", fire.SourceRefID, detectionKey, err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit fires: %w", err)
	}
	return nil
}
