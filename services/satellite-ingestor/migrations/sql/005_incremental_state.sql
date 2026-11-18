-- +goose Up
-- The working set is now written incrementally in one transaction with the
-- feed offset, instead of being rewritten per batch.
ALTER TABLE satellite_detections ADD COLUMN IF NOT EXISTS first_offset BIGINT NOT NULL DEFAULT 0;
ALTER TABLE satellite_fires ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active';
ALTER TABLE satellite_fires ADD COLUMN IF NOT EXISTS detection_keys TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[];
CREATE INDEX IF NOT EXISTS satellite_fires_status_idx ON satellite_fires (status);

-- Session, watermark and cumulative counts that must survive a restart.
CREATE TABLE IF NOT EXISTS satellite_meta (
  name TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- Detections that arrived past the accepted lateness: kept, not clustered.
CREATE TABLE IF NOT EXISTS satellite_late_detections (
  detection_key TEXT NOT NULL,
  feed_offset BIGINT NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL,
  watermark TIMESTAMPTZ NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (feed_offset, detection_key)
);

-- +goose Down
DROP TABLE IF EXISTS satellite_late_detections;
DROP TABLE IF EXISTS satellite_meta;
DROP INDEX IF EXISTS satellite_fires_status_idx;
ALTER TABLE satellite_fires DROP COLUMN IF EXISTS detection_keys;
ALTER TABLE satellite_fires DROP COLUMN IF EXISTS status;
ALTER TABLE satellite_detections DROP COLUMN IF EXISTS first_offset;
