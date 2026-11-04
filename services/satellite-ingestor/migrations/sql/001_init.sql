-- +goose Up
CREATE TABLE IF NOT EXISTS satellite_feed_offset (
  consumer TEXT PRIMARY KEY,
  offset_value BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS satellite_detections (
  detection_key TEXT PRIMARY KEY,
  satellite TEXT NOT NULL,
  acq_date TEXT NOT NULL,
  acq_time TEXT NOT NULL,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  confidence TEXT NOT NULL,
  frp DOUBLE PRECISION NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS satellite_detections_observed_idx ON satellite_detections (observed_at DESC);

CREATE TABLE IF NOT EXISTS satellite_fires (
  source_ref_id TEXT PRIMARY KEY,
  occurred_at TIMESTAMPTZ NOT NULL,
  detection_count INTEGER NOT NULL,
  satellites TEXT[] NOT NULL,
  max_confidence TEXT NOT NULL,
  total_frp DOUBLE PRECISION NOT NULL,
  severity TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS satellite_fires_occurred_idx ON satellite_fires (occurred_at DESC);

CREATE TABLE IF NOT EXISTS satellite_fire_detections (
  source_ref_id TEXT NOT NULL,
  detection_key TEXT NOT NULL,
  PRIMARY KEY (source_ref_id, detection_key)
);

-- +goose Down
DROP TABLE IF EXISTS satellite_fire_detections;
DROP TABLE IF EXISTS satellite_fires;
DROP TABLE IF EXISTS satellite_detections;
DROP TABLE IF EXISTS satellite_feed_offset;
