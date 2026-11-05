-- +goose Up
ALTER TABLE satellite_detections ADD COLUMN IF NOT EXISTS raw_latitude TEXT NOT NULL DEFAULT '';
ALTER TABLE satellite_detections ADD COLUMN IF NOT EXISTS raw_longitude TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE satellite_detections DROP COLUMN IF EXISTS raw_longitude;
ALTER TABLE satellite_detections DROP COLUMN IF EXISTS raw_latitude;
