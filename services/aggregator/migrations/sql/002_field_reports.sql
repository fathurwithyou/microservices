-- +goose Up
CREATE TABLE IF NOT EXISTS field_reports (
  report_id TEXT PRIMARY KEY,
  hazard_type TEXT NOT NULL,
  severity TEXT NOT NULL,
  area_name TEXT NOT NULL,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  description TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'accepted',
  attempts INTEGER NOT NULL DEFAULT 0,
  processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS field_reports_status_idx ON field_reports (status, received_at);

-- +goose Down
DROP TABLE IF EXISTS field_reports;
