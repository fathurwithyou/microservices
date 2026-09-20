-- +goose Up
CREATE TABLE IF NOT EXISTS hazard_events (
  hazard_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  source_ref_id TEXT NOT NULL,
  hazard_type TEXT NOT NULL,
  severity TEXT NOT NULL,
  area_name TEXT NOT NULL,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL,
  attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (source, source_ref_id)
);
CREATE INDEX IF NOT EXISTS hazard_events_source_occurred_idx ON hazard_events (source, occurred_at DESC);
CREATE INDEX IF NOT EXISTS hazard_events_type_occurred_idx ON hazard_events (hazard_type, occurred_at DESC);
CREATE INDEX IF NOT EXISTS hazard_events_severity_occurred_idx ON hazard_events (severity, occurred_at DESC);

CREATE TABLE IF NOT EXISTS source_status (
  source TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  last_success_at TIMESTAMPTZ,
  stale_since TIMESTAMPTZ,
  last_error TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS poll_cursors (
  source TEXT PRIMARY KEY,
  cursor_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS volcano_reference (
  volcano_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL
);

INSERT INTO volcano_reference (volcano_id, name, latitude, longitude) VALUES
  ('merapi', 'Gunung Merapi', -7.54, 110.44),
  ('semeru', 'Gunung Semeru', -8.11, 112.92),
  ('anak_krakatau', 'Anak Krakatau', -6.10, 105.42)
ON CONFLICT (volcano_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS outbox_messages (
  message_id BIGSERIAL PRIMARY KEY,
  hazard_id TEXT NOT NULL,
  correlation_id TEXT NOT NULL,
  routing_key TEXT NOT NULL,
  payload BYTEA NOT NULL,
  published_at TIMESTAMPTZ,
  attempts INTEGER NOT NULL DEFAULT 0,
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_until TIMESTAMPTZ,
  lease_token TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS outbox_messages_pending_idx ON outbox_messages (available_at, message_id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS outbox_messages;
DROP TABLE IF EXISTS volcano_reference;
DROP TABLE IF EXISTS poll_cursors;
DROP TABLE IF EXISTS source_status;
DROP TABLE IF EXISTS hazard_events;
