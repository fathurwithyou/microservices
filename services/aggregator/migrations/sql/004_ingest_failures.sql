-- +goose Up
CREATE TABLE IF NOT EXISTS ingest_failures (
  message_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  received_at TIMESTAMPTZ NOT NULL,
  reason TEXT NOT NULL,
  retryable BOOLEAN NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  payload BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS ingest_failures_status_idx ON ingest_failures (status, received_at);

-- +goose Down
DROP TABLE IF EXISTS ingest_failures;
