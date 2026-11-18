-- +goose Up
-- Owned by coastal-ingestor only; one row per named lease.
CREATE TABLE IF NOT EXISTS coastal_leader_lease (
  name TEXT PRIMARY KEY,
  holder TEXT NOT NULL,
  fence BIGINT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);

-- +goose Down
DROP TABLE IF EXISTS coastal_leader_lease;
