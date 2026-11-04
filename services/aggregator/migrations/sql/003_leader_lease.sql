-- +goose Up
CREATE TABLE IF NOT EXISTS leader_lease (
  name TEXT PRIMARY KEY,
  holder TEXT NOT NULL,
  fence BIGINT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);

-- +goose Down
DROP TABLE IF EXISTS leader_lease;
