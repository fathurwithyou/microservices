-- +goose Up
CREATE TABLE IF NOT EXISTS coastal_cursor (
    source     TEXT PRIMARY KEY,
    cursor     TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS coastal_cursor;
