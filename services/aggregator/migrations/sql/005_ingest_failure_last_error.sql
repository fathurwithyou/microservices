-- +goose Up
-- Keep the original failure reason intact; automatic retries record their
-- latest outcome here instead of overwriting it.
ALTER TABLE ingest_failures ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE ingest_failures DROP COLUMN IF EXISTS last_error;
