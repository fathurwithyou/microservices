-- +goose Up
CREATE TABLE IF NOT EXISTS coastal_quarantine (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id  TEXT NOT NULL,
    source      TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    reason      TEXT NOT NULL,
    retryable   BOOLEAN NOT NULL,
    payload     BYTEA,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS coastal_quarantine_received_at_idx
    ON coastal_quarantine (received_at DESC);

CREATE INDEX IF NOT EXISTS coastal_quarantine_message_id_idx
    ON coastal_quarantine (message_id);

-- One row per source keeps the last accepted batch readable after a restart.
CREATE TABLE IF NOT EXISTS coastal_last_good (
    source      TEXT PRIMARY KEY,
    captured_at TIMESTAMPTZ NOT NULL,
    readings    JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS coastal_last_good;
DROP TABLE IF EXISTS coastal_quarantine;
