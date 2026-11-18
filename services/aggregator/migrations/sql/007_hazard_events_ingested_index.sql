-- +goose Up
-- The Audit API lists events in ingestion order with a keyset cursor
-- (ingested_at, hazard_id). Without an index on that order every page was a
-- full scan plus a top-1000 sort over the whole table; on 540k events the
-- sorts of a few concurrent pages exhausted the node's SQL memory budget
-- (SQLSTATE 53200, 18 Nov). Hash-sharded like the other time-ordered indexes,
-- so a burst of ingestion does not write to one range only.
CREATE INDEX IF NOT EXISTS hazard_events_ingested_hidx
  ON hazard_events (ingested_at ASC, hazard_id ASC) USING HASH WITH (bucket_count = 8);

-- +goose Down
DROP INDEX IF EXISTS hazard_events_ingested_hidx;
