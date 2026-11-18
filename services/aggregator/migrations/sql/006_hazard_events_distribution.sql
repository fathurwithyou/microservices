-- +goose NO TRANSACTION
-- +goose Up
-- M2 Problem 1: spread hazard_events over the store nodes.
--
-- The primary key hazard_id is a SHA-1 UUID, so its keyspace is already
-- uniform no matter how skewed the catalog is geographically. A small table
-- still lives in one range (default max 512 MiB) with one leaseholder, so
-- every write would land on one node. Sticky manual splits at the 16 leading
-- hex digits give the replicate queue 16 primary-key ranges to place and the
-- lease balancer 16 leases to spread.
ALTER TABLE hazard_events SPLIT AT VALUES
  ('1'), ('2'), ('3'), ('4'), ('5'), ('6'), ('7'),
  ('8'), ('9'), ('a'), ('b'), ('c'), ('d'), ('e'), ('f');

-- The listing indexes start with a low-cardinality column followed by time,
-- so every new event of one source/type/severity is appended to the tail of
-- one range: a write hotspot during a regional earthquake burst. Hash-sharded
-- replacements prefix a bucket column, so consecutive writes go to different
-- ranges, and ordered scans still merge the buckets. hazard_id is the
-- listing tiebreaker, so it is part of every listing index.
CREATE INDEX IF NOT EXISTS hazard_events_source_occurred_hidx
  ON hazard_events (source, occurred_at DESC, hazard_id DESC) USING HASH WITH (bucket_count = 8);
CREATE INDEX IF NOT EXISTS hazard_events_type_occurred_hidx
  ON hazard_events (hazard_type, occurred_at DESC, hazard_id DESC) USING HASH WITH (bucket_count = 8);
CREATE INDEX IF NOT EXISTS hazard_events_severity_occurred_hidx
  ON hazard_events (severity, occurred_at DESC, hazard_id DESC) USING HASH WITH (bucket_count = 8);
DROP INDEX IF EXISTS hazard_events_source_occurred_idx;
DROP INDEX IF EXISTS hazard_events_type_occurred_idx;
DROP INDEX IF EXISTS hazard_events_severity_occurred_idx;

-- Cross-source listing ("latest N across every hazard type") orders by
-- occurred_at desc with hazard_id as tiebreaker; keep it a sharded index scan.
CREATE INDEX IF NOT EXISTS hazard_events_occurred_hidx
  ON hazard_events (occurred_at DESC, hazard_id DESC) USING HASH WITH (bucket_count = 8);

-- One range per source for the (source, source_ref_id) uniqueness index.
ALTER INDEX hazard_events@hazard_events_source_source_ref_id_key SPLIT AT VALUES
  ('BMKG'), ('PVMBG'), ('SATELIT'), ('SENSOR_PANTAI');

-- +goose Down
DROP INDEX IF EXISTS hazard_events_occurred_hidx;
CREATE INDEX IF NOT EXISTS hazard_events_source_occurred_idx ON hazard_events (source, occurred_at DESC);
CREATE INDEX IF NOT EXISTS hazard_events_type_occurred_idx ON hazard_events (hazard_type, occurred_at DESC);
CREATE INDEX IF NOT EXISTS hazard_events_severity_occurred_idx ON hazard_events (severity, occurred_at DESC);
DROP INDEX IF EXISTS hazard_events_source_occurred_hidx;
DROP INDEX IF EXISTS hazard_events_type_occurred_hidx;
DROP INDEX IF EXISTS hazard_events_severity_occurred_hidx;
ALTER TABLE hazard_events UNSPLIT ALL;
ALTER INDEX hazard_events@hazard_events_source_source_ref_id_key UNSPLIT ALL;
