-- Storage ownership for CockroachDB (M2 Ketentuan Implementasi 8).
-- Single source for Kubernetes (configMapGenerator cockroachdb-bootstrap)
-- and Docker Compose (cockroach-init). Every statement is idempotent.
CREATE DATABASE IF NOT EXISTS aggregator_db;
CREATE DATABASE IF NOT EXISTS satellite_db;
CREATE DATABASE IF NOT EXISTS coastal_db;

CREATE USER IF NOT EXISTS aggregator;
CREATE USER IF NOT EXISTS satellite;
CREATE USER IF NOT EXISTS coastal;

ALTER DATABASE aggregator_db OWNER TO aggregator;
ALTER DATABASE satellite_db OWNER TO satellite;
ALTER DATABASE coastal_db OWNER TO coastal;

GRANT ALL ON DATABASE aggregator_db TO aggregator;
GRANT ALL ON DATABASE satellite_db TO satellite;
GRANT ALL ON DATABASE coastal_db TO coastal;

GRANT ALL ON SCHEMA aggregator_db.public TO aggregator;
GRANT ALL ON SCHEMA satellite_db.public TO satellite;
GRANT ALL ON SCHEMA coastal_db.public TO coastal;

REVOKE CONNECT ON DATABASE aggregator_db FROM public;
REVOKE CONNECT ON DATABASE satellite_db FROM public;
REVOKE CONNECT ON DATABASE coastal_db FROM public;
REVOKE CREATE ON SCHEMA aggregator_db.public FROM public;
REVOKE CREATE ON SCHEMA satellite_db.public FROM public;
REVOKE CREATE ON SCHEMA coastal_db.public FROM public;

-- The Audit API reads range placement and node liveness
-- (SHOW RANGE ... FOR ROW, crdb_internal.gossip_nodes). This is cluster
-- metadata, not data of another database.
GRANT SYSTEM VIEWCLUSTERMETADATA TO aggregator;

-- server.clock.forward_jump_check.enabled stays off. It was tried on 18 Nov so
-- that a node whose clock jumps forward stops instead of pushing the cluster's
-- hybrid clock ahead. On this testbed (four VMs on one busy host) a vCPU stall
-- of a few hundred milliseconds already counts as a forward jump: three of the
-- four nodes panicked at the same instant and crash-looped. A forward clock
-- jump on a store node is therefore a declared limit, see docs/M2.md.
SET CLUSTER SETTING server.clock.forward_jump_check.enabled = false;
