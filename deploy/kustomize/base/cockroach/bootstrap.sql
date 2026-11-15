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
