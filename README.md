# Platform Koordinasi Kebencanaan

A microservices architecture for disaster coordination between BMKG, PVMBG, and BNPB agencies. Manages earthquake event data, authentication, and fan-out event delivery to multiple consumers.

## Overview

- **BMKG Service**: Mock REST endpoint for seismic event data
- **PVMBG Service**: Mock REST endpoint with runtime schema evolution and delay simulation
- **Aggregator Service**: Parallel poller, dynamic JSON mapping, canonical PostgreSQL store, gRPC API
- **Auth Service**: gRPC-based token issue, refresh, and validation with TTL rotation
- **Client API**: Downstream HTTP entrypoint for authorized consumers
- **Consumer Services**: Dashboard, Notifier, and Portal — independent fan-out listeners with idempotent processing

## Setup

1. Copy the example environment file:
   ```sh
   cp .env.example .env
   ```

2. Generate Go code from Protobuf definitions:
   ```sh
   make generate
   ```

3. Run tests and validation:
   ```sh
   make lint && make test
   ```

4. Start the full stack:
   ```sh
   docker compose --env-file .env up --build
   ```

**Published ports:**
- Client API: 8080
- BMKG REST: 8081
- PVMBG REST: 8082
- Aggregator health: 8090
- Aggregator gRPC: 9090
- Auth gRPC: 9091

All configuration is environment-driven. No credentials are stored in source code, Dockerfiles, or manifests.

## Demo Scenarios

**Additive Schema Evolution**: Activate new fields via admin API without service restart. Decoder forwards unknown fields to `attributes` JSONB map.

**Concurrency & Graceful Degradation**: Configure max delay and inflight limits. Outage simulation marks sources as `stale` while preserving stored events.

**Trust Domains**: BMKG uses header-based keys; PVMBG uses Bearer tokens. Cross-domain credentials are rejected.

**Fan-out Delivery**: Aggregator publishes to durable exchange; messages routed to dashboard, notifier, and portal queues. Manual acknowledgment with per-consumer deduplication.

**Service Restart**: Each service has its own Dockerfile. Independent restarts do not affect other components.

## Database

Migrations are versioned goose files in `services/aggregator/migrations/sql/`. They run automatically on Compose startup (one-shot `migrate` service) and on Kubernetes (aggregator init container), so setup needs no manual step. Explicit commands:

- `make migrate-up` — Apply pending migrations
- `make migrate-status` — Show applied and pending migrations
- `make migrate-down` — Roll back the latest migration (development only)
- `make seed` — Apply migrations, then insert minimal reference rows (idempotent)

Against a directly reachable database, the goose CLI (installed by `make tools` into `.bin/`) works too:

```sh
./.bin/goose -dir services/aggregator/migrations/sql postgres "$AGGREGATOR_DATABASE_URL" up
```

Mock seed size for BMKG/PVMBG is configurable via `SEED_COUNT` (default 20, `0` starts empty).

## Development

- `make generate` — Generate Go from Protobuf
- `make test` — Run unit tests (no services required)
- `make lint` — Code formatting and static analysis
- `make test-e2e` — End-to-end test suite with isolated Compose stack
- `make test-load` — k6 load test (50 VU, 60 seconds)

For K3s deployment: `make k3d-up`, create secrets, `make k3d-deploy`.

## Architecture

- Protobuf-defined contracts (gRPC for internal, REST for BMKG/PVMBG)
- RabbitMQ for fan-out event distribution with durable queues
- PostgreSQL with JSONB attributes for flexible schema evolution
- Per-service Dockerfiles for build isolation
- Kustomize overlays for K3s/K8s manifests