# Platform Koordinasi Kebencanaan

A microservices architecture for disaster coordination between BMKG, PVMBG, and BNPB agencies. Manages earthquake event data, authentication, and fan-out event delivery to multiple consumers.

## Overview

- **BMKG Service**: Mock REST endpoint for seismic event data
- **PVMBG Service**: Mock REST endpoint with runtime schema evolution and delay simulation
- **Aggregator Service**: Parallel poller, dynamic JSON mapping, canonical PostgreSQL store, gRPC API
- **Auth Service**: gRPC-based token issue, refresh, and validation with TTL rotation
- **Client API**: Downstream HTTP entrypoint for authorized consumers
- **Consumer Services**: Dashboard, Notifier, and Portal — independent fan-out listeners with idempotent processing
- **Demo Console** (Compose profile `demo`): React UI plus `demo-api` for live P1–P5 proof. `demo-api` does not read the Canonical Store or the Docker socket.

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

5. Start the stack including the Demo Console (and portal consumer for P5.4):
   ```sh
   make demo-up
   ```
   Open http://localhost:3000. Login uses `MEDIA_CLIENT_SECRET`, `FIELD_TEAM_CLIENT_SECRET`, and `BNPB_OPS_CLIENT_SECRET` from `.env`. Secrets are not stored after submit.

**Published ports:**
- Demo Console: 3000 (profile `demo`)
- Client API: 8080
- BMKG REST: 8081
- PVMBG REST: 8082
- Aggregator health: 8090
- Aggregator gRPC: 9090
- Auth gRPC: 9091
- Demo API: 8086 (profile `demo`)

All configuration is environment-driven. No credentials are stored in source code, Dockerfiles, or manifests. See `.env.example`.

## Demo walkthrough (P1–P5)

Traceability of every criterion to UI, API, and artifact: [docs/DEMO_TRACEABILITY.md](docs/DEMO_TRACEABILITY.md). Report template: [docs/LAPORAN_TEMPLATE.md](docs/LAPORAN_TEMPLATE.md).

Fallback if a step fails: download Evidence JSON, match `correlation_id` in `docker compose logs`, and re-run the matching `make test-*` target.

### P1 Schema evolution

1. Login as Tim Lapangan or BNPB Ops.
2. Scenario Lab → P1 → Run P1.
3. Expected: baseline HazardEvent valid; after schema toggle, new records show `attributes.confidence_level`; old records remain readable. No restart.
4. Artifact after `make test-e2e`: `artifacts/p1-schema-evolution.json`.

### P2 Outage, isolation, load

1. Lab P2 → Run P2: PVMBG outage → `/sources/status` becomes `stale` with `stale_since` → BMKG and last volcanic data still HTTP 200 → outage off → `healthy` without restart.
2. Load (not measured in the browser):
   ```sh
   # isolation: set PVMBG_MAX_DELAY=3s in .env, recreate pvmbg, then
   make test-load-isolation
   make test-load
   ```
3. Open Test Evidence and inspect `artifacts/p2-isolation.json` (BMKG-only p95 < 300ms) and `artifacts/p2-sustained.json` (50 VU / 60s, error rate < 1%, 429 counted separately).

### P3 Trust domains

1. Login all three clients (three identities, not a role flag).
2. Lab P3 → Run P3.
3. Expected: cross-domain 401/403; Media `raw=true` → 403 from the server; summary omits raw fields; access token expires naturally; refresh without re-login; old refresh rejected.
4. Artifact: `artifacts/p3-auth.json`.

### P4 Independent deploy and JSONB store

1. Keep Overview open (health probes).
2. Terminal (do not give the UI a Docker socket):
   ```sh
   docker compose --env-file .env up -d --no-deps --build pvmbg
   ```
3. Expected: aggregator and client-api stay up; BMKG queries still succeed. P1 before/after view proves JSONB coexistence.
4. Artifacts: `artifacts/p4-independent-rebuild.json`, `artifacts/p4-jsonb-coexistence.json`.

### P5 Fan-out

1. Overview / Lab P5 shows queue depth via RabbitMQ management (through demo-api only).
2. Terminal:
   ```sh
   docker compose --env-file .env stop notifier-consumer
   # dashboard stays healthy, notifier queue grows
   docker compose --env-file .env start notifier-consumer
   # notifier queue drains (at-least-once; consumers are idempotent)
   docker compose --env-file .env --profile portal up -d portal-consumer
   ```
3. Outbox-while-broker-down is proven by E2E SQL, not by UI access to Postgres: `artifacts/p5-outbox-recovery.json`.

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
- `make frontend-test` — Demo Console Vitest suite
- `make test-e2e` — End-to-end test suite with isolated Compose stack (writes `artifacts/*.json`)
- `make test-load` — k6 load test (50 VU, 60 seconds)
- `make test-load-isolation` — concurrent BMKG-only vs PVMBG (requires `PVMBG_MAX_DELAY=3s`)
- `make test-demo-e2e` — Playwright smoke against `DEMO_CONSOLE_URL` (default http://127.0.0.1:3000)
- `make demo-up` — Compose with profiles `demo` and `portal`

Frontend local dev (stack already up on 8080/8086):

```sh
cd apps/demo-console
npm install
npm run dev
```

For K3s deployment: `make k3d-up`, create secrets, `make k3d-deploy`.

## Architecture

- Protobuf-defined contracts (gRPC for internal, REST for BMKG/PVMBG)
- RabbitMQ for fan-out event distribution with durable queues
- PostgreSQL with JSONB attributes for flexible schema evolution
- Per-service Dockerfiles for build isolation
- Kustomize overlays for K3s/K8s manifests
- Same-origin Demo Console proxy: `/api/*` → Client API, `/demo/*` → demo-api
