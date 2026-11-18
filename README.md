# Platform Koordinasi Kebencanaan

A microservices architecture for disaster coordination between BMKG, PVMBG, and BNPB agencies. Manages earthquake event data, authentication, and fan-out event delivery to multiple consumers.

## Overview

- **BMKG Service**: Mock REST endpoint for seismic event data
- **PVMBG Service**: Mock REST endpoint with runtime schema evolution and delay simulation
- **Aggregator Service**: Parallel poller, dynamic JSON mapping, canonical CockroachDB store, gRPC API
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
3. Outbox-while-broker-down is proven by E2E SQL against the canonical store, not by UI access: `artifacts/p5-outbox-recovery.json`.

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

For local K3d deployment: `make k3d-up` then `make k3d-deploy`.

## Architecture

- Protobuf-defined contracts (gRPC for internal, REST for BMKG/PVMBG)
- RabbitMQ for fan-out event distribution with durable queues
- CockroachDB with JSONB attributes for flexible schema evolution and RF=3 replication
- Per-service Dockerfiles for build isolation
- Kustomize manifests for local K3d deployment
- Same-origin Demo Console proxy: `/api/*` → Client API, `/demo/*` → demo-api

## M2 — scalable distributed system

M2 adds CockroachDB storage, leader-elected ingestion, Audit API, field reports,
satellite ingestion, coastal sensor ingestion, and a critical notification path.
See [docs/M2.md](docs/M2.md) for architecture, configuration, and the cluster
deployment procedure.

```bash
make m2-verify     # format, vet, unit tests, rendered manifests
make m2-smoke      # live health + Audit API checks
make m2-evidence   # write artifacts/m2/*
make m2-load       # 10k VUs with 3->5->2 scaling
```

M2 local services are behind the Compose profile `m2` and require `FEED_TOKEN`.

### M2 evidence runs

`scripts/m2-demo.sh` runs every P1–P5 scenario against the cluster and writes
one folder per run, `artifacts/m2/runs/m2-<UTC time>/`, with `run.json`
(commit, images, nodes, stage exit codes), `timeline.txt`, one subfolder per
problem, and `SUMMARY.md` / `summary.json` (PASS / FAIL / NOT RUN per
criterion with the evidence file behind each number).

```bash
./scripts/m2-demo.sh all                                  # every non-destructive scenario
./scripts/m2-demo.sh prep dataset <catalog-file> p1 summary   # assistants' catalog (CSV/JSON/NDJSON/GeoJSON, .gz)
CONFIRM=yes ./scripts/m2-demo.sh p1-node-loss             # power off a worker, wipe its store disk, rejoin
CONFIRM=yes ./scripts/m2-demo.sh p2-skew p2-freeze        # leader clock skew, leader VM freeze
CONFIRM=yes NEW_IMAGE=<aggregator image> ./scripts/m2-demo.sh p2-load
CONFIRM=yes ./scripts/m2-demo.sh p2-throughput            # 1 vs 3 Aggregator instances
CONFIRM=yes ./scripts/m2-demo.sh p4-sim                   # P4 rehearsal against the in-cluster stand-in feed
CONFIRM=yes ./scripts/m2-demo.sh p4-feed-real             # point the ingestor back at the assistants' feed
CONFIRM=yes ./scripts/m2-demo.sh p5-critical              # critical-notification latency rounds (restarts the sensor)
CONFIRM=yes ./scripts/m2-demo.sh p5-gameday               # Tester Agent practice scenarios, hands-off
./scripts/m2-demo.sh summary
./scripts/m2-demo.sh restore                              # undo test-only changes
```

`python3 scripts/m2-catalog-normalize.py <file> --inspect` shows how a catalog
of unknown shape maps onto the import fields before anything is written.

### M2 topology declaration

Four nodes, all virtual machines on one physical host (so the "nodes on
different physical hosts" bonus is not claimed). Each VM runs its own kernel.

| Node | Role | Type | Physical host | CPU | RAM | Disk |
|---|---|---|---|---|---|---|
| node-a | control plane + worker | VM (Multipass, QEMU/HVF), Ubuntu 24.04 | Mac mini M4 Pro | 2 vCPU | 4.8 GiB | 20 GiB |
| node-b | worker | VM (Multipass, QEMU/HVF), Ubuntu 24.04 | Mac mini M4 Pro | 2 vCPU | 4.8 GiB | 20 GiB |
| node-c | worker | VM (Multipass, QEMU/HVF), Ubuntu 24.04 | Mac mini M4 Pro | 2 vCPU | 3.8 GiB | 20 GiB |
| node-d | worker | VM (Multipass, QEMU/HVF), Ubuntu 24.04 | Mac mini M4 Pro | 2 vCPU | 3.8 GiB | 20 GiB |

Physical host: Mac mini (Mac16,11), Apple M4 Pro, 12 cores, 24 GB RAM,
macOS 15.6.1. Kubernetes: k3s v1.37.2+k3s1. Node-kill scenarios target a
non-control-plane worker only.

Cluster requirements used by the manifests: namespace
`sistem-informasi-kebencanaan-bnpb`, a `local-path` StorageClass, and nodes
labelled by Kubernetes. Tester Agent is installed outside the application
namespace:

```sh
kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
kubectl -n tester-agent exec daemonset/tester-agent -- /app list
```

The agent runs the host's `iptables` through `/host`. On Ubuntu the binary is
an absolute symlink through `/etc/alternatives`, which does not resolve under
that prefix, so the agent's `block_internode` step fails with "biner iptables
tidak ditemukan". Make the link relative once per node:

```sh
sudo sh -c 'cd /usr/sbin && for b in iptables ip6tables; do ln -sfn "$(basename "$(readlink -f $b)")" $b; done'
```

### M2 scenario triggers

| Scenario | Command |
|---|---|
| Deploy the cluster overlay | `make m2-deploy` |
| Health and Audit API | `make m2-smoke` |
| Evidence files | `make m2-evidence` |
| 10k VU, 3 to 5 to 2, rolling update | `make m2-load` |
| PVMBG schema by version (form the Tester Agent sends) | `curl -X POST -d '{"version":3}' http://pvmbg:8082/admin/schema-version` (1 original, 2 additive, 3 incompatible) |
| PVMBG incompatible schema | `curl -X POST -H "X-PVMBG-Admin-Key: $PVMBG_ADMIN_KEY" -d '{"mode":"breaking"}' http://pvmbg:8082/admin/schema-version` |
| PVMBG outage | `curl -X POST -H "X-PVMBG-Admin-Key: $PVMBG_ADMIN_KEY" -d '{"enabled":true}' http://pvmbg:8082/admin/outage` |
| Satellite feed rehearsal (burst, cut, leader kill, reference) | `CONFIRM=yes ./scripts/m2-demo.sh p4-sim` |
| Result vs reference on the assistants' feed | `python3 scripts/m2-p4-reference.py --stream-url https://satellite.naufarrel.dev --token $FEED_TOKEN --until-offset N --audit-url http://localhost:18090 --audit-token $AUDIT_TOKEN --lateness-minutes 720` |
| Remove rehearsal events of one source | `curl -X POST -H "Authorization: Bearer $OPS_TOKEN" "http://aggregator:8090/internal/ops/purge?source=SATELIT"` |
| Tester Agent practice scenario | `kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice <name>` |
| Replay parked PVMBG payloads | `curl -X POST -H "Authorization: Bearer $OPS_TOKEN" "http://aggregator:8090/internal/ops/replay?source=PVMBG"` |
| Import historical earthquake catalog | `go run ./services/aggregator/cmd/server import-catalog -file catalog.ndjson` |
| Coastal sensor (Compose) | `docker compose --env-file .env --profile m2 up -d sensor-pantai coastal-ingestor` |

The sensor image is pinned by digest in `deploy/kustomize/base/sensor-pantai.yaml`
and in the Compose `m2` profile. Both read the same `SENSOR_TOKEN`, which is
also the image's `PANTAI_AUTH_TOKEN`.

