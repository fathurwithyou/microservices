# Kustomize deployment

Secret values are intentionally not stored in this repository. The Makefile targets create or update the Kubernetes Secret from an environment-managed file, then apply the local overlay.

For local K3d:

```sh
cp .env.example .env
# Replace every replace-with-* value in .env.
make k3d-up
make k3d-deploy
```

`k3d-deploy` builds and imports all application images, including `portal-consumer`, `demo-api`, and `demo-console`. The local overlay exposes Client API through `localhost:8080` using NodePort `30080`, waits for PostgreSQL and all Deployments, and creates `bnpb-secrets` from `K3S_ENV_FILE` (default `.env`). Demo Console is in-cluster; port-forward `svc/demo-console` when you need the UI on K3d.

Only stateless workloads are intended to scale manually in M1:

```sh
K3D_SCALE_SERVICE=client-api K3D_REPLICAS=2 make k3d-scale
K3D_SCALE_SERVICE=auth K3D_REPLICAS=2 make k3d-scale
```

The allowed deployments are `client-api`, `auth`, `dashboard-consumer`, `notifier-consumer`, and `portal-consumer`. Keep BMKG, PVMBG, Aggregator polling, PostgreSQL, and RabbitMQ at one replica unless their state/ownership design is extended explicitly.

PostgreSQL is a single persistent StatefulSet reserved for the Auth Service. The `auth-db-bootstrap` Job creates the `auth` role and `auth_db`; CockroachDB is the Aggregator canonical store, and the Aggregator init containers run migrations and seed reference rows against it. No database or broker port is exposed outside the cluster.

RabbitMQ also uses a PVC so durable queues and persistent messages survive a pod restart. No database or broker port is exposed outside the cluster.

To render without applying:

```sh
kubectl kustomize overlays/local
```

## M2 cluster overlay

```sh
kubectl kustomize deploy/kustomize/overlays/cluster
```

The cluster overlay inherits the base namespace, tags application images `m2`, and scales Aggregator to 3, Client API to 2, and each consumer to 2. New M2 components are the CockroachDB StatefulSet, satellite/coastal ingestors, critical notifier, and a demo observability stack (Prometheus, Grafana, OTel Collector, Tempo). Observability and CockroachDB use no external managed services; CockroachDB persists 10Gi per replica on the `local-path` StorageClass.

CockroachDB runs with TLS, and storage ownership is enforced by the database:

| Database | SQL user | Client cert Secret | Mounted by |
|---|---|---|---|
| `aggregator_db` | `aggregator` | `cockroach-client-aggregator` | aggregator |
| `satellite_db` | `satellite` | `cockroach-client-satellite` | satellite-ingestor |
| `coastal_db` | `coastal` | `cockroach-client-coastal` | coastal-ingestor |

- `cockroachdb-certs` Job: creates the CA (kept in Secret `cockroach-ca`), the node cert, `client.root`, and one client cert per user. Node and root material is merged into `bnpb-secrets`; each user cert goes into its own Secret. Re-running never replaces existing material.
- `cockroachdb-bootstrap` Job: runs `cockroach init` once, then applies `base/cockroach/bootstrap.sql`: one database per owner, `GRANT ALL` to that owner only, `CONNECT` and public-schema `CREATE` revoked from `public`. Only this Job and the CockroachDB pods hold `client.root`.
- NetworkPolicy `cockroach-store-owners` admits SQL only from CockroachDB peers, the bootstrap Job, and the three owners.

```sh
kubectl -n sistem-informasi-kebencanaan-bnpb apply -k deploy/kustomize/overlays/cluster
kubectl -n sistem-informasi-kebencanaan-bnpb wait --for=condition=complete job/cockroachdb-certs --timeout=180s
kubectl -n sistem-informasi-kebencanaan-bnpb wait --for=condition=complete job/cockroachdb-bootstrap --timeout=300s
```

Store owners restart until the bootstrap Job has created their database and user; that is expected on a fresh cluster. The ingestors need `FEED_TOKEN` and `SENSOR_TOKEN` in `bnpb-secrets`. Certificate rotation is manual: delete `cockroach-ca`, the cert keys in `bnpb-secrets`, and the three `cockroach-client-*` Secrets, re-run the certs Job, then restart CockroachDB and the store owners.
