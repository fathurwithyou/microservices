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

CockroachDB runs with TLS. The `cockroachdb-certs` Job generates the CA, node, and root client certificates and merges them into `bnpb-secrets` as `ca.crt`, `node.crt`, `node.key`, `client.root.crt`, and `client.root.key`. It assumes `bnpb-secrets` already exists (it creates an empty one if not) and never replaces the other secret keys, so run the normal secret creation step first:

```sh
kubectl -n sistem-informasi-kebencanaan-bnpb apply -k deploy/kustomize/base
kubectl -n sistem-informasi-kebencanaan-bnpb wait --for=condition=complete job/cockroachdb-certs --timeout=120s
```

The ingestors need `FEED_TOKEN` and `SENSOR_TOKEN` in `bnpb-secrets`. Re-running the Job is a no-op once `node.key` exists; certificate rotation is manual (delete those keys, re-run the Job, restart the StatefulSet).
