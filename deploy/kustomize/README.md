# Kustomize deployment

Secret values are intentionally not stored in this repository. The Makefile targets create or update the Kubernetes Secret from an environment-managed file, then apply the selected overlay.

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

PostgreSQL is a single persistent StatefulSet. Its PVC keeps `aggregator_db` across pod restarts. The Aggregator pod runs ordered init containers that wait for PostgreSQL, execute the ConfigMap bootstrap SQL, run migrations, and seed `volcano_reference` plus initial source status rows. The bootstrap also creates the separate `auth` role and `auth_db` database.

RabbitMQ also uses a PVC so durable queues and persistent messages survive a pod restart. No database or broker port is exposed outside the cluster.

For a remote K3s cluster, publish the application images (including demo-api and demo-console) below a common registry prefix, then run:

```sh
IMAGE_REGISTRY=ghcr.io/your-org K3S_ENV_FILE=.env make k3s-deploy
```

The remote target substitutes `IMAGE_REGISTRY` into the overlay, creates the Secret, applies the LoadBalancer Client API service, and waits for the StatefulSet and Deployments. It assumes the cluster has a default StorageClass; K3s local-path storage is suitable for development.

To render without applying:

```sh
kubectl kustomize overlays/local
kubectl kustomize overlays/remote
```
