# Remote K3s provisioning

`alchemy.run.ts` is deliberately limited to infrastructure orchestration. Provider credentials, VM IDs, firewall rules, and the K3s token must be injected by the Alchemy runtime; they are not stored in this repository.

The server cloud-init installs K3s and writes the join token on the server. The agent cloud-init consumes `K3S_URL` and `K3S_TOKEN` from provisioning-time secret environment. After provisioning, configure the kubeconfig on the deployment host and deploy the workload with:

```sh
IMAGE_REGISTRY=ghcr.io/your-org K3S_ENV_FILE=.env make k3s-deploy
```

The target creates the environment-backed Secret, applies the remote Kustomize overlay, bootstraps persistent PostgreSQL/RabbitMQ storage, and waits for the application rollout. It does not place credentials in Alchemy source or manifests.
