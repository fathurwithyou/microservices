.PHONY: help tools generate test lint build compose-up compose-down k3d-up k3d-deploy k3d-scale k3d-down test-contract test-integration test-e2e test-load test-load-isolation frontend-test demo-up test-demo-e2e migrate-up migrate-down migrate-status seed m2-verify m2-deploy m2-smoke m2-evidence m2-load

K3D_CLUSTER ?= bnpb-m1
K3D_HOST_PORT ?= 8080
K3D_IMAGE_PREFIX ?= bnpb-m1
K3S_NAMESPACE ?= bnpb-m1
K3S_ENV_FILE ?= .env
K3D_SERVICES := bmkg pvmbg aggregator auth client-api dashboard-consumer notifier-consumer portal-consumer satellite-ingestor coastal-ingestor demo-api
K3D_DEPLOYMENTS := bmkg pvmbg aggregator auth client-api dashboard-consumer notifier-consumer portal-consumer satellite-ingestor coastal-ingestor critical-notifier rabbitmq demo-api demo-console
K3D_SCALABLE_DEPLOYMENTS := client-api auth dashboard-consumer notifier-consumer portal-consumer

help:
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' Makefile | sed 's/:.*## / - /'

tools: ## Install local helper CLIs (protoc plugins, goose)
	@mkdir -p .bin contracts/gen/go
	@GOBIN=$${PWD}/.bin go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
	@GOBIN=$${PWD}/.bin go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
	@GOBIN=$${PWD}/.bin go install github.com/pressly/goose/v3/cmd/goose@latest

generate: tools ## Generate protobuf Go code; requires protoc and Go protobuf plugins
	@PATH=$${PWD}/.bin:$$PATH protoc -I contracts/proto --go_out=contracts/gen/go --go_opt=paths=source_relative --go-grpc_out=contracts/gen/go --go-grpc_opt=paths=source_relative contracts/proto/events/hazard_event.proto contracts/proto/aggregator/aggregator.proto contracts/proto/auth/auth.proto

test: ## Run all Go unit tests
	@go test ./contracts/...
	@for module in services/bmkg services/pvmbg services/aggregator services/auth services/client-api services/dashboard-consumer services/notifier-consumer services/portal-consumer services/satellite-ingestor services/coastal-ingestor services/demo-api; do (cd $$module && go test ./...); done

lint: ## Run gofmt, vet, and optional staticcheck
	@gofmt -w $$(find contracts services -name '*.go')
	@set -e; go vet ./contracts/...; for module in services/bmkg services/pvmbg services/aggregator services/auth services/client-api services/dashboard-consumer services/notifier-consumer services/portal-consumer services/satellite-ingestor services/coastal-ingestor services/demo-api; do (cd $$module && go vet ./...); done
	@if command -v staticcheck >/dev/null 2>&1; then staticcheck ./contracts/...; for module in services/bmkg services/pvmbg services/aggregator services/auth services/client-api services/dashboard-consumer services/notifier-consumer services/portal-consumer services/satellite-ingestor services/coastal-ingestor services/demo-api; do (cd $$module && staticcheck ./...); done; fi

build: generate ## Build every service binary
	@mkdir -p bin
	@go build -o bin/bmkg ./services/bmkg/cmd/server
	@go build -o bin/pvmbg ./services/pvmbg/cmd/server
	@go build -o bin/aggregator ./services/aggregator/cmd/server
	@go build -o bin/auth ./services/auth/cmd/server
	@go build -o bin/client-api ./services/client-api/cmd/server
	@go build -o bin/dashboard-consumer ./services/dashboard-consumer/cmd/server
	@go build -o bin/notifier-consumer ./services/notifier-consumer/cmd/server
	@go build -o bin/portal-consumer ./services/portal-consumer/cmd/server
	@go build -o bin/satellite-ingestor ./services/satellite-ingestor/cmd/server
	@go build -o bin/coastal-ingestor ./services/coastal-ingestor/cmd/server
	@go build -o bin/demo-api ./services/demo-api/cmd/server

compose-up: ## Start all local containers; use COMPOSE_ENV_FILE to select an env file
	@docker compose --env-file $${COMPOSE_ENV_FILE:-.env} up --build

compose-down: ## Stop local containers and remove compose-created resources
	@docker compose --env-file $${COMPOSE_ENV_FILE:-.env} down

k3d-up: ## Create the local K3d cluster when it does not already exist
	@if k3d cluster list | awk 'NR > 1 && $$1 == "$(K3D_CLUSTER)" { found = 1 } END { exit !found }'; then \
		echo "K3d cluster $(K3D_CLUSTER) already exists"; \
	else \
		k3d cluster create $(K3D_CLUSTER) --agents 2 --port '$(K3D_HOST_PORT):30080@loadbalancer' --wait; \
	fi

k3d-deploy: k3d-up ## Build local images and apply the Kustomize local overlay
	@test -f $(K3S_ENV_FILE) || (echo "missing $(K3S_ENV_FILE); copy .env.example and fill the deployment secrets" >&2; exit 1)
	@kubectl create namespace $(K3S_NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	@kubectl -n $(K3S_NAMESPACE) create secret generic bnpb-secrets --from-env-file=$(K3S_ENV_FILE) --dry-run=client -o yaml | kubectl apply -f -
	@for service in $(K3D_SERVICES); do docker build -t $(K3D_IMAGE_PREFIX)/$$service:local -f services/$$service/Dockerfile .; done
	@docker build -t $(K3D_IMAGE_PREFIX)/demo-console:local -f apps/demo-console/Dockerfile .
	@k3d image import -c $(K3D_CLUSTER) $(foreach service,$(K3D_SERVICES),$(K3D_IMAGE_PREFIX)/$(service):local) $(K3D_IMAGE_PREFIX)/demo-console:local
	@kubectl apply -k deploy/kustomize/overlays/local
	@kubectl -n $(K3S_NAMESPACE) rollout status statefulset/postgres --timeout=180s
	@for deployment in $(K3D_DEPLOYMENTS); do kubectl -n $(K3S_NAMESPACE) rollout restart deployment/$$deployment; done
	@for deployment in $(K3D_DEPLOYMENTS); do kubectl -n $(K3S_NAMESPACE) rollout status deployment/$$deployment --timeout=180s || exit $$?; done

k3d-scale: ## Scale one stateless deployment; use K3D_SCALE_SERVICE and K3D_REPLICAS
	@test -n "$(K3D_SCALE_SERVICE)" || (echo "K3D_SCALE_SERVICE is required" >&2; exit 1)
	@case " $(K3D_SCALABLE_DEPLOYMENTS) " in *" $(K3D_SCALE_SERVICE) "*) ;; *) echo "unsupported scalable deployment: $(K3D_SCALE_SERVICE); allowed: $(K3D_SCALABLE_DEPLOYMENTS)" >&2; exit 1;; esac
	@test "$(K3D_REPLICAS)" -ge 1 || (echo "K3D_REPLICAS must be >= 1" >&2; exit 1)
	@kubectl -n $(K3S_NAMESPACE) scale deployment/$(K3D_SCALE_SERVICE) --replicas=$(K3D_REPLICAS)
	@kubectl -n $(K3S_NAMESPACE) rollout status deployment/$(K3D_SCALE_SERVICE) --timeout=180s

k3d-down: ## Delete the local K3d cluster
	@k3d cluster delete $(K3D_CLUSTER)

test-contract: ## Run source contract checks
	@(cd tests/contract && GOWORK=off go test ./...)

test-integration: ## Run integration tests; services must be running
	@(cd tests/integration && GOWORK=off go test ./...)

test-e2e: ## Build an isolated Compose stack and run P1-P5 end-to-end checks
	@./scripts/test-e2e.sh

test-load: ## Run authenticated k6 load test; BASE_URL and FIELD_TEAM_CLIENT_SECRET are required
	@mkdir -p artifacts
	@k6 run -e BASE_URL=$${BASE_URL:-http://localhost:8080} -e FIELD_TEAM_CLIENT_SECRET=$${FIELD_TEAM_CLIENT_SECRET:?set FIELD_TEAM_CLIENT_SECRET} -e K6_SUMMARY_PATH=$${K6_SUMMARY_PATH:-artifacts/p2-sustained.json} tests/load/k6/sustained.js

test-load-isolation: ## Concurrent BMKG-only vs PVMBG load; set PVMBG_MAX_DELAY=3s before compose-up
	@mkdir -p artifacts
	@k6 run -e BASE_URL=$${BASE_URL:-http://localhost:8080} -e FIELD_TEAM_CLIENT_SECRET=$${FIELD_TEAM_CLIENT_SECRET:?set FIELD_TEAM_CLIENT_SECRET} -e K6_SUMMARY_PATH=$${K6_SUMMARY_PATH:-artifacts/p2-isolation.json} tests/load/k6/isolation.js

frontend-test: ## Run Demo Console unit tests
	@cd apps/demo-console && npm test

demo-up: ## Start the stack including demo-api, demo-console, and portal consumer
	@docker compose --env-file $${COMPOSE_ENV_FILE:-.env} --profile demo --profile portal up --build

test-demo-e2e: ## Playwright smoke against DEMO_CONSOLE_URL (default http://127.0.0.1:3000)
	@cd apps/demo-console && npx playwright test


migrate-up: ## Apply pending aggregator migrations via one-shot container
	@docker compose --env-file $${COMPOSE_ENV_FILE:-.env} run --rm aggregator migrate up

migrate-down: ## Roll back the latest migration (destructive, development only)
	@docker compose --env-file $${COMPOSE_ENV_FILE:-.env} run --rm aggregator migrate down --yes

migrate-status: ## Show applied and pending aggregator migrations
	@docker compose --env-file $${COMPOSE_ENV_FILE:-.env} run --rm aggregator migrate status

seed: migrate-up ## Apply migrations, then insert minimal reference rows
	@docker compose --env-file $${COMPOSE_ENV_FILE:-.env} run --rm aggregator seed

m2-verify: ## Format, vet, test, and render M2 cluster manifests
	@test -z "$$(gofmt -l contracts services pkg)" || (echo "gofmt required:"; gofmt -l contracts services pkg; exit 1)
	@$(MAKE) test
	@$(MAKE) lint
	@kubectl kustomize deploy/kustomize/overlays/cluster >/dev/null
	@echo "M2 verification passed"

m2-deploy: ## Apply the M2 cluster overlay
	@kubectl apply -k deploy/kustomize/overlays/cluster

m2-smoke: ## Run health, audit, and field-report smoke checks
	@./scripts/m2-smoke.sh

m2-evidence: ## Generate M2 evidence artifacts
	@./scripts/m2-evidence.sh

m2-load: ## Run M2 10k-VU load scenario with 3->5->2 scaling and rolling update
	@./scripts/m2-load.sh
