@echo off
setlocal EnableDelayedExpansion

if "%COMPOSE_ENV_FILE%"=="" set "COMPOSE_ENV_FILE=.env"
if "%K3D_CLUSTER%"=="" set "K3D_CLUSTER=bnpb-m1"
if "%K3D_HOST_PORT%"=="" set "K3D_HOST_PORT=8080"
if "%K3D_IMAGE_PREFIX%"=="" set "K3D_IMAGE_PREFIX=bnpb-m1"
if "%K3S_NAMESPACE%"=="" set "K3S_NAMESPACE=bnpb-m1"
if "%K3S_ENV_FILE%"=="" set "K3S_ENV_FILE=.env"
if "%IMAGE_REGISTRY%"=="" set "IMAGE_REGISTRY=ghcr.io/fathurwithyou"

set "TARGET=%~1"
if "%TARGET%"=="" goto help
if /i "%TARGET%"=="help" goto help

if /i "%TARGET%"=="demo-up" goto demo-up
if /i "%TARGET%"=="compose-up" goto compose-up
if /i "%TARGET%"=="compose-down" goto compose-down
if /i "%TARGET%"=="migrate-up" goto migrate-up
if /i "%TARGET%"=="migrate-down" goto migrate-down
if /i "%TARGET%"=="migrate-status" goto migrate-status
if /i "%TARGET%"=="seed" goto seed
if /i "%TARGET%"=="frontend-test" goto frontend-test
if /i "%TARGET%"=="test-demo-e2e" goto test-demo-e2e
if /i "%TARGET%"=="tools" goto tools
if /i "%TARGET%"=="generate" goto generate
if /i "%TARGET%"=="test" goto test
if /i "%TARGET%"=="build" goto build
if /i "%TARGET%"=="test-contract" goto test-contract
if /i "%TARGET%"=="test-integration" goto test-integration
if /i "%TARGET%"=="test-e2e" goto test-e2e
if /i "%TARGET%"=="test-load" goto test-load
if /i "%TARGET%"=="test-load-isolation" goto test-load-isolation
if /i "%TARGET%"=="k3d-up" goto k3d-up
if /i "%TARGET%"=="k3d-down" goto k3d-down
if /i "%TARGET%"=="k3d-deploy" goto k3d-deploy
if /i "%TARGET%"=="k3d-scale" goto k3d-scale
if /i "%TARGET%"=="k3s-deploy" goto k3s-deploy
if /i "%TARGET%"=="lint" goto lint

echo Unknown target: %TARGET%
echo Run make.bat help
exit /b 1

:help
echo Windows wrapper for Makefile targets. Usage: make.bat ^<target^>
echo.
echo   demo-up              - Compose with demo + portal profiles
echo   compose-up           - docker compose up --build
echo   compose-down         - docker compose down
echo   migrate-up           - apply aggregator migrations
echo   migrate-down         - roll back latest migration
echo   migrate-status       - migration status
echo   seed                 - migrate-up then seed
echo   tools                - install protoc/goose into .bin
echo   generate             - protobuf codegen
echo   test                 - Go unit tests
echo   lint                 - gofmt + go vet (staticcheck if installed)
echo   build                - build service binaries into bin\
echo   frontend-test        - Demo Console Vitest
echo   test-demo-e2e        - Playwright smoke
echo   test-contract        - contract tests
echo   test-integration     - integration tests
echo   test-e2e             - full e2e (requires bash, e.g. Git Bash)
echo   test-load            - k6 sustained load
echo   test-load-isolation  - k6 isolation load
echo   k3d-up / k3d-down / k3d-deploy / k3d-scale / k3s-deploy
echo.
echo Set COMPOSE_ENV_FILE to override the default .env file.
exit /b 0

:demo-up
docker compose --env-file "%COMPOSE_ENV_FILE%" --profile demo --profile portal up --build
exit /b !ERRORLEVEL!

:compose-up
docker compose --env-file "%COMPOSE_ENV_FILE%" up --build
exit /b !ERRORLEVEL!

:compose-down
docker compose --env-file "%COMPOSE_ENV_FILE%" down
exit /b !ERRORLEVEL!

:migrate-up
docker compose --env-file "%COMPOSE_ENV_FILE%" run --rm aggregator migrate up
exit /b !ERRORLEVEL!

:migrate-down
docker compose --env-file "%COMPOSE_ENV_FILE%" run --rm aggregator migrate down --yes
exit /b !ERRORLEVEL!

:migrate-status
docker compose --env-file "%COMPOSE_ENV_FILE%" run --rm aggregator migrate status
exit /b !ERRORLEVEL!

:seed
call :migrate-up
if errorlevel 1 exit /b 1
docker compose --env-file "%COMPOSE_ENV_FILE%" run --rm aggregator seed
exit /b !ERRORLEVEL!

:frontend-test
pushd apps\demo-console
call npm test
set "RC=!ERRORLEVEL!"
popd
exit /b !RC!

:test-demo-e2e
pushd apps\demo-console
call npx playwright test
set "RC=!ERRORLEVEL!"
popd
exit /b !RC!

:tools
if not exist .bin mkdir .bin
if not exist contracts\gen\go mkdir contracts\gen\go
set "GOBIN=%CD%\.bin"
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
if errorlevel 1 exit /b 1
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
if errorlevel 1 exit /b 1
go install github.com/pressly/goose/v3/cmd/goose@latest
exit /b !ERRORLEVEL!

:generate
call :tools
if errorlevel 1 exit /b 1
set "PATH=%CD%\.bin;%PATH%"
protoc -I contracts/proto --go_out=contracts/gen/go --go_opt=paths=source_relative --go-grpc_out=contracts/gen/go --go-grpc_opt=paths=source_relative contracts/proto/events/hazard_event.proto contracts/proto/aggregator/aggregator.proto contracts/proto/auth/auth.proto
exit /b !ERRORLEVEL!

:test
go test ./contracts/...
if errorlevel 1 exit /b 1
for %%M in (services/bmkg services/pvmbg services/aggregator services/auth services/client-api services/dashboard-consumer services/notifier-consumer services/portal-consumer services/demo-api) do (
  pushd %%M
  go test ./...
  if errorlevel 1 (
    popd
    exit /b 1
  )
  popd
)
exit /b 0

:build
call :generate
if errorlevel 1 exit /b 1
if not exist bin mkdir bin
go build -o bin/bmkg ./services/bmkg/cmd/server
if errorlevel 1 exit /b 1
go build -o bin/pvmbg ./services/pvmbg/cmd/server
if errorlevel 1 exit /b 1
go build -o bin/aggregator ./services/aggregator/cmd/server
if errorlevel 1 exit /b 1
go build -o bin/auth ./services/auth/cmd/server
if errorlevel 1 exit /b 1
go build -o bin/client-api ./services/client-api/cmd/server
if errorlevel 1 exit /b 1
go build -o bin/dashboard-consumer ./services/dashboard-consumer/cmd/server
if errorlevel 1 exit /b 1
go build -o bin/notifier-consumer ./services/notifier-consumer/cmd/server
if errorlevel 1 exit /b 1
go build -o bin/portal-consumer ./services/portal-consumer/cmd/server
if errorlevel 1 exit /b 1
go build -o bin/demo-api ./services/demo-api/cmd/server
exit /b !ERRORLEVEL!

:lint
for /r contracts %%f in (*.go) do gofmt -w "%%f"
for /r services %%f in (*.go) do gofmt -w "%%f"
go vet ./contracts/...
if errorlevel 1 exit /b 1
for %%M in (services/bmkg services/pvmbg services/aggregator services/auth services/client-api services/dashboard-consumer services/notifier-consumer services/portal-consumer services/demo-api) do (
  pushd %%M
  go vet ./...
  if errorlevel 1 (
    popd
    exit /b 1
  )
  popd
)
where staticcheck >nul 2>&1
if errorlevel 1 exit /b 0
staticcheck ./contracts/...
if errorlevel 1 exit /b 1
for %%M in (services/bmkg services/pvmbg services/aggregator services/auth services/client-api services/dashboard-consumer services/notifier-consumer services/portal-consumer services/demo-api) do (
  pushd %%M
  staticcheck ./...
  if errorlevel 1 (
    popd
    exit /b 1
  )
  popd
)
exit /b 0

:test-contract
pushd tests\contract
set "GOWORK=off"
go test ./...
set "RC=!ERRORLEVEL!"
popd
exit /b !RC!

:test-integration
pushd tests\integration
set "GOWORK=off"
go test ./...
set "RC=!ERRORLEVEL!"
popd
exit /b !RC!

:test-e2e
where bash >nul 2>&1
if errorlevel 1 (
  echo test-e2e requires bash. Install Git for Windows and ensure bash is on PATH.
  exit /b 1
)
bash scripts/test-e2e.sh
exit /b !ERRORLEVEL!

:test-load
if not exist artifacts mkdir artifacts
if "%FIELD_TEAM_CLIENT_SECRET%"=="" (
  echo FIELD_TEAM_CLIENT_SECRET is required
  exit /b 1
)
if "%BASE_URL%"=="" set "BASE_URL=http://localhost:8080"
if "%K6_SUMMARY_PATH%"=="" set "K6_SUMMARY_PATH=artifacts/p2-sustained.json"
k6 run -e BASE_URL=%BASE_URL% -e FIELD_TEAM_CLIENT_SECRET=%FIELD_TEAM_CLIENT_SECRET% -e K6_SUMMARY_PATH=%K6_SUMMARY_PATH% tests/load/k6/sustained.js
exit /b !ERRORLEVEL!

:test-load-isolation
if not exist artifacts mkdir artifacts
if "%FIELD_TEAM_CLIENT_SECRET%"=="" (
  echo FIELD_TEAM_CLIENT_SECRET is required
  exit /b 1
)
if "%BASE_URL%"=="" set "BASE_URL=http://localhost:8080"
if "%K6_SUMMARY_PATH%"=="" set "K6_SUMMARY_PATH=artifacts/p2-isolation.json"
k6 run -e BASE_URL=%BASE_URL% -e FIELD_TEAM_CLIENT_SECRET=%FIELD_TEAM_CLIENT_SECRET% -e K6_SUMMARY_PATH=%K6_SUMMARY_PATH% tests/load/k6/isolation.js
exit /b !ERRORLEVEL!

:k3d-up
k3d cluster list | findstr /R /C:"^%K3D_CLUSTER% " >nul 2>&1
if not errorlevel 1 (
  echo K3d cluster %K3D_CLUSTER% already exists
  exit /b 0
)
k3d cluster create %K3D_CLUSTER% --agents 2 --port "%K3D_HOST_PORT%:30080@loadbalancer" --wait
exit /b !ERRORLEVEL!

:k3d-down
k3d cluster delete %K3D_CLUSTER%
exit /b !ERRORLEVEL!

:k3d-deploy
call :k3d-up
if errorlevel 1 exit /b 1
if not exist "%K3S_ENV_FILE%" (
  echo missing %K3S_ENV_FILE%; copy .env.example and fill the deployment secrets
  exit /b 1
)
kubectl create namespace %K3S_NAMESPACE% --dry-run=client -o yaml | kubectl apply -f -
kubectl -n %K3S_NAMESPACE% create secret generic bnpb-secrets --from-env-file=%K3S_ENV_FILE% --dry-run=client -o yaml | kubectl apply -f -
for %%S in (bmkg pvmbg aggregator auth client-api dashboard-consumer notifier-consumer portal-consumer demo-api) do (
  docker build -t %K3D_IMAGE_PREFIX%/%%S:local -f services/%%S/Dockerfile .
  if errorlevel 1 exit /b 1
)
docker build -t %K3D_IMAGE_PREFIX%/demo-console:local -f apps/demo-console/Dockerfile .
if errorlevel 1 exit /b 1
k3d image import -c %K3D_CLUSTER% %K3D_IMAGE_PREFIX%/bmkg:local %K3D_IMAGE_PREFIX%/pvmbg:local %K3D_IMAGE_PREFIX%/aggregator:local %K3D_IMAGE_PREFIX%/auth:local %K3D_IMAGE_PREFIX%/client-api:local %K3D_IMAGE_PREFIX%/dashboard-consumer:local %K3D_IMAGE_PREFIX%/notifier-consumer:local %K3D_IMAGE_PREFIX%/portal-consumer:local %K3D_IMAGE_PREFIX%/demo-api:local %K3D_IMAGE_PREFIX%/demo-console:local
if errorlevel 1 exit /b 1
kubectl apply -k deploy/kustomize/overlays/local
if errorlevel 1 exit /b 1
kubectl -n %K3S_NAMESPACE% rollout status statefulset/postgres --timeout=180s
if errorlevel 1 exit /b 1
for %%D in (bmkg pvmbg aggregator auth client-api dashboard-consumer notifier-consumer portal-consumer rabbitmq demo-api demo-console) do (
  kubectl -n %K3S_NAMESPACE% rollout restart deployment/%%D
)
for %%D in (bmkg pvmbg aggregator auth client-api dashboard-consumer notifier-consumer portal-consumer rabbitmq demo-api demo-console) do (
  kubectl -n %K3S_NAMESPACE% rollout status deployment/%%D --timeout=180s
  if errorlevel 1 exit /b 1
)
exit /b 0

:k3d-scale
if "%K3D_SCALE_SERVICE%"=="" (
  echo K3D_SCALE_SERVICE is required
  exit /b 1
)
if "%K3D_REPLICAS%"=="" (
  echo K3D_REPLICAS is required
  exit /b 1
)
kubectl -n %K3S_NAMESPACE% scale deployment/%K3D_SCALE_SERVICE% --replicas=%K3D_REPLICAS%
if errorlevel 1 exit /b 1
kubectl -n %K3S_NAMESPACE% rollout status deployment/%K3D_SCALE_SERVICE% --timeout=180s
exit /b !ERRORLEVEL!

:k3s-deploy
if "%IMAGE_REGISTRY%"=="" (
  echo IMAGE_REGISTRY is required
  exit /b 1
)
if not exist "%K3S_ENV_FILE%" (
  echo missing %K3S_ENV_FILE%; provide deployment secrets
  exit /b 1
)
kubectl create namespace %K3S_NAMESPACE% --dry-run=client -o yaml | kubectl apply -f -
kubectl -n %K3S_NAMESPACE% create secret generic bnpb-secrets --from-env-file=%K3S_ENV_FILE% --dry-run=client -o yaml | kubectl apply -f -
kubectl kustomize deploy/kustomize/overlays/remote | powershell -NoProfile -Command "$input | ForEach-Object { $_ -replace 'ghcr.io/fathurwithyou', '%IMAGE_REGISTRY%' }" | kubectl apply -f -
if errorlevel 1 exit /b 1
kubectl -n %K3S_NAMESPACE% rollout status statefulset/postgres --timeout=180s
if errorlevel 1 exit /b 1
for %%D in (bmkg pvmbg aggregator auth client-api dashboard-consumer notifier-consumer portal-consumer rabbitmq demo-api demo-console) do (
  kubectl -n %K3S_NAMESPACE% rollout restart deployment/%%D
)
for %%D in (bmkg pvmbg aggregator auth client-api dashboard-consumer notifier-consumer portal-consumer rabbitmq demo-api demo-console) do (
  kubectl -n %K3S_NAMESPACE% rollout status deployment/%%D --timeout=180s
  if errorlevel 1 exit /b 1
)
exit /b 0
