#!/bin/sh
set -eu

env_file="$(mktemp)"
umask 077

secret() {
  openssl rand -hex 24
}

project_name="m1e2e$(openssl rand -hex 4)"
bmkg_host_port=18081
pvmbg_host_port=18082
aggregator_http_host_port=18090
aggregator_grpc_host_port=19090
auth_grpc_host_port=19091
client_api_host_port=18080
aggregator_db_password=$(secret)
auth_db_password=$(secret)
postgres_admin_password=$(secret)
rabbitmq_user="e2e$(secret)"
rabbitmq_password=$(secret)

cat > "$env_file" <<EOF
BMKG_PORT=8081
PVMBG_PORT=8082
AGGREGATOR_GRPC_PORT=9090
AGGREGATOR_HTTP_PORT=8090
AUTH_GRPC_PORT=9091
CLIENT_API_PORT=8080
BMKG_HOST_PORT=$bmkg_host_port
PVMBG_HOST_PORT=$pvmbg_host_port
AGGREGATOR_HTTP_HOST_PORT=$aggregator_http_host_port
AGGREGATOR_GRPC_HOST_PORT=$aggregator_grpc_host_port
AUTH_GRPC_HOST_PORT=$auth_grpc_host_port
CLIENT_API_HOST_PORT=$client_api_host_port
BMKG_URL=http://bmkg:8081
PVMBG_URL=http://pvmbg:8082
AGGREGATOR_GRPC_ADDR=aggregator:9090
AUTH_GRPC_ADDR=auth:9091
BMKG_API_KEY=$(secret)
PVMBG_TOKEN=$(secret)
PVMBG_ADMIN_KEY=$(secret)
INTERNAL_SERVICE_TOKEN=$(secret)
MEDIA_CLIENT_SECRET=$(secret)
FIELD_TEAM_CLIENT_SECRET=$(secret)
BNPB_OPS_CLIENT_SECRET=$(secret)
SEED_COUNT=20
POLL_INTERVAL=500ms
NEW_EVENT_INTERVAL=1s
BMKG_DELAY=50ms
PVMBG_MIN_DELAY=10ms
PVMBG_MAX_DELAY=10ms
ACCESS_TOKEN_TTL=2s
MAX_INFLIGHT_REQUESTS=64
REQUEST_TIMEOUT=2s
STORE_DRIVER=postgres
AGGREGATOR_DATABASE_URL=postgres://aggregator:$aggregator_db_password@postgres:5432/aggregator_db?sslmode=disable
AUTH_DATABASE_URL=postgres://auth:$auth_db_password@postgres:5432/auth_db?sslmode=disable
RABBITMQ_URL=amqp://$rabbitmq_user:$rabbitmq_password@rabbitmq:5672/
POSTGRES_ADMIN_PASSWORD=$postgres_admin_password
AGGREGATOR_DB_PASSWORD=$aggregator_db_password
AUTH_DB_PASSWORD=$auth_db_password
RABBITMQ_USER=$rabbitmq_user
RABBITMQ_PASSWORD=$rabbitmq_password
EOF

compose() {
  docker compose --project-name "$project_name" --env-file "$env_file" "$@"
}

cleanup() {
  if [ "${E2E_KEEP_RUNNING:-0}" != "1" ]; then
    compose down -v --remove-orphans
  fi
  rm -f "$env_file"
}
trap cleanup EXIT INT TERM

compose up -d --build

attempt=0
until curl --fail --silent --show-error "http://127.0.0.1:$client_api_host_port/health" >/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    compose logs --no-color
    exit 1
  fi
  sleep 1
done

export CLIENT_API_URL="http://127.0.0.1:$client_api_host_port"
export BMKG_E2E_URL="http://127.0.0.1:$bmkg_host_port"
export PVMBG_E2E_URL="http://127.0.0.1:$pvmbg_host_port"
set -a
. "$env_file"
set +a

(cd tests/e2e && GOWORK=off go test -count=1 -v ./...)

before_schema=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT count(*) FROM hazard_events WHERE source = 'PVMBG' AND NOT (attributes ? 'confidence_level')")
after_schema=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT count(*) FROM hazard_events WHERE source = 'PVMBG' AND attributes ? 'confidence_level'")
[ "$before_schema" -gt 0 ]
[ "$after_schema" -gt 0 ]

aggregator_id=$(compose ps -q aggregator)
client_api_id=$(compose ps -q client-api)
compose up -d --no-deps --build pvmbg
[ "$aggregator_id" = "$(compose ps -q aggregator)" ]
[ "$client_api_id" = "$(compose ps -q client-api)" ]
field_token=$(curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"field-team-client\",\"client_secret\":\"$FIELD_TEAM_CLIENT_SECRET\"}" \
  "$CLIENT_API_URL/auth/token" | python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])')
curl --fail --silent --show-error \
  -H "Authorization: Bearer $field_token" \
  "$CLIENT_API_URL/hazards?source=BMKG" >/dev/null

compose stop notifier-consumer
sleep 3
queued=$(compose exec -T rabbitmq rabbitmqctl list_queues name messages | awk '$1 == "notifier.hazard-events" { print $2 }')
[ "${queued:-0}" -gt 0 ]
compose logs --no-color dashboard-consumer | grep -q 'event processed'
compose start notifier-consumer

attempt=0
while :; do
  queued=$(compose exec -T rabbitmq rabbitmqctl list_queues name messages | awk '$1 == "notifier.hazard-events" { print $2 }')
  if [ "${queued:-0}" -eq 0 ]; then
    break
  fi
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then
    exit 1
  fi
  sleep 1
done
compose logs --no-color notifier-consumer | grep -q 'event processed'
compose --profile portal up -d portal-consumer
sleep 2
compose logs --no-color portal-consumer | grep -q 'event processed'

compose stop rabbitmq
sleep 3
pending=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT count(*) FROM outbox_messages WHERE published_at IS NULL")
[ "$pending" -gt 0 ]
compose start rabbitmq

attempt=0
while :; do
  pending=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT count(*) FROM outbox_messages WHERE published_at IS NULL")
  if [ "$pending" -eq 0 ]; then
    break
  fi
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then
    exit 1
  fi
  sleep 1
done
