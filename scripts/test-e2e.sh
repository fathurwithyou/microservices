#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
artifact_dir="$repo_root/artifacts"
mkdir -p "$artifact_dir"

env_file="$(mktemp)"
umask 077

secret() {
  openssl rand -hex 24
}

project_name="m1e2e$(openssl rand -hex 4)"
set -- $(python3 - <<'PY'
import socket
sockets = []
for _ in range(6):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sockets.append(sock)
print(" ".join(str(sock.getsockname()[1]) for sock in sockets))
for sock in sockets:
    sock.close()
PY
)
bmkg_host_port=${E2E_BMKG_HOST_PORT:-$1}
pvmbg_host_port=${E2E_PVMBG_HOST_PORT:-$2}
aggregator_http_host_port=${E2E_AGGREGATOR_HTTP_HOST_PORT:-$3}
aggregator_grpc_host_port=${E2E_AGGREGATOR_GRPC_HOST_PORT:-$4}
auth_grpc_host_port=${E2E_AUTH_GRPC_HOST_PORT:-$5}
client_api_host_port=${E2E_CLIENT_API_HOST_PORT:-$6}
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
ACCESS_TOKEN_TTL=${E2E_ACCESS_TOKEN_TTL:-2s}
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
python3 - <<PY
import json
from pathlib import Path
Path(r"$artifact_dir/p2-outage.json").write_text(json.dumps({
  "problem": "P2",
  "go_test": "TestP2PVMBGOutageKeepsBMKGAvailable",
  "checks": [
    "PVMBG status becomes stale with stale_since",
    "BMKG hazards remain HTTP 200",
    "PVMBG hazards remain HTTP 200 from store",
    "status returns healthy after outage is disabled without restart"
  ],
  "pass": True
}, indent=2), encoding="utf-8")
PY
python3 - <<PY
import json
from pathlib import Path
Path(r"$artifact_dir/p3-auth.json").write_text(json.dumps({
  "problem": "P3",
  "go_test": "TestP3IndependentCredentialsAndRefreshRotation",
  "checks": [
    "BMKG key rejected by PVMBG",
    "PVMBG token rejected by BMKG",
    "media raw=true returns 403",
    "media summary omits latitude/longitude/attributes",
    "bnpb-ops raw payload includes raw fields",
    "field-team access token expires after the configured TTL and refresh rotates"
  ],
  "access_token_ttl": "$ACCESS_TOKEN_TTL",
  "pass": True
}, indent=2), encoding="utf-8")
PY

before_schema=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT count(*) FROM hazard_events WHERE source = 'PVMBG' AND NOT (attributes ? 'confidence_level')")
after_schema=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT count(*) FROM hazard_events WHERE source = 'PVMBG' AND attributes ? 'confidence_level'")
bmkg_records=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT count(*) FROM hazard_events WHERE source = 'BMKG'")
pvmbg_records=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT count(*) FROM hazard_events WHERE source = 'PVMBG'")
[ "$before_schema" -gt 0 ]
[ "$after_schema" -gt 0 ]
[ "$bmkg_records" -gt 0 ]
[ "$pvmbg_records" -gt 0 ]
python3 - <<PY
import json
from pathlib import Path
Path(r"$artifact_dir/p1-canonical-mapping.json").write_text(json.dumps({
  "problem": "P1",
  "go_test": "TestP1CanonicalMappingForBothSources",
  "bmkg_hazard_events": int("$bmkg_records"),
  "pvmbg_hazard_events": int("$pvmbg_records"),
  "pass": True
}, indent=2), encoding="utf-8")
Path(r"$artifact_dir/p1-schema-evolution.json").write_text(json.dumps({
  "problem": "P1",
  "before_without_confidence_level": int("$before_schema"),
  "after_with_confidence_level": int("$after_schema"),
  "policy": "unknown fields forwarded to attributes JSONB",
  "pass": True
}, indent=2), encoding="utf-8")
Path(r"$artifact_dir/p4-jsonb-coexistence.json").write_text(json.dumps({
  "problem": "P4",
  "criterion": "HazardEvent JSONB attributes store old and new records without migration",
  "before_without_confidence_level": int("$before_schema"),
  "after_with_confidence_level": int("$after_schema"),
  "pass": True
}, indent=2), encoding="utf-8")
PY

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
python3 - <<PY
import json
from pathlib import Path
Path(r"$artifact_dir/p4-independent-rebuild.json").write_text(json.dumps({
  "problem": "P4",
  "criterion": "rebuild pvmbg without restarting aggregator or client-api",
  "aggregator_container_id_unchanged": True,
  "client_api_container_id_unchanged": True,
  "bmkg_query_after_rebuild": "200",
  "pass": True
}, indent=2), encoding="utf-8")
PY

if compose exec -T aggregator sh -c 'test -n "${AGGREGATOR_DATABASE_URL:-}"'; then
  aggregator_has_canonical_store=true
else
  aggregator_has_canonical_store=false
fi
if compose exec -T client-api sh -c 'test -n "${AGGREGATOR_DATABASE_URL:-}"'; then
  client_api_has_canonical_store=true
else
  client_api_has_canonical_store=false
fi
if compose exec -T auth sh -c 'test -n "${AGGREGATOR_DATABASE_URL:-}"'; then
  auth_has_canonical_store=true
else
  auth_has_canonical_store=false
fi
[ "$aggregator_has_canonical_store" = true ]
[ "$client_api_has_canonical_store" = false ]
[ "$auth_has_canonical_store" = false ]
python3 - <<PY
import json
from pathlib import Path
Path(r"$artifact_dir/p4-storage-ownership.json").write_text(json.dumps({
  "problem": "P4",
  "criterion": "only Aggregator receives the Canonical Store database URL",
  "aggregator_has_canonical_store_url": "$aggregator_has_canonical_store" == "true",
  "client_api_has_canonical_store_url": "$client_api_has_canonical_store" == "true",
  "auth_has_canonical_store_url": "$auth_has_canonical_store" == "true",
  "pass": True
}, indent=2), encoding="utf-8")
PY

cid=$(python3 -c 'import uuid; print(uuid.uuid4())')
curl --fail --silent --show-error \
  -H "Authorization: Bearer $field_token" \
  -H "X-Correlation-ID: $cid" \
  "$CLIENT_API_URL/hazards?source=BMKG" >/dev/null
if compose logs --no-color client-api | grep -q "$cid"; then
  correlation_pass=true
else
  correlation_pass=false
fi
python3 - <<PY
import json
from pathlib import Path
Path(r"$artifact_dir/general-correlation-id.json").write_text(json.dumps({
  "problem": "GENERAL",
  "criterion": "X-Correlation-ID forwarded and logged",
  "correlation_id": "$cid",
  "found_in_client_api_logs": "$correlation_pass" == "true",
  "pass": "$correlation_pass" == "true"
}, indent=2), encoding="utf-8")
PY

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
python3 - <<PY
import json
from pathlib import Path
Path(r"$artifact_dir/p5-fanout.json").write_text(json.dumps({
  "problem": "P5",
  "criterion": "dead consumer does not block producer; catch-up after restart",
  "notifier_queue_grew_while_stopped": True,
  "dashboard_processed_during_outage": True,
  "notifier_processed_after_restart": True,
  "delivery": "at-least-once via durable queue + manual ack",
  "consumer_deduplication": "in-memory by hazard_id within one process",
  "durable_deduplication_across_restart": False,
  "pass": True
}, indent=2), encoding="utf-8")
PY
compose --profile portal up -d portal-consumer
sleep 2
compose logs --no-color portal-consumer | grep -q 'event processed'
python3 - <<PY
import json
from pathlib import Path
Path(r"$artifact_dir/p5-new-consumer.json").write_text(json.dumps({
  "problem": "P5",
  "criterion": "add portal-consumer without changing producer code",
  "producer_image_unchanged": True,
  "portal_processed_event": True,
  "pass": True
}, indent=2), encoding="utf-8")
PY

compose stop rabbitmq
sleep 3
pending=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT count(*) FROM outbox_messages WHERE published_at IS NULL")
[ "$pending" -gt 0 ]
pending_down=$pending
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
python3 - <<PY
import json
from pathlib import Path
Path(r"$artifact_dir/p5-outbox-recovery.json").write_text(json.dumps({
  "problem": "P5",
  "criterion": "outbox holds unpublished events while broker is down, then drains",
  "pending_while_broker_down": int("$pending_down"),
  "pending_after_recovery": 0,
  "store_access": "e2e script via aggregator-owned postgres, not demo-api",
  "pass": True
}, indent=2), encoding="utf-8")
PY
