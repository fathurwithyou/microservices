#!/bin/sh
set -eu

umask 077
repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
artifact_dir="$repo_root/artifacts"
mkdir -p "$artifact_dir"
run_timestamp=$(date -u '+%Y%m%dT%H%M%SZ')
run_id="e2e-${run_timestamp}-$$-$(openssl rand -hex 4)"
evidence_dir="$artifact_dir/$run_id"
mkdir "$evidence_dir"
run_started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

env_file="$(mktemp)"

secret() {
  openssl rand -hex 24
}

project_name="m1e2e$(openssl rand -hex 4)"
python3 - <<PY
import json
from pathlib import Path
Path(r"$evidence_dir/run.json").write_text(json.dumps({
  "run_id": "$run_id",
  "compose_project": "$project_name",
  "started_at_utc": "$run_started_at",
  "evidence_scope": "observations captured by the E2E harness; no credentials or tokens"
}, indent=2) + "\n", encoding="utf-8")
PY
REPO_ROOT="$repo_root" EVIDENCE_DIR="$evidence_dir" python3 - <<'PY'
import json
import os
import re
import subprocess
from pathlib import Path

repo = Path(os.environ["REPO_ROOT"])
evidence = Path(os.environ["EVIDENCE_DIR"])
tracked = subprocess.run(
    ["git", "-C", str(repo), "ls-files", "-z"],
    check=True,
    capture_output=True,
).stdout.decode().split("\0")
tracked = [path for path in tracked if path]
environment_files = [
    path for path in tracked
    if Path(path).name == ".env" or Path(path).name.startswith(".env.")
]
non_example_environment_files = [
    path for path in environment_files if Path(path).name != ".env.example"
]
credential_patterns = [
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"),
    re.compile(
        r"(?i)\b(?:password|passwd|secret|client_secret|api[_-]?key|"
        r"access[_-]?token|private[_-]?key)\s*[:=]\s*['\"`]"
        r"(?!\$\{)[A-Za-z0-9+/=_-]{20,}['\"`]"
    ),
]
scan_roots = ("services/", "apps/", "deploy/", "infra/", "scripts/", "tests/", ".github/")
candidate_files = []
for relative in tracked:
    if not relative.startswith(scan_roots) or Path(relative).name == ".env.example":
        continue
    path = repo / relative
    if not path.is_file() or path.stat().st_size > 2_000_000:
        continue
    try:
        source = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        continue
    if any(pattern.search(source) for pattern in credential_patterns):
        candidate_files.append(relative)
result = {
    "problem": "P3",
    "tracked_env_example_present": ".env.example" in tracked,
    "tracked_non_example_environment_file_count": len(non_example_environment_files),
    "credential_signature_candidate_file_count": len(candidate_files),
    "scan_scope": "tracked implementation and deployment text under services, apps, deploy, infra, scripts, tests, and .github",
    "scan_note": "heuristic signature scan; does not prove absence of every possible secret format",
}
(evidence / "p3-configuration-audit.json").write_text(
    json.dumps(result, indent=2) + "\n", encoding="utf-8"
)
if non_example_environment_files or candidate_files or not result["tracked_env_example_present"]:
    raise SystemExit("tracked environment or credential audit requires review")
PY
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
NEW_REPORT_INTERVAL=1s
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
export E2E_EVIDENCE_DIR="$evidence_dir"
aggregator_id_before_e2e=$(compose ps -q aggregator)
pvmbg_id_before_e2e=$(compose ps -q pvmbg)
[ -n "$aggregator_id_before_e2e" ]
[ -n "$pvmbg_id_before_e2e" ]
set -a
. "$env_file"
set +a

if (cd tests/e2e && GOWORK=off go test -count=1 -v ./...) >"$evidence_dir/go-test.log" 2>&1; then
  go_test_status=0
else
  go_test_status=$?
fi
cat "$evidence_dir/go-test.log"
run_finished_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
python3 - <<PY
import json
from pathlib import Path
path = Path(r"$evidence_dir/run.json")
run = json.loads(path.read_text(encoding="utf-8"))
run["go_tests_finished_at_utc"] = "$run_finished_at"
run["go_test_exit_code"] = int("$go_test_status")
path.write_text(json.dumps(run, indent=2) + "\n", encoding="utf-8")
PY
[ "$go_test_status" -eq 0 ] || exit "$go_test_status"

aggregator_id_after_e2e=$(compose ps -q aggregator)
pvmbg_id_after_e2e=$(compose ps -q pvmbg)
[ "$aggregator_id_before_e2e" = "$aggregator_id_after_e2e" ]
[ "$pvmbg_id_before_e2e" = "$pvmbg_id_after_e2e" ]
python3 - <<PY
import json
from pathlib import Path
Path(r"$evidence_dir/p1-schema-no-restart.json").write_text(json.dumps({
  "problem": "P1",
  "aggregator_container_id_before": "$aggregator_id_before_e2e",
  "aggregator_container_id_after": "$aggregator_id_after_e2e",
  "pvmbg_container_id_before": "$pvmbg_id_before_e2e",
  "pvmbg_container_id_after": "$pvmbg_id_after_e2e",
  "same_running_instances": True,
  "scope_note": "identities sampled around the live schema and API checks"
}, indent=2), encoding="utf-8")
PY

before_schema=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT count(*) FROM hazard_events WHERE source = 'PVMBG' AND NOT (attributes ? 'confidence_level')")
after_schema=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT count(*) FROM hazard_events WHERE source = 'PVMBG' AND attributes ? 'confidence_level'")
bmkg_records=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT count(*) FROM hazard_events WHERE source = 'BMKG'")
pvmbg_records=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT count(*) FROM hazard_events WHERE source = 'PVMBG'")
counts_observed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
[ "$before_schema" -gt 0 ]
[ "$after_schema" -gt 0 ]
[ "$bmkg_records" -gt 0 ]
[ "$pvmbg_records" -gt 0 ]
python3 - <<PY
import json
from pathlib import Path
Path(r"$evidence_dir/p1-canonical-counts.json").write_text(json.dumps({
  "problem": "P1",
  "observed_at_utc": "$counts_observed_at",
  "source": "live Canonical Store count query",
  "bmkg_hazard_events": int("$bmkg_records"),
  "pvmbg_hazard_events": int("$pvmbg_records")
}, indent=2), encoding="utf-8")
Path(r"$evidence_dir/p1-schema-counts.json").write_text(json.dumps({
  "problem": "P1",
  "observed_at_utc": "$counts_observed_at",
  "source": "live Canonical Store count query",
  "before_without_confidence_level": int("$before_schema"),
  "after_with_confidence_level": int("$after_schema")
}, indent=2), encoding="utf-8")
Path(r"$evidence_dir/p4-jsonb-counts.json").write_text(json.dumps({
  "problem": "P4",
  "observed_at_utc": "$counts_observed_at",
  "before_without_confidence_level": int("$before_schema"),
  "after_with_confidence_level": int("$after_schema"),
  "source": "live Canonical Store count query"
}, indent=2), encoding="utf-8")
PY

aggregator_id_before=$(compose ps -q aggregator)
client_api_id_before=$(compose ps -q client-api)
pvmbg_id_before=$(compose ps -q pvmbg)
compose up -d --no-deps --build pvmbg
aggregator_id_after=$(compose ps -q aggregator)
client_api_id_after=$(compose ps -q client-api)
pvmbg_id_after=$(compose ps -q pvmbg)
[ -n "$aggregator_id_before" ]
[ -n "$client_api_id_before" ]
[ -n "$pvmbg_id_before" ]
[ -n "$pvmbg_id_after" ]
[ "$aggregator_id_before" = "$aggregator_id_after" ]
[ "$client_api_id_before" = "$client_api_id_after" ]
field_token=$(curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"field-team-client\",\"client_secret\":\"$FIELD_TEAM_CLIENT_SECRET\"}" \
  "$CLIENT_API_URL/auth/token" | python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])')
bmkg_query_status=$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
  -H "Authorization: Bearer $field_token" \
  "$CLIENT_API_URL/hazards?source=BMKG")
[ "$bmkg_query_status" = "200" ]
python3 - <<PY
import json
from pathlib import Path
Path(r"$evidence_dir/p4-independent-rebuild.json").write_text(json.dumps({
  "problem": "P4",
  "operation": "compose up --no-deps --build pvmbg",
  "pvmbg_container_id_before": "$pvmbg_id_before",
  "pvmbg_container_id_after": "$pvmbg_id_after",
  "aggregator_container_id_before": "$aggregator_id_before",
  "aggregator_container_id_after": "$aggregator_id_after",
  "client_api_container_id_before": "$client_api_id_before",
  "client_api_container_id_after": "$client_api_id_after",
  "bmkg_query_http_status_after_rebuild": int("$bmkg_query_status")
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
auth_canonical_select=$(compose exec -T postgres psql -U aggregator -d aggregator_db -Atc "SELECT has_table_privilege('auth', 'public.hazard_events', 'SELECT')")
[ "$aggregator_has_canonical_store" = true ]
[ "$client_api_has_canonical_store" = false ]
[ "$auth_has_canonical_store" = false ]
[ "$auth_canonical_select" = "f" ]
python3 - <<PY
import json
from pathlib import Path
Path(r"$evidence_dir/p4-storage-environment.json").write_text(json.dumps({
  "problem": "P4",
  "aggregator_has_database_url_environment": "$aggregator_has_canonical_store" == "true",
  "client_api_has_database_url_environment": "$client_api_has_canonical_store" == "true",
  "auth_has_database_url_environment": "$auth_has_canonical_store" == "true",
  "auth_role_can_select_canonical_hazards": "$auth_canonical_select" == "t",
  "scope_note": "records service database URL assignment and PostgreSQL SELECT privilege for the Auth database role"
}, indent=2), encoding="utf-8")
PY
compose --profile portal config --format json | python3 -c '
import json, sys
services = json.load(sys.stdin)["services"]
result = {"problem": "P4", "canonical_database_url_assigned": {}}
for name in ("aggregator", "migrate", "client-api", "auth", "bmkg", "pvmbg", "dashboard-consumer", "notifier-consumer", "portal-consumer"):
    service = services.get(name, {})
    environment = service.get("environment", {})
    names = {item.split("=", 1)[0] for item in environment} if isinstance(environment, list) else set(environment)
    result["canonical_database_url_assigned"][name] = "AGGREGATOR_DATABASE_URL" in names
print(json.dumps(result, indent=2))
' > "$evidence_dir/p4-compose-database-ownership.json"

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
Path(r"$evidence_dir/general-correlation-id.json").write_text(json.dumps({
  "problem": "GENERAL",
  "criterion": "X-Correlation-ID forwarded and logged",
  "correlation_id": "$cid",
  "found_in_client_api_logs": "$correlation_pass" == "true"
}, indent=2), encoding="utf-8")
PY

queue_message_count() {
  compose exec -T rabbitmq rabbitmqctl list_queues name messages |
    awk -v queue="$1" '$1 == queue { print $2; found = 1 } END { if (!found) print 0 }'
}

log_field() {
  printf '%s\n' "$1" | python3 -c 'import json, sys; line=sys.stdin.readline(); payload=line.split("|", 1)[-1].strip(); print(json.loads(payload)[sys.argv[1]])' "$2"
}

notifier_queue_before_stop=$(queue_message_count notifier.hazard-events)
notifier_log_before_stop=$(compose logs --no-color notifier-consumer | grep 'event processed' | tail -n 1)
compose stop notifier-consumer
sleep 3
queued=$(queue_message_count notifier.hazard-events)
[ "${queued:-0}" -gt 0 ]
notifier_queue_after_stop=$queued
dashboard_log_after_stop=$(compose logs --no-color dashboard-consumer | grep 'event processed' | tail -n 1)
[ -n "$dashboard_log_after_stop" ]
dashboard_event_id_during_stop=$(log_field "$dashboard_log_after_stop" event_id)
dashboard_correlation_id_during_stop=$(log_field "$dashboard_log_after_stop" correlation_id)
compose start notifier-consumer

attempt=0
while :; do
  queued=$(queue_message_count notifier.hazard-events)
  if [ "${queued:-0}" -eq 0 ]; then
    break
  fi
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then
    exit 1
  fi
  sleep 1
done
notifier_queue_after_recovery=$queued
notifier_log_after_recovery=$(compose logs --no-color --tail=500 notifier-consumer | grep -F "$dashboard_event_id_during_stop" | tail -n 1)
[ -n "$notifier_log_after_recovery" ]
[ "$(log_field "$notifier_log_after_recovery" event_id)" = "$dashboard_event_id_during_stop" ]
[ "$(log_field "$notifier_log_after_recovery" correlation_id)" = "$dashboard_correlation_id_during_stop" ]
P5_DASHBOARD_AFTER_STOP="$dashboard_log_after_stop" P5_NOTIFIER_AFTER_RECOVERY="$notifier_log_after_recovery" P5_NOTIFIER_BEFORE_STOP="$notifier_log_before_stop" P5_QUEUED_EVENT_ID="$dashboard_event_id_during_stop" P5_QUEUED_CORRELATION_ID="$dashboard_correlation_id_during_stop" python3 - <<PY
import json
import os
from pathlib import Path
Path(r"$evidence_dir/p5-consumer-recovery.json").write_text(json.dumps({
  "problem": "P5",
  "notifier_queue_messages_before_stop": int("$notifier_queue_before_stop"),
  "notifier_queue_messages_after_stop": int("$notifier_queue_after_stop"),
  "notifier_queue_messages_after_recovery": int("$notifier_queue_after_recovery"),
  "dashboard_log_sample_read_after_notifier_stop": os.environ["P5_DASHBOARD_AFTER_STOP"],
  "notifier_log_for_same_event_after_recovery": os.environ["P5_NOTIFIER_AFTER_RECOVERY"],
  "notifier_log_sample_before_stop": os.environ["P5_NOTIFIER_BEFORE_STOP"],
  "queued_event_id_observed_by_dashboard": os.environ["P5_QUEUED_EVENT_ID"],
  "queued_event_correlation_id": os.environ["P5_QUEUED_CORRELATION_ID"],
  "scope_note": "one event processed by Dashboard during the stop interval was matched to Notifier after restart; queue depth was 0 after recovery"
}, indent=2), encoding="utf-8")
PY

producer_container_before_portal=$(compose ps -q aggregator)
producer_image_before_portal=$(docker inspect --format '{{.Image}}' "$producer_container_before_portal")
compose --profile portal up -d portal-consumer
sleep 2
portal_log=$(compose logs --no-color portal-consumer | grep 'event processed' | tail -n 1)
producer_container_after_portal=$(compose ps -q aggregator)
producer_image_after_portal=$(docker inspect --format '{{.Image}}' "$producer_container_after_portal")
[ "$producer_container_before_portal" = "$producer_container_after_portal" ]
[ "$producer_image_before_portal" = "$producer_image_after_portal" ]
portal_event_id=$(log_field "$portal_log" event_id)
portal_correlation_id=$(log_field "$portal_log" correlation_id)
dashboard_log=$(compose logs --no-color --tail=500 dashboard-consumer | grep -F "$portal_event_id" | tail -n 1)
notifier_log=$(compose logs --no-color --tail=500 notifier-consumer | grep -F "$portal_event_id" | tail -n 1)
assert_consumer_log_fields() {
  case "$1" in
    *'"event_id"'*'"correlation_id"'*'"redelivered"'*) ;;
    *) echo "consumer log is missing event_id, correlation_id, or redelivered" >&2; exit 1 ;;
  esac
}
assert_consumer_log_fields "$dashboard_log"
assert_consumer_log_fields "$notifier_log"
assert_consumer_log_fields "$portal_log"
[ "$(log_field "$dashboard_log" event_id)" = "$portal_event_id" ]
[ "$(log_field "$notifier_log" event_id)" = "$portal_event_id" ]
[ "$(log_field "$dashboard_log" correlation_id)" = "$portal_correlation_id" ]
[ "$(log_field "$notifier_log" correlation_id)" = "$portal_correlation_id" ]
P5_DASHBOARD_LOG="$dashboard_log" P5_NOTIFIER_LOG="$notifier_log" P5_PORTAL_LOG="$portal_log" P5_EVENT_ID="$portal_event_id" P5_CORRELATION_ID="$portal_correlation_id" python3 - <<PY
import json
import os
from pathlib import Path
Path(r"$evidence_dir/p5-consumer-observation.json").write_text(json.dumps({
  "problem": "P5",
  "event_id_semantics": "HazardEvent hazard_id",
  "sample_logs": {
    "dashboard": os.environ["P5_DASHBOARD_LOG"],
    "notifier": os.environ["P5_NOTIFIER_LOG"],
    "portal": os.environ["P5_PORTAL_LOG"]
  },
  "shared_event_id": os.environ["P5_EVENT_ID"],
  "shared_correlation_id": os.environ["P5_CORRELATION_ID"],
  "fields_present": ["event_id", "correlation_id", "redelivered"]
}, indent=2), encoding="utf-8")
PY
P5_PORTAL_LOG="$portal_log" python3 - <<PY
import json
import os
from pathlib import Path
Path(r"$evidence_dir/p5-new-consumer.json").write_text(json.dumps({
  "problem": "P5",
  "producer_container_id_before": "$producer_container_before_portal",
  "producer_container_id_after": "$producer_container_after_portal",
  "producer_image_id_before": "$producer_image_before_portal",
  "producer_image_id_after": "$producer_image_after_portal",
  "portal_log_sample": os.environ["P5_PORTAL_LOG"]
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
Path(r"$evidence_dir/p5-outbox-recovery.json").write_text(json.dumps({
  "problem": "P5",
  "pending_while_broker_down": int("$pending_down"),
  "pending_after_recovery": int("$pending"),
  "source": "live Canonical Store outbox count query"
}, indent=2), encoding="utf-8")
PY
completed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
python3 - <<PY
import json
from pathlib import Path
path = Path(r"$evidence_dir/run.json")
run = json.loads(path.read_text(encoding="utf-8"))
run["completed_at_utc"] = "$completed_at"
run["runner_exit_code"] = 0
path.write_text(json.dumps(run, indent=2) + "\n", encoding="utf-8")
PY
