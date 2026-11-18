#!/usr/bin/env bash
# M2 P3.1/P3.2/P3.4/P3.5: incompatible PVMBG schema change end to end.
#
#   1. PVMBG switches to mode "breaking" (renamed, retyped, missing fields).
#   2. The Aggregator parks every such record with its reason (default policy
#      refuses these changes); PVMBG goes back to normal afterwards.
#   3. Only the Ops token can read parked messages and their payloads.
#   4. The operator adapts the system (enables the pvmbg-breaking-v3 adapter)
#      and replays: every parked record recovers once, in arrival order.
#
# Every artifact is a direct API response. Needs a port-forward to the
# Aggregator HTTP port (AGGREGATOR_URL) and kubectl access.
set -euo pipefail

NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
AGGREGATOR_URL="${AGGREGATOR_URL:-http://localhost:18090}"
OUT="${OUT:-artifacts/m2/p3}"
BREAKING_SECONDS="${BREAKING_SECONDS:-60}"
mkdir -p "$OUT"
secret() { kubectl -n "$NAMESPACE" get secret bnpb-secrets -o "jsonpath={.data.$1}" | base64 -d; }
AUDIT_TOKEN=$(secret AUDIT_TOKEN)
OPS_TOKEN=$(secret OPS_TOKEN)
ADMIN_KEY=$(secret PVMBG_ADMIN_KEY)
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$OUT/schema-timeline.txt"; }
: >"$OUT/schema-timeline.txt"

pod() { kubectl -n "$NAMESPACE" get pod -l app=aggregator --field-selector status.phase=Running -o jsonpath='{.items[0].metadata.name}'; }
mock_mode() {
  kubectl -n "$NAMESPACE" exec "$(pod)" -c aggregator -- wget -q -O - \
    --header "X-PVMBG-Admin-Key: $ADMIN_KEY" --header "Content-Type: application/json" \
    --post-data "$1" http://pvmbg:8082/admin/schema-version
  echo
}
get() { curl -s -w '\n' --retry 3 --retry-all-errors --retry-delay 2 -m 30 -H "Authorization: Bearer $1" "$AGGREGATOR_URL$2"; }
status() { curl -s -o /dev/null -w '%{http_code}' ${1:+-H "Authorization: Bearer $1"} "$AGGREGATOR_URL$2"; }

get "$AUDIT_TOKEN" "/internal/audit/checksum?source=PVMBG" >"$OUT/checksum-before.json"
log "before: $(cat "$OUT/checksum-before.json")"

log "PVMBG mode breaking: $(mock_mode '{"mode":"breaking"}')"
sleep "$BREAKING_SECONDS"
log "PVMBG mode normal: $(mock_mode '{"mode":"normal"}')"
sleep 10

get "$OPS_TOKEN" "/internal/ops/failures?source=PVMBG&limit=1000" >"$OUT/ops-failures-parked.json"
first=$(python3 -c "import json,sys; d=json.load(open('$OUT/ops-failures-parked.json')); items=[i for i in d['items'] if i['status']=='parked']; print(items[0]['message_id'] if items else '')")
[ -n "$first" ] || { log "no parked PVMBG failure found"; exit 1; }
get "$OPS_TOKEN" "/internal/ops/failures/$first" >"$OUT/ops-failure-detail.json"
get "$AUDIT_TOKEN" "/internal/audit/unprocessed?source=PVMBG&limit=1000" >"$OUT/audit-unprocessed.json"
{
  printf '{"ops_token_list":%s,' "$(status "$OPS_TOKEN" "/internal/ops/failures?source=PVMBG")"
  printf '"ops_token_detail":%s,' "$(status "$OPS_TOKEN" "/internal/ops/failures/$first")"
  printf '"audit_token_list":%s,' "$(status "$AUDIT_TOKEN" "/internal/ops/failures?source=PVMBG")"
  printf '"audit_token_detail":%s,' "$(status "$AUDIT_TOKEN" "/internal/ops/failures/$first")"
  printf '"no_token_detail":%s,' "$(status "" "/internal/ops/failures/$first")"
  printf '"audit_token_replay":%s}\n' "$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $AUDIT_TOKEN" "$AGGREGATOR_URL/internal/ops/replay?source=PVMBG")"
} >"$OUT/ops-access.json"
log "access matrix: $(cat "$OUT/ops-access.json")"
get "$AUDIT_TOKEN" "/internal/audit/checksum?source=PVMBG" >"$OUT/checksum-while-parked.json"

log "adapt: enable PVMBG_SCHEMA_ADAPTERS=pvmbg-breaking-v3 and roll the Aggregator"
kubectl -n "$NAMESPACE" patch configmap bnpb-config --type merge -p '{"data":{"PVMBG_SCHEMA_ADAPTERS":"pvmbg-breaking-v3"}}' >/dev/null
kubectl -n "$NAMESPACE" rollout restart deployment/aggregator >/dev/null
kubectl -n "$NAMESPACE" rollout status deployment/aggregator --timeout=300s >/dev/null
# Old pods keep answering while they terminate (preStop) and do not have the
# adapter. Wait until they are gone, then make a local port-forward (pinned
# to one pod) reconnect to a new pod.
while kubectl -n "$NAMESPACE" get pods -l app=aggregator --no-headers 2>/dev/null | grep -q Terminating; do sleep 2; done
pkill -f "port-forward svc/aggregator" 2>/dev/null || true
for _ in $(seq 1 60); do
  ok=0
  for _ in 1 2 3; do curl -s -f -m 3 -o /dev/null "$AGGREGATOR_URL/health" && ok=$((ok + 1)); sleep 1; done
  [ "$ok" = 3 ] && break
done
# Replay is idempotent (a second replay recovers nothing), so a request that
# failed may be repeated; only a 200 answer is kept as evidence.
replay() {
  local body code
  for attempt in 1 2 3 4 5; do
    body=$(curl -s -m 60 -w '\n%{http_code}' -X POST -H "Authorization: Bearer $OPS_TOKEN" "$AGGREGATOR_URL/internal/ops/replay?source=PVMBG&limit=1000") || body=$'\n000'
    code=${body##*$'\n'}
    if [ "$code" = 200 ]; then printf '%s\n' "${body%$'\n'*}"; return; fi
    log "replay attempt $attempt answered $code: ${body%$'\n'*}" >&2
    sleep 3
  done
  printf '%s\n' "${body%$'\n'*}"
}
replay >"$OUT/replay.json"
log "replay: $(cat "$OUT/replay.json")"
# A second replay must recover nothing new (idempotent).
replay >"$OUT/replay-again.json"
log "replay again: $(cat "$OUT/replay-again.json")"
get "$OPS_TOKEN" "/internal/ops/failures?source=PVMBG&limit=1000" >"$OUT/ops-failures-after.json"
get "$AUDIT_TOKEN" "/internal/audit/checksum?source=PVMBG" >"$OUT/checksum-after.json"
log "after: $(cat "$OUT/checksum-after.json")"
AUDIT_TOKEN="$AUDIT_TOKEN" python3 "$(dirname "$0")/m2-p3-verify.py" --dir "$OUT" --aggregator "$AGGREGATOR_URL" >/dev/null
log "verification: $(python3 "$(dirname "$0")/m2-p3-verify.py" --brief --file "$OUT/schema-verification.json")"
