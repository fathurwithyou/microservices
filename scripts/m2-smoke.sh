#!/usr/bin/env bash
# M2 smoke checks. Requires a running deployment and AUDIT_TOKEN.
set -euo pipefail

CLIENT_API_URL="${CLIENT_API_URL:-http://localhost:8080}"
AGGREGATOR_URL="${AGGREGATOR_URL:-http://localhost:8090}"
AUDIT_TOKEN="${AUDIT_TOKEN:?set AUDIT_TOKEN}"

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
ok() { printf 'OK: %s\n' "$*"; }

curl --fail --silent --show-error "$CLIENT_API_URL/health" >/dev/null || fail "client-api health"
ok "client-api health"

curl --fail --silent --show-error "$AGGREGATOR_URL/health" >/dev/null || fail "aggregator health"
ok "aggregator health"

checksum=$(curl --fail --silent --show-error \
  -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$AGGREGATOR_URL/internal/audit/checksum?source=SATELIT")
printf '%s' "$checksum" | grep -q '"algorithm":"sha256"' || fail "audit checksum response"
ok "audit checksum"

events=$(curl --fail --silent --show-error \
  -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$AGGREGATOR_URL/internal/audit/events?source=SATELIT&limit=1")
printf '%s' "$events" | grep -q '"items"' || fail "audit events response"
ok "audit events"

printf 'M2 smoke passed\n'
