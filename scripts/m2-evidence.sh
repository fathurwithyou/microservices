#!/usr/bin/env bash
# Generate M2 evidence artifacts from a running deployment.
# This script never fabricates numbers: every file is a direct API response.
set -euo pipefail

AGGREGATOR_URL="${AGGREGATOR_URL:-http://localhost:8090}"
CLIENT_API_URL="${CLIENT_API_URL:-http://localhost:8080}"
OUT_DIR="${OUT_DIR:-artifacts/m2}"
AUDIT_TOKEN="${AUDIT_TOKEN:?set AUDIT_TOKEN}"

mkdir -p "$OUT_DIR"/{p1,p2,p3,p4,p5}
stamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)

write() {
  local path="$1"
  shift
  "$@" >"$path"
}

audit() {
  curl --fail --silent --show-error -H "Authorization: Bearer $AUDIT_TOKEN" "$@"
}

write "$OUT_DIR/p1/audit-checksum.json" audit \
  "$AGGREGATOR_URL/internal/audit/checksum?source=SATELIT"

write "$OUT_DIR/p1/audit-events.json" audit \
  "$AGGREGATOR_URL/internal/audit/events?source=SATELIT&limit=100"

curl --fail --silent --show-error "$CLIENT_API_URL/health" \
  >"$OUT_DIR/p2/client-api-health.json"
curl --fail --silent --show-error "$AGGREGATOR_URL/health" \
  >"$OUT_DIR/p2/aggregator-health.json"

printf '{"generated_at":"%s","source":"live API","note":"fill scenario-specific files with scripts/test-e2e.sh and load tests"}\n' "$stamp" \
  >"$OUT_DIR/README.json"

printf 'Wrote M2 evidence to %s\n' "$OUT_DIR"
