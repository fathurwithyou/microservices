#!/usr/bin/env bash
# M2 P2 load scenario: 10k VUs, scale 3 -> 5 -> 2, then rolling update.
set -euo pipefail

NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
DEPLOYMENT="${DEPLOYMENT:-aggregator}"
BASE_URL="${BASE_URL:-http://localhost:8080}"
K6_SUMMARY_PATH="${K6_SUMMARY_PATH:-artifacts/m2/p2/load.json}"
: "${FIELD_TEAM_CLIENT_SECRET:?set FIELD_TEAM_CLIENT_SECRET}"

mkdir -p "$(dirname "$K6_SUMMARY_PATH")"

scale() {
  kubectl -n "$NAMESPACE" scale "deployment/$DEPLOYMENT" --replicas="$1"
  printf 'scaled %s to %s\n' "$DEPLOYMENT" "$1"
}

scale 3
k6 run \
  -e BASE_URL="$BASE_URL" \
  -e FIELD_TEAM_CLIENT_SECRET="$FIELD_TEAM_CLIENT_SECRET" \
  -e K6_SUMMARY_PATH="$K6_SUMMARY_PATH" \
  tests/load/k6/m2-sustained.js &
k6_pid=$!

sleep 40
scale 5
sleep 40
scale 2
sleep 20
kubectl -n "$NAMESPACE" rollout restart "deployment/$DEPLOYMENT"
kubectl -n "$NAMESPACE" rollout status "deployment/$DEPLOYMENT" --timeout=120s

wait "$k6_pid"
printf 'load evidence: %s\n' "$K6_SUMMARY_PATH"
