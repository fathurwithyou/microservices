#!/usr/bin/env bash
# M2 P2 load scenario: ramp to 10k VUs, hold >= 120 s, scale the Aggregator
# 3 -> 5 -> 2 during the hold, then replace its version (rolling update to
# NEW_IMAGE) while the load is still running.
#
#   BASE_URLS   comma-separated Client API entry points (one NodePort per node)
#   NEW_IMAGE   image for the version change, e.g. bnpb-m1/aggregator:m2.8
set -euo pipefail

NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
DEPLOYMENT="${DEPLOYMENT:-aggregator}"
BASE_URL="${BASE_URL:-http://localhost:8080}"
BASE_URLS="${BASE_URLS:-$BASE_URL}"
VUS="${VUS:-10000}"
RAMP="${RAMP:-30s}"
HOLD="${HOLD:-150s}"
K6_SUMMARY_PATH="${K6_SUMMARY_PATH:-artifacts/m2/p2/load.json}"
TIMELINE="${TIMELINE:-artifacts/m2/p2/load-timeline.txt}"
: "${FIELD_TEAM_CLIENT_SECRET:?set FIELD_TEAM_CLIENT_SECRET}"
: "${NEW_IMAGE:?set NEW_IMAGE to the Aggregator image of the new version}"

mkdir -p "$(dirname "$K6_SUMMARY_PATH")"
: >"$TIMELINE"
mark() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$TIMELINE"; }

# Under 10k VUs the API server can be slow to answer; retry each kubectl
# call instead of aborting the scenario half way.
kk() {
  local attempt
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    kubectl --request-timeout=20s -n "$NAMESPACE" "$@" && return 0
    # stderr: kk's stdout may be captured as a command result.
    mark "kubectl $1 failed (attempt $attempt), retrying" >&2
    sleep 5
  done
  return 1
}
k6_pid=""
trap '[ -n "$k6_pid" ] && kill "$k6_pid" 2>/dev/null || true' EXIT

scale() {
  kk scale "deployment/$DEPLOYMENT" --replicas="$1" >/dev/null
  mark "scaled $DEPLOYMENT to $1"
}

scale 3
kk rollout status "deployment/$DEPLOYMENT" --timeout=180s >/dev/null
mark "k6 start: ramp $RAMP to $VUS VUs, hold $HOLD"
# Secrets reach k6 through the environment (k6 exposes it as __ENV), not
# through -e, so they never appear in the process list.
export FIELD_TEAM_CLIENT_SECRET
k6 run --quiet \
  -e BASE_URL="$BASE_URL" -e BASE_URLS="$BASE_URLS" \
  -e VUS="$VUS" -e RAMP="$RAMP" -e DURATION="$HOLD" -e LABEL="p2-scaling-and-rolling-update" \
  -e K6_SUMMARY_PATH="$K6_SUMMARY_PATH" \
  tests/load/k6/m2-sustained.js >/dev/null &
k6_pid=$!

sleep 45   # ramp done, 15 s into the hold
scale 5
sleep 35
scale 2
sleep 25
container_images=$(kk get "deployment/$DEPLOYMENT" -o jsonpath='{range .spec.template.spec.initContainers[*]}{.name}{"\n"}{end}{range .spec.template.spec.containers[*]}{.name}{"\n"}{end}' | sed "s|\$|=$NEW_IMAGE|" | tr '\n' ' ')
mark "version change to $NEW_IMAGE"
# shellcheck disable=SC2086
kk set image "deployment/$DEPLOYMENT" $container_images >/dev/null
kk rollout status "deployment/$DEPLOYMENT" --timeout=300s >/dev/null
mark "version change complete"

wait "$k6_pid" || true
k6_pid=""
mark "k6 finished"
printf 'load evidence: %s\n' "$K6_SUMMARY_PATH"
