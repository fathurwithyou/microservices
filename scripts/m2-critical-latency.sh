#!/usr/bin/env bash
# Verify the critical-notification target (p95 <= TARGET_SECONDS) from the
# critical_notification_latency_seconds histogram exposed by the notifiers.
#
# The histogram has an exact bucket edge at 5 s, so "share of events within
# 5 s >= 95%" is read straight from bucket counts, with no interpolation.
# Nothing here is hand-entered: the artifact is derived from /metrics.
#
# Critical events must have flowed (any coastal observation flagged critical;
# `m2-demo.sh p5-critical` with CONFIRM=yes produces them in rounds).
# MIN_SAMPLES (default 20) is the number of samples needed for a verdict.
set -euo pipefail

# Space-separated /metrics URLs; list every critical-notifier replica.
NOTIFIER_METRICS_URLS="${NOTIFIER_METRICS_URLS:-http://localhost:8093/metrics}"
SERVICE="${SERVICE:-critical-notifier}"
# Reference point that gates pass/fail (GATING_REFERENCE, or the older name
# LATENCY_FROM). All three are always reported:
#   received_at  wall clock when the ingestor finished reading the sensor
#                response that carried the event, to notifier delivery. Both
#                ends are our own clocks, so this is the measured part of the
#                path. The sensor exposes an observation at most one poll
#                interval before the ingestor fetches it, so the end-to-end
#                bound is this figure plus POLL_INTERVAL_SECONDS.
#   occurred_at  sensor observation time to delivery. Informational: Sensor
#                Pantai runs on a virtual, accelerated clock, so this mixes two
#                clocks and is not a latency.
#   ingested_at  Aggregator storage time (1 s resolution) to delivery; isolates
#                outbox + broker + consumer.
LATENCY_FROM="${GATING_REFERENCE:-${LATENCY_FROM:-received_at}}"
# The ingestor's COASTAL_INGESTOR_POLL_INTERVAL (1s in the cluster manifest).
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-1}"
TARGET_SECONDS="${TARGET_SECONDS:-5}"
REQUIRED_RATIO="${REQUIRED_RATIO:-0.95}"
MIN_SAMPLES="${MIN_SAMPLES:-20}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"
OUT_DIR="${OUT_DIR:-artifacts/m2}"

fail() { printf 'FAIL: %s\n' "$*" >&2; }

# summarize <from> reads Prometheus text on stdin and prints:
#   count within_target ratio p95_upper_bound
# ratio and p95_upper_bound are "null" when there are no samples or the
# 95th percentile falls beyond the largest finite bucket.
summarize() {
  awk -v from="$1" -v service="$SERVICE" -v target="$TARGET_SECONDS" '
    function label(line, name,   pattern, value) {
      pattern = "[{,]" name "=\"[^\"]*\""
      if (match(line, pattern)) {
        return substr(line, RSTART + length(name) + 3, RLENGTH - length(name) - 4)
      }
      return ""
    }
    /^critical_notification_latency_seconds_(bucket|count)\{/ {
      if (label($0, "from") != from) next
      if (service != "" && label($0, "service") != service) next
      value = $NF + 0
      if ($0 ~ /^critical_notification_latency_seconds_count/) { count += value; next }
      le = label($0, "le")
      if (le == "+Inf") next
      bucket[le + 0] += value
    }
    END {
      within = 0
      for (edge in bucket) if (edge + 0 == target + 0) within = bucket[edge]
      ratio = "null"; p95 = "null"
      if (count > 0) {
        ratio = sprintf("%.6f", within / count)
        for (edge in bucket) {
          if (bucket[edge] >= 0.95 * count && (p95 == "null" || edge + 0 < p95 + 0)) p95 = edge + 0
        }
      }
      printf "%d %d %s %s\n", count, within, ratio, p95
    }'
}

fetch_all() {
  local url
  for url in $NOTIFIER_METRICS_URLS; do
    # Explicit return: errexit is suspended inside `$(...) || ...`, so without
    # it an unreachable notifier would be ignored and the loop would just
    # wait out WAIT_SECONDS with zero samples.
    curl --fail --silent --show-error --max-time 5 "$url" || return 1
    printf '\n'
  done
}

deadline=$((SECONDS + WAIT_SECONDS))
while :; do
  metrics=$(fetch_all) || { fail "could not read notifier metrics from: $NOTIFIER_METRICS_URLS"; exit 2; }
  read -r count within ratio p95 <<<"$(printf '%s\n' "$metrics" | summarize "$LATENCY_FROM")"
  if (( count >= MIN_SAMPLES )) || (( SECONDS >= deadline )); then
    break
  fi
  printf 'waiting for critical events: %s/%s samples\n' "$count" "$MIN_SAMPLES"
  sleep 2
done

summary_json() {
  local from="$1" c w r p
  read -r c w r p <<<"$(printf '%s\n' "$metrics" | summarize "$from")"
  printf '{"from":"%s","samples":%s,"within_target":%s,"ratio_within_target":%s,"p95_upper_bound_seconds":%s}' \
    "$from" "$c" "$w" "$r" "$p"
}

mkdir -p "$OUT_DIR/p5"
artifact="$OUT_DIR/p5/critical-latency.json"
pass=false
if (( count >= MIN_SAMPLES )) && awk -v r="$ratio" -v need="$REQUIRED_RATIO" 'BEGIN { exit !(r != "null" && r + 0 >= need + 0) }'; then
  pass=true
fi

# The end-to-end bound adds the one thing the histogram cannot see: how long
# the observation sat in the sensor before a poll picked it up.
bound=null
if [[ "$LATENCY_FROM" == received_at && "$p95" != null ]]; then
  bound=$(awk -v p="$p95" -v poll="$POLL_INTERVAL_SECONDS" 'BEGIN { printf "%g", p + poll }')
fi

printf '{"generated_at":"%s","service":"%s","target_seconds":%s,"required_ratio":%s,"min_samples":%s,"gating_reference":"%s","pass":%s,"poll_interval_seconds":%s,"end_to_end_p95_upper_bound_seconds":%s,"references":[%s,%s,%s]}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$SERVICE" "$TARGET_SECONDS" "$REQUIRED_RATIO" "$MIN_SAMPLES" "$LATENCY_FROM" "$pass" \
  "$POLL_INTERVAL_SECONDS" "$bound" \
  "$(summary_json received_at)" "$(summary_json occurred_at)" "$(summary_json ingested_at)" >"$artifact"

printf 'Wrote %s\n' "$artifact"
printf 'gating reference %s: %s samples, %s within %ss (ratio %s, p95 <= %ss)\n' \
  "$LATENCY_FROM" "$count" "$within" "$TARGET_SECONDS" "$ratio" "$p95"
for other in received_at occurred_at ingested_at; do
  [[ "$other" == "$LATENCY_FROM" ]] && continue
  read -r oc ow orat op <<<"$(printf '%s\n' "$metrics" | summarize "$other")"
  printf 'informational %s: %s samples, %s within %ss (ratio %s, p95 <= %ss)\n' "$other" "$oc" "$ow" "$TARGET_SECONDS" "$orat" "$op"
done
if [[ "$LATENCY_FROM" == received_at ]]; then
  printf 'End-to-end bound: the histogram starts when the ingestor has read the sensor response. The sensor\n'
  printf 'exposes an observation at most one poll interval (%ss) before that while the ingestor is polling\n' "$POLL_INTERVAL_SECONDS"
  printf '(breaker closed, caught up), so sensor-exposure-to-notifier p95 <= %ss + %ss = %ss.\n' "$p95" "$POLL_INTERVAL_SECONDS" "$bound"
  if [[ "$bound" != null ]] && awk -v b="$bound" -v t="$TARGET_SECONDS" 'BEGIN { exit !(b + 0 > t + 0) }'; then
    printf 'NOTE: that bound (%ss) is above the %ss target although the measured part passes; the histogram bucket edges are 0.1 0.25 0.5 1 2 3 4 5.\n' "$bound" "$TARGET_SECONDS"
  fi
fi

if (( count < MIN_SAMPLES )); then
  fail "only $count samples after ${WAIT_SECONDS}s; need $MIN_SAMPLES critical events"
  exit 3
fi
if [[ "$pass" != true ]]; then
  fail "critical-notification latency target not met"
  exit 1
fi
printf 'PASS: p95 critical-notification latency <= %ss\n' "$TARGET_SECONDS"
