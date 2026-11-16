#!/usr/bin/env bash
# M2 P5.3/P5.4: Sensor Pantai outage and automatic recovery.
#
# Scales the vendor sensor (image untouched) to zero for OUTAGE seconds, then
# back to one. Every SAMPLE seconds it records, as JSON lines:
#   - requests the coastal-ingestor leader sent to the sensor, by outcome, and
#     its circuit breaker state (the request rate must not climb);
#   - GET /coastal/latest status and latency (answered fast with the last
#     stored reading or an explicit unavailable status, never a timeout);
#   - GET /hazards?source=BMKG status and latency (other sources unaffected);
#   - accepted coastal readings (recovery without intervention).
set -euo pipefail

NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
BASE_URL="${BASE_URL:?set BASE_URL to a Client API entry point}"
OUTAGE="${OUTAGE:-90}"
AFTER="${AFTER:-90}"
SAMPLE="${SAMPLE:-10}"
OUT="${OUT:-artifacts/m2/p5/sensor-outage.jsonl}"
mkdir -p "$(dirname "$OUT")"
: >"$OUT"

secret=$(kubectl -n "$NAMESPACE" get secret bnpb-secrets -o jsonpath='{.data.FIELD_TEAM_CLIENT_SECRET}' | base64 -d)
# Access tokens live 60 s; take a fresh one for every sample.
new_token() {
  curl -s -X POST "$BASE_URL/auth/token" -H 'Content-Type: application/json' \
    -d "{\"client_id\":\"field-team-client\",\"client_secret\":\"$secret\"}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_token"])'
}

metrics() {
  for pod in $(kubectl -n "$NAMESPACE" get pod -l app=coastal-ingestor -o name); do
    kubectl -n "$NAMESPACE" exec "$pod" -- wget -q -O - http://localhost:8087/metrics 2>/dev/null |
      grep -E '^coastal_(sensor_requests_total|circuit_state|readings_accepted_total)' | sed "s|^|${pod#pod/} |"
  done
}
probe() {
  curl -s -o /dev/null -m 10 -w '%{http_code} %{time_total}' -H "Authorization: Bearer $token" "$BASE_URL$1" || echo "0 10"
}
sample() {
  local phase="$1" coastal hazards
  token=$(new_token)
  coastal=$(probe /coastal/latest)
  hazards=$(probe "/hazards?source=BMKG&page_size=10")
  metrics | python3 -c '
import json, sys, time
phase, coastal, hazards = sys.argv[1], sys.argv[2].split(), sys.argv[3].split()
pods = {}
for line in sys.stdin:
    pod, name, value = line.split()[0], line.split()[1], float(line.split()[2])
    pods.setdefault(pod, {})[name] = value
print(json.dumps({"t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "phase": phase,
  "coastal_latest": {"status": int(coastal[0]), "seconds": float(coastal[1])},
  "hazards_bmkg": {"status": int(hazards[0]), "seconds": float(hazards[1])},
  "coastal_ingestor": pods}))' "$phase" "$coastal" "$hazards" | tee -a "$OUT"
}

for _ in 1 2 3; do sample before; sleep "$SAMPLE"; done
kubectl -n "$NAMESPACE" scale deployment/sensor-pantai --replicas=0 >/dev/null
echo "{\"event\":\"sensor scaled to 0\",\"t\":\"$(date -u +%FT%TZ)\"}" | tee -a "$OUT"
end=$(( $(date +%s) + OUTAGE )); while [ "$(date +%s)" -lt "$end" ]; do sample outage; sleep "$SAMPLE"; done
kubectl -n "$NAMESPACE" scale deployment/sensor-pantai --replicas=1 >/dev/null
echo "{\"event\":\"sensor scaled to 1\",\"t\":\"$(date -u +%FT%TZ)\"}" | tee -a "$OUT"
end=$(( $(date +%s) + AFTER )); while [ "$(date +%s)" -lt "$end" ]; do sample recovery; sleep "$SAMPLE"; done
