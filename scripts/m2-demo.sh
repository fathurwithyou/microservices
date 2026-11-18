#!/usr/bin/env bash
# M2 evidence runner: one command per scenario, every result in one run folder.
#
#   scripts/m2-demo.sh <stage> [<stage> ...]
#
# Every stage writes into $RUN (default: the newest artifacts/m2/runs/<id>,
# created by `prep`). `summary` turns the folder into SUMMARY.md/summary.json
# with PASS / FAIL / NOT RUN per criterion and the evidence file for each.
#
# Safe stages (no fault injection; minutes each):
#   prep                 new run folder, cluster/state snapshot, run.json
#   dataset FILE         normalise the assistants' catalog (any CSV/JSON shape)
#                        -> $RUN/p1/catalog.ndjson (+ report, reference checksum)
#                        EXPECTED_CHECKSUM=<sha256> compares with theirs
#   p1                   import + per-node load, distribution, one-region burst,
#                        alternatives, cross-store read, analytics vs audit, snapshot
#   p2-spread            60 requests through every NodePort -> per-pod gRPC counts
#   p2-failover          kill the ingestion leader (SIGKILL), gap + duplicate check
#                        FAILOVER_KILL=pod force-deletes the pod instead, so
#                        another instance (not the restarted container) takes over
#   p3                   PVMBG breaking schema -> park -> adapter -> replay,
#                        malformed responses, field reports with pod kills
#   p4                   satellite feed status, metrics, SATELIT checksum
#                        (P4_WATCH=<seconds> samples the live stream)
#   p4-sim               (CONFIRM) P4 rehearsal against the in-cluster stand-in
#                        feed: 60x burst, cut connections, leader killed,
#                        catch-up, lateness, result vs independent reference
#   p4-feed-sim|-real    (CONFIRM) point the ingestor at the stand-in / the
#                        assistants' feed
#   p5-quarantine        quarantine/critical counters per reason
#   p5-outage            sensor scaled to 0 and back (breaker, isolation, recovery)
#   p5-shedding          100 Field Team VU vs 2000 Media VU
#   p5-critical          critical-notification p95 from notifier histograms. With
#                        CONFIRM=yes it first produces samples: CRITICAL_ROUNDS
#                        (7) x [purge SENSOR_PANTAI events, restart the sensor,
#                        wait CRITICAL_ROUND_WAIT (200 s)] -> critical-rounds.json
#                        (ingestor vs notifier counts per round = "0 lost").
#                        PANTAI_EXAM=yes PANTAI_KEY_FILE=<path> runs it on the
#                        exam profile and switches back to practice afterwards
#   p5-gameday           Tester Agent practice scenarios, hands-off window, SLO,
#                        post-mortem skeleton (GAMEDAY_SCENARIOS, GAMEDAY_WINDOW)
#   summary              SUMMARY.md + summary.json
#   all                  prep p1 p2-spread p2-failover p3 p4 p5-quarantine
#                        p5-outage p5-shedding p5-critical summary
#
# Heavy or destructive stages (need CONFIRM=yes):
#   p1-node-loss         power off a worker VM, wipe its CockroachDB disk, heal,
#                        rejoin as a new node, rebalance (~20 min). VICTIM=node-x
#   p2-skew              leader node clock +45 s then -30 s, NTP off, restored
#   p2-freeze            suspend the leader VM ~60 s, resume
#   p2-load              10k VU, scale 3->5->2, version change (NEW_IMAGE=...)
#   p2-throughput        10k VU against 1 then 3 Aggregator instances
#
# Utilities:
#   clocks               compare every VM clock with this host (CLOCK_SKEW_FIX=yes
#                        restarts timesyncd on drifted nodes); also runs before
#                        every other stage
#   restore              undo test-only changes (NTP on, adapter off, sensor up,
#                        replicas and env back to the overlay, probe pod gone)
#   set-audit-token      store $NEW_AUDIT_TOKEN (the assistants' token) in
#                        bnpb-secrets and roll the Aggregator
#
# Environment: KUBECONFIG (default ~/.kube/bnpb-m2.yaml), NAMESPACE, RUN.
# Secrets are read from bnpb-secrets and never written to the run folder.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$ROOT"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/bnpb-m2.yaml}"
NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
RUNS="artifacts/m2/runs"
AGG="http://localhost:18090"
BG_PIDS=()

# ------------------------------------------------------------------ helpers

log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$RUN/timeline.txt" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
k() { kubectl -n "$NS" "$@"; }
secret() { k get secret bnpb-secrets -o "jsonpath={.data.$1}" | base64 -d; }
need_confirm() { [ "${CONFIRM:-}" = yes ] || die "$1 changes the cluster; rerun with CONFIRM=yes"; }

# check_clocks compares every VM clock with this host. CockroachDB refuses a
# node whose offset exceeds 500 ms, and a VM that was paused drifts by minutes
# while timesyncd still reports "synchronized" until its next poll (17 Nov,
# 18 Nov). A skew above CLOCK_SKEW_MAX seconds (default 3) aborts the stage;
# CLOCK_SKEW_FIX=yes restarts timesyncd on the drifted nodes first.
check_clocks() {
  command -v multipass >/dev/null || return 0
  # Host time is taken right before the exec: the remote stamp lands at most
  # a second or two later, so a real skew still stands out. (mp is not used
  # here: inside $(...) its sleep guard would hold the pipe open for 15 s.)
  local max="${CLOCK_SKEW_MAX:-3}" n remote host skew bad=""
  for n in $(kubectl get nodes -o name 2>/dev/null | sed 's|node/||'); do
    host=$(date -u +%s)
    remote=$(multipass exec "$n" -- date -u +%s 2>/dev/null | tr -d '[:space:]') || continue
    [ -n "$remote" ] || continue
    skew=$((remote - host))
    [ "${skew#-}" -le "$max" ] || bad="$bad $n(${skew}s)"
  done
  [ -n "$bad" ] || return 0
  if [ "${CLOCK_SKEW_FIX:-}" = yes ]; then
    log "clock skew on$bad: restarting systemd-timesyncd"
    for n in $bad; do mp 30 "${n%%(*}" -- sudo systemctl restart systemd-timesyncd >/dev/null 2>&1 || true; done
    sleep 3; CLOCK_SKEW_FIX= check_clocks
  else
    die "clock skew above ${max}s on$bad; run 'CLOCK_SKEW_FIX=yes $0 clocks' (or stage restore), then retry"
  fi
}

stage_clocks() { check_clocks; echo "all node clocks within ${CLOCK_SKEW_MAX:-3}s of this host" >&2; }

cleanup() {
  for pid in "${BG_PIDS[@]:-}"; do [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; done
  pkill -f "port-forward svc/aggregator 18090:8090" 2>/dev/null || true
  pkill -f "port-forward pod/critical-notifier" 2>/dev/null || true
}
trap cleanup EXIT

use_run() {
  if [ -z "${RUN:-}" ]; then
    RUN=$(ls -1d "$RUNS"/m2-* 2>/dev/null | sort | tail -1 || true)
    [ -n "$RUN" ] || die "no run folder yet; start with: scripts/m2-demo.sh prep"
  fi
  mkdir -p "$RUN"/{p1,p2,p3,p4,p5,cluster}
}

load_secrets() {
  export FIELD_TEAM_CLIENT_SECRET MEDIA_CLIENT_SECRET AUDIT_TOKEN OPS_TOKEN
  FIELD_TEAM_CLIENT_SECRET=$(secret FIELD_TEAM_CLIENT_SECRET)
  MEDIA_CLIENT_SECRET=$(secret MEDIA_CLIENT_SECRET)
  AUDIT_TOKEN=$(secret AUDIT_TOKEN)
  OPS_TOKEN=$(secret OPS_TOKEN)
}

# One IPv4 address per node. A node can report several InternalIPs (node-d
# gained an IPv6 one after a restart); an IPv6 literal without brackets is not
# a usable URL host, and a second address would count the node twice.
node_ips() { kubectl get nodes -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}' |
  awk '{for (i = 1; i <= NF; i++) if ($i ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/) { print $i; break }}'; }
base_urls() { node_ips | sed 's|.*|http://&:30080|' | paste -sd, -; }
# Entry points for the 10k-VU runs. Default: every node (the load spreads over
# all NodePorts; m2-load.sh retries kubectl while the API server is slow).
# LOAD_ENTRY=workers leaves the control-plane node out, which keeps kubectl
# responsive but concentrates the load on fewer nodes.
worker_urls() {
  if [ "${LOAD_ENTRY:-all}" != workers ]; then base_urls; return; fi
  kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
    -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}' | sed 's|.*|http://&:30080|' | paste -sd, -
}
first_url() { base_urls | cut -d, -f1; }

# Aggregator port-forward that reconnects, plus a watchdog: a forward is pinned
# to one pod and hangs silently when that pod goes away.
forward() {
  if curl -s -m 3 -o /dev/null "$AGG/health"; then return; fi
  # set +e: these loops inherit errexit from the stage subshell; one failed
  # kubectl/curl/pkill must not end them.
  ( set +e; while :; do kubectl -n "$NS" port-forward svc/aggregator 18090:8090 >/dev/null 2>&1; sleep 1; done ) &
  BG_PIDS+=($!)
  ( set +e; while :; do sleep 5; curl -s -m 3 -o /dev/null "$AGG/health" || pkill -f "port-forward svc/aggregator 18090:8090"; done ) &
  BG_PIDS+=($!)
  for _ in $(seq 1 30); do curl -s -m 3 -o /dev/null "$AGG/health" && return; sleep 1; done
  die "aggregator port-forward did not come up"
}

audit_get() { curl -s -m 30 -H "Authorization: Bearer $AUDIT_TOKEN" "$AGG$1"; }

# Collect Aggregator and upstream-mock logs into DIR (for m2-p2-analyze.py).
collector() {
  local dir="$1"; mkdir -p "$dir"
  (
    set +e
    while :; do
      for p in $(kubectl -n "$NS" get pod -l app=aggregator -o name 2>/dev/null); do
        n=${p#pod/}
        kubectl -n "$NS" logs "$p" -c aggregator --since=25s >>"$dir/aggregator-$n.log" 2>/dev/null || true
        kubectl -n "$NS" logs "$p" -c aggregator --previous --since=25s >>"$dir/aggregator-$n.previous.log" 2>/dev/null || true
      done
      kubectl -n "$NS" logs deploy/bmkg --since=25s >>"$dir/bmkg.log" 2>/dev/null || true
      kubectl -n "$NS" logs deploy/pvmbg --since=25s >>"$dir/pvmbg.log" 2>/dev/null || true
      sleep 10
    done
  ) &
  BG_PIDS+=($!)
}

# multipass exec can hang (after a clock jump, after crictl stop); bound it.
mp() {
  local seconds="$1"; shift
  multipass exec "$@" &
  local pid=$!
  ( sleep "$seconds"; kill "$pid" 2>/dev/null ) &
  local guard=$!
  local rc=0
  wait "$pid" 2>/dev/null || rc=$?
  kill "$guard" 2>/dev/null || true
  return "$rc"
}

# Current ingestion leader: holder of the newest "poll batch persisted" line.
leader() {
  k logs -l app=aggregator -c aggregator --since=60s --tail=-1 2>/dev/null |
    grep -E '"(poll batch persisted|leader acquired)"' |
    python3 -c '
import json, sys
rows = []
for line in sys.stdin:
    try:
        rows.append(json.loads(line))
    except ValueError:
        pass
rows = [r for r in rows if r.get("holder")]
rows.sort(key=lambda r: r.get("time", ""))
print(rows[-1]["holder"] if rows else "")'
}
node_of() { k get pod "$1" -o jsonpath='{.spec.nodeName}'; }
# Column positions in `kubectl get -o wide` shift when RESTARTS reads
# "22 (148m ago)", so node lookups use jsonpath / field selectors only.
pod_nodes() { k get pod -o jsonpath='{range .items[*]}{.metadata.name} {.spec.nodeName}{"\n"}{end}'; }
# Nodes whose loss takes a whole service down: single-instance stores, and
# auth when it still runs one replica (every token validation goes through it).
singleton_nodes() {
  local auth_replicas
  auth_replicas=$(k get deploy auth -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 1)
  pod_nodes | awk -v auth="$auth_replicas" '$1 ~ /^(postgres-|rabbitmq-)/ || (auth < 2 && $1 ~ /^auth-/) {print $2}' | sort -u | paste -sd' ' -
}
# Move ingestion leadership off unwanted nodes (control plane, and with
# AVOID_SINGLETONS=1 the nodes holding postgres/rabbitmq) by restarting the
# leader pod; prints the final leader pod.
leader_off() {
  local pod node avoid tries=0
  while :; do
    pod=$(leader); node=$(node_of "$pod")
    avoid=""
    is_control_plane "$node" && avoid="control plane"
    if [ "${AVOID_SINGLETONS:-0}" = 1 ]; then case " $(singleton_nodes) " in *" $node "*) avoid="holds postgres/rabbitmq" ;; esac; fi
    if [ -z "$avoid" ] || [ $tries -ge 5 ]; then echo "$pod"; return; fi
    log "leader $pod on $node ($avoid): restarting it so another instance leads"
    k delete pod "$pod" >/dev/null
    tries=$((tries + 1))
    sleep 45
  done
}
is_control_plane() { kubectl get node "$1" -o jsonpath='{.metadata.labels}' | grep -q 'node-role.kubernetes.io/control-plane'; }
node_offset() { # seconds node clock - host clock, measured through a pod on that node
  local pod
  pod=$(k get pod -l app=aggregator --field-selector "spec.nodeName=$1,status.phase=Running" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  [ -n "$pod" ] || { echo "?"; return; }
  echo $(( $(k exec "$pod" -c aggregator -- date -u +%s 2>/dev/null || date -u +%s) - $(date -u +%s) ))
}

stage_done() { log "stage $1 finished (exit $2)"; python3 - "$RUN/run.json" "$1" "$2" <<'PY'
import json, sys, datetime
path, stage, code = sys.argv[1], sys.argv[2], int(sys.argv[3])
try:
    data = json.load(open(path))
except (OSError, ValueError):
    data = {}
data.setdefault("stages", []).append({"stage": stage, "exit": code,
    "finished_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")})
json.dump(data, open(path, "w"), indent=2)
PY
}

# ------------------------------------------------------------------ stages

stage_prep() {
  RUN="$RUNS/m2-$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$RUN"/{p1,p2,p3,p4,p5,cluster}
  log "run folder $RUN"
  kubectl get nodes -o wide >"$RUN/cluster/nodes.txt"
  k get pods -o wide >"$RUN/cluster/pods.txt"
  k get deploy,statefulset -o wide >"$RUN/cluster/workloads.txt"
  python3 scripts/m2-store-stats.py --label prep --out "$RUN/cluster/store.json" >/dev/null 2>&1 || log "store stats unavailable"
  load_secrets; forward
  audit_get /internal/audit/distribution >"$RUN/cluster/distribution.json"
  for s in BMKG PVMBG SATELIT SENSOR_PANTAI; do audit_get "/internal/audit/checksum?source=$s"; echo; done >"$RUN/cluster/checksums.jsonl"
  local not_ready
  not_ready=$(kubectl get nodes --no-headers | awk '$2!="Ready"{print $1}' | paste -sd' ' -)
  python3 - "$RUN" "$not_ready" <<'PY'
import json, subprocess, sys, datetime
run, not_ready = sys.argv[1], sys.argv[2]
git = lambda *a: subprocess.run(["git", *a], capture_output=True, text=True).stdout.strip()
images = subprocess.run(["kubectl", "-n", "sistem-informasi-kebencanaan-bnpb", "get", "deploy,statefulset", "-o",
    "jsonpath={range .items[*]}{.metadata.name}={.spec.template.spec.containers[0].image}{\"\\n\"}{end}"],
    capture_output=True, text=True).stdout.split()
json.dump({"run_id": run.rsplit("/", 1)[-1], "started_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "git_commit": git("rev-parse", "HEAD"), "git_branch": git("rev-parse", "--abbrev-ref", "HEAD"),
           "git_dirty": bool(git("status", "--porcelain", "--untracked-files=no")),
           "images": dict(i.split("=", 1) for i in images if "=" in i), "nodes_not_ready": not_ready.split(),
           "evidence_scope": "observations from the cluster; no credentials or tokens", "stages": []},
          open(f"{run}/run.json", "w"), indent=2)
PY
  [ -z "$not_ready" ] || log "WARNING nodes not Ready: $not_ready"
  python3 -c "
import json; d=json.load(open('$RUN/cluster/distribution.json'))
print('store nodes:', ', '.join(f\"{n['node']}#{n['node_id']} live={n['live']} replicas={n['replicas']}\" for n in d['nodes']), '| partial', d['partial'])" | tee -a "$RUN/timeline.txt"
}

stage_dataset() {
  local file="${1:-${DATASET:-}}"
  [ -n "$file" ] && [ -f "$file" ] || die "usage: scripts/m2-demo.sh dataset <file>  (CSV, JSON, NDJSON, GeoJSON, .gz)"
  python3 scripts/m2-catalog-normalize.py "$file" --inspect
  # shellcheck disable=SC2086
  python3 scripts/m2-catalog-normalize.py "$file" --out "$RUN/p1/catalog.ndjson" --report "$RUN/p1/catalog-report.json" \
    ${ASSUME_TZ:+--assume-tz "$ASSUME_TZ"} ${EXPECTED_CHECKSUM:+--expected-checksum "$EXPECTED_CHECKSUM"} ${NORMALIZE_ARGS:-}
  python3 - "$RUN/run.json" "$file" <<'PY'
import json, sys
d = json.load(open(sys.argv[1])); d["catalog"] = sys.argv[2]; json.dump(d, open(sys.argv[1], "w"), indent=2)
PY
}

catalog_file() {
  if [ -f "$RUN/p1/catalog.ndjson" ]; then echo "$RUN/p1/catalog.ndjson"; return; fi
  if [ -n "${CATALOG:-}" ]; then
    python3 scripts/m2-catalog-normalize.py "$CATALOG" --out "$RUN/p1/catalog.ndjson" --report "$RUN/p1/catalog-report.json" >/dev/null
    echo "$RUN/p1/catalog.ndjson"; return
  fi
  die "no catalog: run 'scripts/m2-demo.sh dataset <assistants-file>' (or CATALOG=output/catalog/earthquake-catalog-100k.ndjson)"
}

# import FILE LABEL SAMPLES WINDOW -> $RUN/p1/<label>-import-log.json, load-during-<label>[-peak].json
import_with_load() {
  local file="$1" label="$2" samples="$3" window="$4" pod
  pod=$(k get pod -l app=aggregator --field-selector status.phase=Running -o jsonpath='{.items[0].metadata.name}')
  k cp "$file" "$pod:/tmp/$label.ndjson" -c aggregator
  python3 scripts/m2-store-stats.py --samples "$samples" --interval 2 --label "$label" --out "$RUN/p1/load-during-$label.json" >/dev/null 2>&1 &
  local sampler=$!
  sleep 6
  log "import $label ($(wc -l <"$file" | tr -d ' ') rows) via $pod"
  local started=$SECONDS
  k exec "$pod" -c aggregator -- aggregator import-catalog -file "/tmp/$label.ndjson" >"$RUN/p1/$label-import-output.log" 2>&1 || true
  log "import $label took $((SECONDS - started)) s"
  grep '"catalog import completed"' "$RUN/p1/$label-import-output.log" | tail -1 >"$RUN/p1/$label-import-log.json" || true
  cat "$RUN/p1/$label-import-log.json" >&2
  k exec "$pod" -c aggregator -- rm -f "/tmp/$label.ndjson" || true
  wait "$sampler" || true
  python3 scripts/m2-load-window.py "$RUN/p1/load-during-$label.json" --seconds "$window" >"$RUN/p1/load-during-$label-peak.json" || true
}

stage_p1() {
  load_secrets; forward
  local catalog rows samples url
  catalog=$(catalog_file) || exit 1; rows=$(wc -l <"$catalog" | tr -d ' '); url=$(first_url)
  samples=$(( rows / 2000 + 30 ))
  audit_get /internal/audit/distribution >"$RUN/p1/distribution-before-import.json"
  import_with_load "$catalog" catalog "$samples" 20
  cp "$RUN/p1/catalog-import-log.json" "$RUN/p1/import-log.json"
  cp "$RUN/p1/load-during-catalog-peak.json" "$RUN/p1/load-during-import-peak.json"
  audit_get /internal/audit/distribution >"$RUN/p1/distribution-after-import.json"

  local zone="${BURST_ZONE:-laut-maluku}"
  python3 scripts/generate-earthquake-catalog.py --burst --burst-zone "$zone" --burst-count "${BURST_COUNT:-20000}" \
    --burst-minutes 30 --burst-start "$(date -u +%Y-%m-%dT%H:%M:00Z)" --seed "$RANDOM" \
    --out "$RUN/p1/burst.ndjson" --summary "$RUN/p1/burst-dataset-summary.json" >/dev/null
  import_with_load "$RUN/p1/burst.ndjson" burst 25 8
  python3 scripts/m2-p1-alternatives.py --catalog "$catalog" --burst "$RUN/p1/burst.ndjson" \
    --nodes "$(k get pod -l app=cockroachdb --no-headers | wc -l | tr -d ' ')" --out "$RUN/p1/alternative-partitioning.json" >/dev/null

  log "cross-store read"
  python3 scripts/m2-p1-crossread.py --client-api "$url" --pages 20 --label normal --out "$RUN/p1/crossread-normal.json" >/dev/null
  python3 scripts/m2-p1-crossread.py --client-api "$url" --page-size 100 --pages "${CROSSREAD_PAGES:-450}" --label deep \
    --out "$RUN/p1/crossread.json" >/dev/null
  log "analytics vs audit"
  python3 scripts/m2-p1-analytics-check.py --client-api "$url" --out "$RUN/p1/analytics-vs-audit.json" >/dev/null
  python3 scripts/m2-p1-snapshot.py --catalog "$catalog" --label after-import --out "$RUN/p1/snapshot-after-import.json" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print("catalog check:", json.dumps(d.get("catalog", {}))[:400])' | tee -a "$RUN/timeline.txt"
  python3 scripts/m2-store-stats.py --label after-p1 --out "$RUN/p1/store-after-import.json" >/dev/null 2>&1 || true
}

probe_pod_start() {
  k create configmap p1-probe-scripts --from-file=scripts/m2-p1-probe.py --from-file=scripts/m2-p1-crossread.py \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  k delete pod p1-probe --ignore-not-found --wait=true >/dev/null
  cat <<YAML | k apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata: { name: p1-probe, labels: { app: p1-probe, role: other } }
spec:
  restartPolicy: Never
  nodeSelector: { node-role.kubernetes.io/control-plane: "true" }
  tolerations: [ { operator: Exists } ]
  securityContext: { runAsUser: 10001, runAsNonRoot: true }
  containers:
    - name: probe
      image: python:3.13-alpine
      # Balances per request over the pod addresses (headless Services) with a
      # 1 s connect timeout, the way a client of these APIs must during the
      # window before Kubernetes notices a dead node; see PodBalancer.
      command: ["python3", "-u", "/scripts/m2-p1-probe.py", "--client-api", "http://client-api:8080",
                "--aggregator", "http://aggregator:8090", "--client-api-pods", "client-api-pods", "--aggregator-pods", "aggregator-pods",
                "--duration", "3600", "--stop-file", "/tmp/stop",
                "--label", "${PROBE_LABEL:-node-loss-and-rejoin (in-cluster, per-request pod balancing)}", "--out", "/tmp/probe.json"]
      env:
        - { name: FIELD_TEAM_CLIENT_SECRET, valueFrom: { secretKeyRef: { name: bnpb-secrets, key: FIELD_TEAM_CLIENT_SECRET } } }
        - { name: AUDIT_TOKEN, valueFrom: { secretKeyRef: { name: bnpb-secrets, key: AUDIT_TOKEN } } }
      volumeMounts: [ { name: scripts, mountPath: /scripts } ]
      resources: { requests: { cpu: 20m, memory: 32Mi }, limits: { cpu: 200m, memory: 128Mi } }
  volumes: [ { name: scripts, configMap: { name: p1-probe-scripts } } ]
YAML
  k wait --for=condition=Ready pod/p1-probe --timeout=240s >/dev/null
}

probe_pod_finish() {
  k exec p1-probe -- touch /tmp/stop || true
  for _ in $(seq 1 30); do
    [ "$(k get pod p1-probe -o jsonpath='{.status.phase}' 2>/dev/null)" = Succeeded ] && break
    k exec p1-probe -- test -f /tmp/probe.json.tmp 2>/dev/null || break
    sleep 2
  done
  # Write next to the target and move only on success, so a failed copy never
  # truncates the previous run's evidence. A finished container cannot be
  # exec'd into; the probe also prints its result as the last log line, so
  # the pod log is the fallback.
  if k exec p1-probe -- cat /tmp/probe.json >"$1.tmp" 2>/dev/null && [ -s "$1.tmp" ]; then
    mv "$1.tmp" "$1"
  elif k logs p1-probe 2>/dev/null | grep '^{' | tail -1 >"$1.tmp" && [ -s "$1.tmp" ]; then
    mv "$1.tmp" "$1"
  else
    rm -f "$1.tmp"; k logs p1-probe >"$1.log" 2>&1 || true
    log "probe result could not be collected; see $1.log"
  fi
  k delete pod p1-probe --wait=false >/dev/null 2>&1 || true
  k delete configmap p1-probe-scripts >/dev/null 2>&1 || true
}

# One compact distribution sample. The Aggregator port-forward may be pinned
# to a pod on the node that just died; retry while the watchdog reconnects
# and report {"error": ...} instead of failing the stage.
distribution_line() {
  local attempt body
  for attempt in 1 2 3 4 5 6; do
    body=$(audit_get /internal/audit/distribution || true)
    if printf '%s' "$body" | python3 -c 'import json,sys; json.load(sys.stdin)' 2>/dev/null; then
      printf '%s' "$body" | distribution_compact
      return
    fi
    sleep 5
  done
  echo '{"error":"distribution unavailable","partial":true,"nodes":{},"min_live_share":null}'
}
distribution_compact() {
  python3 -c '
import json, sys
d = json.load(sys.stdin)
live = [n for n in d["nodes"] if n["live"]]
print(json.dumps({"ranges": d["ranges"], "partial": d["partial"], "unavailable_units": d.get("unavailable_units"),
                  "nodes": {n["node"] + "#" + str(n["node_id"]): [n["live"], n["replicas"], n["leaseholders"]] for n in d["nodes"]},
                  "min_live_share": round(min(n["replicas"] for n in live) / d["ranges"], 3) if live and d["ranges"] else None}))'
}

retry3() { "$@" || { sleep 10; "$@"; } || { sleep 20; "$@"; }; }

stage_p1_node_loss() {
  need_confirm p1-node-loss
  load_secrets; forward
  local dir="$RUN/p1/node-loss" catalog victim crdb pvc path
  mkdir -p "$dir"
  catalog=$(catalog_file) || exit 1
  if [ "${RESUME:-0}" = 1 ] && [ -f "$dir/victim.env" ] && [ -f "$dir/killed-at.txt" ]; then
    # Continue a run whose fault was already injected (the stage stopped
    # while the node was down): pick up at the healing wait.
    # shellcheck disable=SC1091
    . "$dir/victim.env"
    log "resuming node-loss on $victim (powered off at $(cat "$dir/killed-at.txt"))"
  else
  # Wiping one replica is only safe while every other replica is healthy
  # (RF=3): refuse to start unless every store node is live and every
  # CockroachDB pod is Ready.
  local live total ready
  live=$(audit_get /internal/audit/distribution | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(1 for n in d["nodes"] if n["live"]), d["partial"])')
  total=$(k get pod -l app=cockroachdb --no-headers | wc -l | tr -d ' ')
  ready=$(k get pod -l app=cockroachdb --no-headers | grep -c ' 1/1 *Running' || true)
  [ "$live" = "$total False" ] && [ "$ready" = "$total" ] || die "store not healthy (live+partial: $live, ready pods: $ready/$total); not wiping a node"
  victim="${VICTIM:-}"
  if [ -z "$victim" ]; then
    local singles
    singles=$(singleton_nodes)
    for n in $(k get pod -l app=cockroachdb -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}'); do
      is_control_plane "$n" && continue
      case " $singles " in *" $n "*) continue ;; esac
      victim="$n"; break
    done
  fi
  [ -n "$victim" ] || die "no worker node without postgres/rabbitmq/single auth holds a CockroachDB pod; set VICTIM"
  crdb=$(k get pod -l app=cockroachdb --field-selector "spec.nodeName=$victim" -o jsonpath='{.items[0].metadata.name}')
  [ -n "$crdb" ] || die "no CockroachDB pod on $victim"
  pvc="data-$crdb"
  path=$(kubectl get pv "$(k get pvc "$pvc" -o jsonpath='{.spec.volumeName}')" -o jsonpath='{.spec.hostPath.path}{.spec.local.path}')
  [ -n "$path" ] || die "cannot find the host path of $pvc"
  log "victim $victim (store pod $crdb, disk $path)"
  printf 'victim=%q\ncrdb=%q\npath=%q\n' "$victim" "$crdb" "$path" >"$dir/victim.env"
  k get pods --field-selector "spec.nodeName=$victim" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' >"$dir/pods-on-victim-before.txt"

  python3 scripts/m2-p1-snapshot.py --catalog "$catalog" --label before-node-loss --out "$dir/snapshot-before.json" >/dev/null
  # The snapshot just paged the whole Audit API; let the store and the host
  # settle so the probe's baseline is the system at rest, not our own load.
  sleep "${PROBE_SETTLE:-30}"
  probe_pod_start
  log "in-cluster probe running"
  sleep 20

  # Keep k3s-agent from starting at the next boot until the disk is wiped.
  mp 30 "$victim" -- sudo sh -c 'mkdir -p /etc/systemd/system/k3s-agent.service.d && printf "[Unit]\nConditionPathExists=!/etc/bnpb-hold-k3s\n" > /etc/systemd/system/k3s-agent.service.d/bnpb-hold.conf && touch /etc/bnpb-hold-k3s && systemctl daemon-reload'
  local ip
  ip=$(kubectl get node "$victim" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
  log "POWER OFF $victim (sysrq, no clean shutdown)"
  date -u +%FT%TZ >"$dir/killed-at.txt"
  mp 15 "$victim" -- sudo sh -c 'echo 1 > /proc/sys/kernel/sysrq; echo o > /proc/sysrq-trigger' || true
  until ! ping -c1 -W1 "$ip" >/dev/null 2>&1; do sleep 1; done
  log "$victim unreachable"
  until kubectl get node "$victim" --no-headers | grep -q NotReady; do sleep 3; done
  log "$victim NotReady"
  fi

  log "waiting for re-replication (CockroachDB declares a store dead after 5 min)"
  local deadline=$((SECONDS + ${HEAL_TIMEOUT:-1200})) line
  while [ $SECONDS -lt $deadline ]; do
    line=$(distribution_line); echo "{\"t\":\"$(date -u +%FT%TZ)\",\"phase\":\"healing\",\"d\":$line}" >>"$dir/distribution-series.jsonl"
    echo "$line" | python3 -c "
import json,sys; d=json.load(sys.stdin); v=[k for k in d['nodes'] if k.startswith('$victim#') and d['nodes'][k][1]>0]
sys.exit(0 if 'error' not in d and not d['partial'] and not v else 1)" && break
    sleep 20
  done
  log "healed: $(tail -1 "$dir/distribution-series.jsonl" | cut -c1-300)"
  audit_get /internal/audit/distribution >"$dir/distribution-after-node-loss.json"
  retry3 python3 scripts/m2-p1-snapshot.py --catalog "$catalog" --label after-node-loss --out "$dir/snapshot-after-loss.json" >/dev/null
  python3 scripts/m2-p1-snapshot.py --compare "$dir/snapshot-before.json" "$dir/snapshot-after-loss.json" >"$dir/compare.json" || true
  python3 -c "import json;d=json.load(open('$dir/compare.json'));print('lost',d['lost'],'of',d['before_count'],'catalog match',(d.get('catalog_after') or {}).get('match'))" | tee -a "$RUN/timeline.txt"

  log "START $victim with an empty store disk"
  multipass start "$victim" >/dev/null 2>&1 || true
  until mp 10 "$victim" -- true >/dev/null 2>&1; do sleep 3; done
  # The boot-time hold is best effort (k3s-agent may still start), so stop the
  # agent and every container it started before touching the store disk.
  mp 120 "$victim" -- sudo sh -c "systemctl stop k3s-agent; /usr/local/bin/k3s-killall.sh >/dev/null 2>&1; echo agent: \$(systemctl is-active k3s-agent); du -sh '$path'; rm -rf '$path'/* '$path'/.[!.]* 2>/dev/null; echo wiped; ls -A '$path' | wc -l" | tee -a "$dir/wipe.txt"
  mp 60 "$victim" -- sudo sh -c 'rm -f /etc/bnpb-hold-k3s /etc/systemd/system/k3s-agent.service.d/bnpb-hold.conf; systemctl daemon-reload; systemctl start k3s-agent'
  until kubectl get node "$victim" --no-headers | grep -q ' Ready'; do sleep 3; done
  log "$victim Ready"
  k wait --for=condition=Ready "pod/$crdb" --timeout=600s >/dev/null || true
  k logs "$crdb" 2>&1 | grep -E "initialized new node|node ID|nodeID" | head -5 >"$dir/rejoin-log.txt" || true
  log "rejoin: $(head -1 "$dir/rejoin-log.txt" | cut -c1-200)"

  if [ "${REJOIN_BURST:-1}" = 1 ]; then
    python3 scripts/generate-earthquake-catalog.py --burst --burst-zone "${REJOIN_BURST_ZONE:-jawa-barat}" --burst-count 20000 \
      --burst-minutes 30 --burst-start "$(date -u +%Y-%m-%dT%H:%M:00Z)" --seed "$RANDOM" --out "$dir/rejoin-burst.ndjson" >/dev/null
    local saved="$RUN/p1"
    import_with_load "$dir/rejoin-burst.ndjson" rejoin 20 8
    mv "$saved"/rejoin-import-log.json "$saved"/load-during-rejoin*.json "$saved"/rejoin-import-output.log "$dir/" 2>/dev/null || true
  fi

  log "waiting for rebalance (min live share >= ${REBALANCE_SHARE:-0.6})"
  deadline=$((SECONDS + ${REBALANCE_TIMEOUT:-900}))
  while [ $SECONDS -lt $deadline ]; do
    line=$(distribution_line); echo "{\"t\":\"$(date -u +%FT%TZ)\",\"phase\":\"rebalance\",\"d\":$line}" >>"$dir/distribution-series.jsonl"
    echo "$line" | python3 -c "import json,sys; d=json.load(sys.stdin); sys.exit(0 if 'error' not in d and (d['min_live_share'] or 0) >= ${REBALANCE_SHARE:-0.6} and not d['partial'] else 1)" && break
    sleep 20
  done
  audit_get /internal/audit/distribution >"$dir/distribution-final.json"
  python3 scripts/m2-store-stats.py --label after-rejoin --out "$dir/store-final.json" >/dev/null 2>&1 || true
  # A failing final snapshot is evidence too (18 Nov: the Audit API listing
  # ran out of SQL memory); the probe and the timeline must still be
  # collected below, so it does not abort the stage.
  retry3 python3 scripts/m2-p1-snapshot.py --catalog "$catalog" --label final --out "$dir/snapshot-final.json" >/dev/null || log "final snapshot failed; compare-final.json will be missing"
  python3 scripts/m2-p1-snapshot.py --compare "$dir/snapshot-before.json" "$dir/snapshot-final.json" >"$dir/compare-final.json" 2>/dev/null || rm -f "$dir/compare-final.json"
  python3 scripts/m2-p1-crossread.py --client-api "$(first_url)" --pages 20 --label after-rejoin --out "$dir/crossread-after-rejoin.json" >/dev/null || true
  python3 scripts/m2-p1-analytics-check.py --client-api "$(first_url)" --label after-rejoin --out "$dir/analytics-vs-audit-after-rejoin.json" >/dev/null || true
  probe_pod_finish "$dir/probe.json"
  cp "$RUN/timeline.txt" "$dir/timeline.txt"
  log "dead store node IDs stay listed until decommissioned: cockroach node decommission <id> (root certificate)"
}

stage_p2_spread() {
  load_secrets
  local before after token urls
  urls=$(base_urls)
  per_pod() { for p in $(k get pod -l app=aggregator -o name); do printf '%s %s\n' "${p#pod/}" \
    "$(k exec "${p#pod/}" -c aggregator -- wget -q -O - http://localhost:8090/metrics 2>/dev/null | awk '/^grpc_requests_total.*ListHazards/{s+=$NF} END{print s+0}')"; done; }
  before=$(per_pod)
  token=$(printf '{"client_id":"field-team-client","client_secret":"%s"}' "$FIELD_TEAM_CLIENT_SECRET" |
    curl -s -X POST "$(first_url)/auth/token" -H 'Content-Type: application/json' --data-binary @- | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_token"])')
  local i codes="" entries
  IFS=, read -ra entries <<<"$urls"
  for i in $(seq 0 $(( ${SPREAD_REQUESTS:-60} - 1 ))); do
    codes="$codes $(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $token" "${entries[i % ${#entries[@]}]}/hazards?page_size=5")"
  done
  after=$(per_pod)
  python3 - "$RUN/p2/entry-spread.json" "$before" "$after" "$urls" "$codes" <<'PY'
import collections, json, subprocess, sys
out, before, after, urls, codes = sys.argv[1:]
b = dict(l.split() for l in before.splitlines() if l.strip())
a = dict(l.split() for l in after.splitlines() if l.strip())
nodes = {}
for pod in a:
    nodes[pod] = subprocess.run(["kubectl", "-n", "sistem-informasi-kebencanaan-bnpb", "get", "pod", pod, "-o",
                                 "jsonpath={.spec.nodeName}"], capture_output=True, text=True).stdout
per_pod = {pod: int(float(a[pod]) - float(b.get(pod, 0))) for pod in a}
result = {"requests": len(codes.split()), "entry_points": urls.split(","), "statuses": dict(collections.Counter(codes.split())),
          "per_pod": per_pod, "pod_node": nodes,
          "method": "grpc_requests_total{method=ListHazards} delta per Aggregator pod around GET /hazards sent round-robin over every NodePort"}
json.dump(result, open(out, "w"), indent=2)
print(json.dumps(result))
PY
}

stage_p2_failover() {
  load_secrets; forward
  local logs="$RUN/p2/logs" since pod node cid kt
  collector "$logs"
  since=$(date -u +%FT%TZ)
  log "collecting logs 40 s before the fault"
  sleep 40
  pod=$(leader); [ -n "$pod" ] || die "cannot find the ingestion leader in the Aggregator logs"
  node=$(node_of "$pod")
  if [ "${FAILOVER_KILL:-crictl}" = pod ]; then
    # Pod removed at once (SIGKILL, no grace): its replacement needs a fresh
    # start, so another running instance takes the lease.
    kt=$(date -u +%FT%T.%NZ 2>/dev/null | sed 's/\.N*Z$/Z/')
    log "force-delete leader pod $pod on $node"
    k delete pod "$pod" --grace-period=0 --force >/dev/null 2>&1 || true
    echo "$kt $pod $node force delete (grace 0)" | tee "$RUN/p2/failover-actions.txt"
  else
    # Container SIGKILL on the node; kubelet restarts it in the same pod, which
    # may win the next term itself (still one writer, no gap).
    cid=$(mp 30 "$node" -- sudo k3s crictl ps --name aggregator -q --label "io.kubernetes.pod.name=$pod" | head -1)
    log "SIGKILL leader $pod on $node (crictl stop -t 0 $cid)"
    mp 20 "$node" -- sudo k3s crictl stop -t 0 "$cid" >/dev/null 2>&1 || true
    kt=$(mp 30 "$node" -- sudo k3s crictl inspect "$cid" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"]["finishedAt"])' 2>/dev/null || date -u +%FT%TZ)
    echo "$kt $pod $node SIGKILL (crictl stop -t 0)" | tee "$RUN/p2/failover-actions.txt"
  fi
  sleep "${AFTER_FAULT:-90}"
  python3 scripts/m2-p2-analyze.py "$logs" --since "$since" --until "$(date -u +%FT%TZ)" --event "$kt" \
    --label leader-crash-failover --out "$RUN/p2/failover.json" >/dev/null || true
  python3 -c "import json;d=json.load(open('$RUN/p2/failover.json'));print('failover:',json.dumps(d.get('event'))[:400])" | tee -a "$RUN/timeline.txt"
  local agg
  agg=$(k get pod -l app=aggregator --field-selector status.phase=Running -o jsonpath='{.items[0].metadata.name}')
  k exec "$agg" -c aggregator -- sh -c "wget -q -O - --header \"X-BMKG-Key: \$BMKG_API_KEY\" \"\$BMKG_URL/seismic-events?since=$since\"" >"$RUN/p2/bmkg-mock-events.json"
  sleep 20   # let the newest mock events reach the store before comparing
  python3 scripts/m2-p2-gap-check.py --mock "$RUN/p2/bmkg-mock-events.json" --since "$since" \
    --window-start "$kt" --window-end "$(date -u +%FT%TZ)" --out "$RUN/p2/failover-gap-check.json" >/dev/null || true
  cat "$RUN/p2/failover-gap-check.json" >&2
}

stage_p2_skew() {
  need_confirm p2-skew
  local logs="$RUN/p2/logs" since pod node act="$RUN/p2/clock-skew-actions.txt"
  collector "$logs"; since=$(date -u +%FT%TZ); sleep 30
  pod=$(leader_off); node=$(node_of "$pod")
  is_control_plane "$node" && die "leader $pod still on the control-plane node $node"
  : >"$act"
  note() { echo "$(date -u +%FT%TZ) host | $1 | offset $(node_offset "$node") s" | tee -a "$act"; }
  note "before (leader $pod on $node)"
  mp 20 "$node" -- sudo timedatectl set-ntp false || true
  mp 20 "$node" -- sudo date -s "@$(( $(date +%s) + ${SKEW_AHEAD:-45} ))" >/dev/null 2>&1 || true
  note "node clock +${SKEW_AHEAD:-45}s, NTP off"
  sleep "${SKEW_HOLD:-120}"
  note "after hold"
  mp 20 "$node" -- sudo date -s "@$(( $(date +%s) - ${SKEW_BEHIND:-30} ))" >/dev/null 2>&1 || true
  note "node clock -${SKEW_BEHIND:-30}s"
  sleep "${SKEW_HOLD:-120}"
  note "after hold"
  mp 30 "$node" -- sudo sh -c 'timedatectl set-ntp true; systemctl restart systemd-timesyncd' >/dev/null 2>&1 || true
  for _ in $(seq 1 30); do o=$(node_offset "$node"); [ "$o" != "?" ] && [ "${o#-}" -le 1 ] && break; sleep 5; done
  note "NTP restored"
  # A store node on the skewed VM may crash some time after the clock is
  # back (CockroachDB rejects remote clocks too far off) and then sit in
  # CrashLoopBackOff. Watch for 3 minutes; restart any store pod that is not
  # Ready so the cluster is whole again before the next scenario.
  local deadline=$((SECONDS + 180)) p
  while [ $SECONDS -lt $deadline ]; do
    for p in $(k get pod -l app=cockroachdb --no-headers | awk '$3=="CrashLoopBackOff" || $3=="Error" {print $1}'); do
      k delete pod "$p" --wait=false >/dev/null; note "restarted crash-looping $p"
    done
    sleep 15
  done
  k wait --for=condition=Ready pod -l app=cockroachdb --timeout=300s >/dev/null || note "WARNING store pods not all Ready"
  python3 scripts/m2-p2-analyze.py "$logs" --since "$since" --until "$(date -u +%FT%TZ)" --label "clock-skew-$node" --out "$RUN/p2/clock-skew.json" >/dev/null || true
}

stage_p2_freeze() {
  need_confirm p2-freeze
  local logs="$RUN/p2/logs" since pod node act="$RUN/p2/freeze-actions.txt" event
  collector "$logs"; since=$(date -u +%FT%TZ); sleep 30
  pod=$(AVOID_SINGLETONS=1 leader_off); node=$(node_of "$pod")
  if is_control_plane "$node"; then die "leader $pod still on the control-plane node $node"; fi
  event=$(date -u +%FT%TZ)
  echo "$event suspend $node (leader $pod)" | tee "$act"
  multipass suspend "$node" >/dev/null 2>&1 || true
  sleep "${FREEZE_SECONDS:-60}"
  echo "$(date -u +%FT%TZ) resume $node" | tee -a "$act"
  multipass start "$node" >/dev/null 2>&1 || true
  echo "$(date -u +%FT%TZ) $node: $(multipass list | awk -v n="$node" '$1==n{print $2}')" | tee -a "$act"
  sleep "${AFTER_FAULT:-90}"
  python3 scripts/m2-p2-analyze.py "$logs" --since "$since" --until "$(date -u +%FT%TZ)" --event "$event" \
    --label "freeze-$node" --out "$RUN/p2/freeze.json" >/dev/null || true
}

# Test-only settings for the 10k-VU runs, recorded in load-test-config.txt and
# restored afterwards:
#  - auth ACCESS_TOKEN_TTL=10m: tokens fetched in k6 setup() outlive the run
#    (a refresh per VU would be 10k bcrypt checks);
#  - client-api LOAD_CLIENT_API_REPLICAS (default 4): the entry tier, not the
#    Aggregator, is what saturates first on 2-vCPU nodes.
with_test_ttl() {
  local replicas
  replicas=$(k get deploy client-api -o jsonpath='{.spec.replicas}')
  k set env deploy/auth ACCESS_TOKEN_TTL=10m >/dev/null
  k scale deploy/client-api --replicas="${LOAD_CLIENT_API_REPLICAS:-4}" >/dev/null
  k rollout status deploy/auth --timeout=180s >/dev/null
  k rollout status deploy/client-api --timeout=180s >/dev/null
  echo "$(date -u +%FT%TZ) test-only: auth ACCESS_TOKEN_TTL=10m, client-api replicas $replicas -> ${LOAD_CLIENT_API_REPLICAS:-4} (restored after)" >>"$RUN/p2/load-test-config.txt"
  local rc=0; "$@" || rc=$?
  k set env deploy/auth ACCESS_TOKEN_TTL- >/dev/null
  k scale deploy/client-api --replicas="$replicas" >/dev/null
  echo "$(date -u +%FT%TZ) restored auth ACCESS_TOKEN_TTL, client-api replicas $replicas" >>"$RUN/p2/load-test-config.txt"
  return $rc
}

run_load() {
  BASE_URL=$(worker_urls | cut -d, -f1) BASE_URLS=$(worker_urls) K6_SUMMARY_PATH="$RUN/p2/load.json" TIMELINE="$RUN/p2/load-timeline.txt" \
    ./scripts/m2-load.sh
}

stage_p2_load() {
  need_confirm p2-load
  command -v k6 >/dev/null || die "k6 not found"
  [ -n "${NEW_IMAGE:-}" ] || die "set NEW_IMAGE (e.g. retag the current aggregator image: docker tag <img>:m2.10 <img>:m2.11 && docker push)"
  local current
  current=$(k get deploy aggregator -o jsonpath='{.spec.template.spec.containers[0].image}')
  [ "$current" != "$NEW_IMAGE" ] || die "aggregator already runs $NEW_IMAGE, so there would be no version change; run 'restore' (back to the overlay image) or pick another NEW_IMAGE"
  load_secrets
  local logs="$RUN/p2/logs" since; collector "$logs"; since=$(date -u +%FT%TZ)
  with_test_ttl run_load
  python3 scripts/m2-p2-analyze.py "$logs" --since "$since" --until "$(date -u +%FT%TZ)" --label load-scaling-rolling --out "$RUN/p2/load-exactly-once.json" >/dev/null || true
}

# THROUGHPUT_AGG_CPU=<cpu> (e.g. 500m) caps every aggregator pod's CPU for both
# runs, so the ratio measures how the aggregator tier scales when it is the
# bottleneck; without it the host, client-api and CockroachDB saturate first
# on this testbed and the ratio says little about the aggregator. Removed after.
# A capped run writes throughput-<n>-instance-cpu<cap>.json so the uncapped
# baseline that the summary reads is never overwritten (THROUGHPUT_SUFFIX
# still wins when set).
run_throughput() {
  if [ -n "${THROUGHPUT_AGG_CPU:-}" ]; then
    THROUGHPUT_SUFFIX="${THROUGHPUT_SUFFIX:--cpu$THROUGHPUT_AGG_CPU}"
    k set resources deploy/aggregator --requests=cpu="$THROUGHPUT_AGG_CPU" --limits=cpu="$THROUGHPUT_AGG_CPU" >/dev/null
    k rollout status deploy/aggregator --timeout=180s >/dev/null
    echo "$(date -u +%FT%TZ) test-only: aggregator cpu request/limit $THROUGHPUT_AGG_CPU for both throughput runs (removed after)" >>"$RUN/p2/load-test-config.txt"
  fi
  for r in 1 3; do
    k scale deploy/aggregator --replicas=$r >/dev/null
    until [ "$(k get pods -l app=aggregator --no-headers | grep -c '1/1 *Running')" = "$r" ] && \
          [ "$(k get pods -l app=aggregator --no-headers | wc -l | tr -d ' ')" = "$r" ]; do sleep 2; done
    sleep 40
    log "throughput with $r instance(s)"
    k6 run -q --no-color -e BASE_URL="$(worker_urls | cut -d, -f1)" -e BASE_URLS="$(worker_urls)" -e VUS="${VUS:-10000}" -e RAMP="${RAMP:-30s}" -e DURATION=60s -e THINK="${THINK:-1}" \
      -e LABEL="throughput-$r-instance-${VUS:-10000}vu" \
      -e K6_SUMMARY_PATH="$RUN/p2/throughput-$r-instance${THROUGHPUT_SUFFIX:-}.json" tests/load/k6/m2-sustained.js >/dev/null 2>&1 || true
  done
  if [ -n "${THROUGHPUT_AGG_CPU:-}" ]; then
    # Remove the keys instead of setting them to 0: "cpu: 0" would stay in the
    # live spec and later overlay applies would not clear it.
    k patch deploy/aggregator --type json -p '[{"op":"remove","path":"/spec/template/spec/containers/0/resources/limits/cpu"},{"op":"remove","path":"/spec/template/spec/containers/0/resources/requests/cpu"}]' >/dev/null
    echo "$(date -u +%FT%TZ) removed aggregator cpu request/limit" >>"$RUN/p2/load-test-config.txt"
    if [ "${THROUGHPUT_CANONICAL:-}" = yes ]; then
      # Make the capped pair the one the summary reads for criterion 7 and keep
      # the uncapped pair beside it as -uncapped: on this single-host testbed
      # the uncapped aggregator is not the bottleneck (host CPU, client-api and
      # the store saturate first), so only a per-instance cap measures how the
      # aggregator tier scales. throughput-method.json records the cap.
      for r in 1 3; do
        [ -f "$RUN/p2/throughput-$r-instance.json" ] && [ ! -f "$RUN/p2/throughput-$r-instance-uncapped.json" ] && \
          mv "$RUN/p2/throughput-$r-instance.json" "$RUN/p2/throughput-$r-instance-uncapped.json"
        cp "$RUN/p2/throughput-$r-instance${THROUGHPUT_SUFFIX}.json" "$RUN/p2/throughput-$r-instance.json"
      done
      printf '{"cpu_per_instance": "%s", "vus": %s, "think_seconds": %s, "duration": "60s", "recorded_at": "%s", "note": "aggregator pods capped to this CPU for both runs so the aggregator tier is the bottleneck being scaled; uncapped baseline kept as throughput-*-instance-uncapped.json"}\n' \
        "$THROUGHPUT_AGG_CPU" "${VUS:-10000}" "${THINK:-1}" "$(date -u +%FT%TZ)" >"$RUN/p2/throughput-method.json"
    fi
  fi
  k scale deploy/aggregator --replicas=3 >/dev/null
}

stage_p2_throughput() {
  need_confirm p2-throughput
  command -v k6 >/dev/null || die "k6 not found"
  load_secrets
  with_test_ttl run_throughput
}

stage_p3() {
  load_secrets
  if k get configmap bnpb-config -o jsonpath='{.data.PVMBG_SCHEMA_ADAPTERS}' | grep -q .; then
    log "adapter still enabled from an earlier run: back to the default policy first"
    k patch configmap bnpb-config --type json -p '[{"op":"remove","path":"/data/PVMBG_SCHEMA_ADAPTERS"}]' >/dev/null
    k rollout restart deploy/aggregator >/dev/null
    k rollout status deploy/aggregator --timeout=300s >/dev/null
  fi
  forward
  local logs="$RUN/p2/logs"; collector "$logs"
  AGGREGATOR_URL="$AGG" OUT="$RUN/p3" ./scripts/m2-p3-schema.sh || log "schema scenario exited non-zero"
  log "malformed upstream responses"
  local pod key
  key=$(secret PVMBG_ADMIN_KEY)
  pod=$(k get pod -l app=aggregator --field-selector status.phase=Running -o jsonpath='{.items[0].metadata.name}')
  k get pods --no-headers | awk '{print $1, $4}' >"$RUN/p3/restarts-before.txt"
  : >"$RUN/p3/malformed-timeline.txt"
  for m in truncated deep_nesting invalid_utf8 oversized; do
    echo "$(date -u +%FT%TZ) mode $m" | tee -a "$RUN/p3/malformed-timeline.txt"
    k exec "$pod" -c aggregator -- wget -q -O - --header "X-PVMBG-Admin-Key: $key" --header "Content-Type: application/json" \
      --post-data "{\"mode\":\"$m\",\"count\":1}" http://pvmbg:8082/admin/schema-version >>"$RUN/p3/malformed-timeline.txt" 2>&1 || true
    echo >>"$RUN/p3/malformed-timeline.txt"
    sleep 12
  done
  k exec "$pod" -c aggregator -- wget -q -O - --header "X-PVMBG-Admin-Key: $key" --header "Content-Type: application/json" \
    --post-data '{"mode":"normal"}' http://pvmbg:8082/admin/schema-version >/dev/null 2>&1 || true
  k get pods --no-headers | awk '{print $1, $4}' >"$RUN/p3/restarts-after.txt"
  curl -s -H "Authorization: Bearer $OPS_TOKEN" "$AGG/internal/ops/failures?source=PVMBG&limit=1000" >"$RUN/p3/ops-failures-malformed.json"
  log "field reports with pod kills"
  python3 scripts/m2-p3-field-reports.py --base-urls "$(base_urls)" --aggregator "$AGG" --count "${REPORTS:-400}" --workers 40 \
    --logs "$logs" --out "$RUN/p3/field-reports.json" >/dev/null || log "field-report check exited non-zero"
  if k get configmap bnpb-config -o jsonpath='{.data.PVMBG_SCHEMA_ADAPTERS}' | grep -q .; then
    log "removing the PVMBG adapter again (default policy: no adapter)"
    k patch configmap bnpb-config --type json -p '[{"op":"remove","path":"/data/PVMBG_SCHEMA_ADAPTERS"}]' >/dev/null
    k rollout restart deploy/aggregator >/dev/null
  fi
}

stage_p4() {
  load_secrets; forward
  local pod
  pod=$(k get pod -l app=satellite-ingestor --field-selector status.phase=Running -o jsonpath='{.items[0].metadata.name}')
  k logs -l app=satellite-ingestor --since=10m --tail=400 >"$RUN/p4/ingestor.log" 2>&1 || true
  python3 - "$RUN" "$pod" <<'PY'
import json, re, subprocess, sys, urllib.request, os
run, pod = sys.argv[1], sys.argv[2]
ns = "sistem-informasi-kebencanaan-bnpb"
logs = open(f"{run}/p4/ingestor.log", errors="replace").read()
# Derived from the ingestor's own log: the last "feed status NNN" error in
# the last 10 minutes, or 200 when the stream reported no error.
errors = re.findall(r'feed status (\d{3})', logs)
feed_status = int(errors[-1]) if errors else 200
metrics = {}
for p in subprocess.run(["kubectl", "-n", ns, "get", "pod", "-l", "app=satellite-ingestor", "-o", "name"], capture_output=True, text=True).stdout.split():
    text = subprocess.run(["kubectl", "-n", ns, "exec", p.split("/")[1], "--", "wget", "-q", "-O", "-", "http://localhost:8086/metrics"],
                          capture_output=True, text=True).stdout
    metrics[p.split("/")[1]] = {l.split()[0]: float(l.split()[1]) for l in text.splitlines() if l.startswith("satellite_")}
req = urllib.request.Request("http://localhost:18090/internal/audit/checksum?source=SATELIT",
                             headers={"Authorization": "Bearer " + os.environ["AUDIT_TOKEN"]})
checksum = json.load(urllib.request.urlopen(req, timeout=30))
feed_url = subprocess.run(["kubectl", "-n", ns, "get", "configmap", "bnpb-config", "-o", "jsonpath={.data.FEED_URL}"], capture_output=True, text=True).stdout
result = {"feed_url": feed_url, "feed_status": feed_status, "metrics": metrics, "audit_checksum": checksum}
reference = os.environ.get("P4_REFERENCE")
if reference:
    ref = json.load(open(reference))
    result["reference_comparison"] = {"reference": reference, "reference_count": ref.get("count"), "reference_checksum": ref.get("checksum"),
                                      "count": checksum.get("count"), "checksum": checksum.get("checksum"),
                                      "match": ref.get("checksum") == checksum.get("checksum")}
json.dump(result, open(f"{run}/p4/status.json", "w"), indent=2)
print(json.dumps({k: v for k, v in result.items() if k != "metrics"})[:500])
PY
  if [ -n "${P4_WATCH:-}" ]; then
    log "sampling satellite metrics every 10 s for $P4_WATCH s"
    local end=$((SECONDS + P4_WATCH))
    while [ $SECONDS -lt $end ]; do
      for p in $(k get pod -l app=satellite-ingestor -o name); do
        k exec "${p#pod/}" -- wget -q -O - http://localhost:8086/metrics 2>/dev/null | grep '^satellite_' |
          python3 -c "import json,sys,time; print(json.dumps({'t': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'pod': '${p#pod/}', 'm': {l.split()[0]: float(l.split()[1]) for l in sys.stdin}}))"
      done >>"$RUN/p4/watch.jsonl"
      sleep 10
    done
  fi
}

# p4-feed <sim|real>: point the satellite-ingestor at the in-cluster stand-in
# (services/satellite-feed) or back at the assistants' feed. The ingestor
# notices the different session and starts over at offset 0 by itself.
p4_feed() {
  local url="https://satellite.naufarrel.dev"
  [ "$1" = sim ] && url="http://satellite-feed:8088"
  if [ "$(k get configmap bnpb-config -o jsonpath='{.data.FEED_URL}')" != "$url" ]; then
    k patch configmap bnpb-config --type merge -p "{\"data\":{\"FEED_URL\":\"$url\"}}" >/dev/null
    k rollout restart deploy/satellite-ingestor >/dev/null
  fi
  k rollout status deploy/satellite-ingestor --timeout=240s >/dev/null
  log "satellite-ingestor FEED_URL=$url"
}
stage_p4_feed_sim() { need_confirm p4-feed-sim; p4_feed sim; }
stage_p4_feed_real() { need_confirm p4-feed-real; p4_feed real; }

# p4-sim: the P4 rehearsal (scripts/m2-p4-sim.py) against the stand-in feed:
# live minute, 60x burst with two cut connections and the leader pod killed,
# catch-up time, fast-forward past the accepted lateness, then the stored
# SATELIT events against the independent reference (m2-p4-reference.py).
# Purges SATELIT events first. Leaves the ingestor on the stand-in
# (p4-feed-real switches back).
stage_p4_sim() {
  need_confirm p4-sim
  load_secrets; forward
  mkdir -p "$RUN/p4/sim"; rm -f "$RUN/p4/sim/watch.jsonl" "$RUN/p4/sim/timeline.txt"
  if ! k get secret bnpb-secrets -o jsonpath='{.data.FEED_MOCK_ADMIN_TOKEN}' | grep -q .; then
    k patch secret bnpb-secrets --type merge -p "{\"data\":{\"FEED_MOCK_ADMIN_TOKEN\":\"$(openssl rand -hex 24 | tr -d '\n' | base64 | tr -d '\n')\"}}" >/dev/null
    k rollout restart deploy/satellite-feed >/dev/null 2>&1 || true
  fi
  k rollout status deploy/satellite-feed --timeout=180s >/dev/null
  export FEED_ADMIN_TOKEN FEED_TOKEN
  FEED_ADMIN_TOKEN=$(secret FEED_MOCK_ADMIN_TOKEN); FEED_TOKEN=$(secret FEED_TOKEN)
  p4_feed sim
  ( set +e; while :; do kubectl -n "$NS" port-forward svc/satellite-feed 18088:8088 >/dev/null 2>&1; sleep 1; done ) &
  BG_PIDS+=($!)
  for _ in $(seq 1 30); do curl -s -m 3 -o /dev/null http://localhost:18088/health && break; sleep 1; done
  local rc=0
  FEED=http://localhost:18088 AGG="$AGG" NAMESPACE="$NS" python3 scripts/m2-p4-sim.py "$RUN/p4/sim" || rc=$?
  k logs -l app=satellite-ingestor --tail=3000 --prefix >"$RUN/p4/sim/ingestor.log" 2>&1 || true
  cat "$RUN/p4/sim/timeline.txt" >>"$RUN/timeline.txt"
  return $rc
}

stage_p5_quarantine() {
  for p in $(k get pod -l app=coastal-ingestor -o name); do
    k exec "${p#pod/}" -- wget -q -O - http://localhost:8087/metrics 2>/dev/null | grep -E '^coastal_' | sed "s|^|${p#pod/} |"
  done | python3 -c '
import json, sys
out = {}
for line in sys.stdin:
    pod, name, value = line.split()[0], line.split()[1], float(line.split()[2])
    out.setdefault(pod, {})[name] = value
print(json.dumps(out, indent=2))' >"$RUN/p5/quarantine.json"
  python3 -c "
import json; d=json.load(open('$RUN/p5/quarantine.json'))
for pod, m in d.items():
    print(pod, {k: v for k, v in m.items() if 'quarantined' in k or 'critical' in k or 'accepted' in k})" | tee -a "$RUN/timeline.txt"
}

stage_p5_outage() {
  BASE_URL=$(first_url) OUT="$RUN/p5/sensor-outage.jsonl" ./scripts/m2-p5-sensor-outage.sh >/dev/null
  k scale deployment/sensor-pantai --replicas=1 >/dev/null
  tail -1 "$RUN/p5/sensor-outage.jsonl" | cut -c1-300 >&2
}

stage_p5_shedding() {
  load_secrets
  # load_secrets exported the client secrets; k6 reads them from __ENV.
  k6 run -q --no-color \
    -e FIELD_VUS="${FIELD_VUS:-100}" -e MEDIA_VUS="${MEDIA_VUS:-2000}" -e BASE_URLS="$(base_urls)" \
    -e K6_SUMMARY_PATH="$RUN/p5/shedding.json" tests/load/k6/m2-shedding.js >/dev/null 2>&1 || true
  python3 -c "import json;d=json.load(open('$RUN/p5/shedding.json'));print(json.dumps({k:{x:v[x] for x in ('requests','statuses','served_ratio')} for k,v in d['classes'].items()}))" | tee -a "$RUN/timeline.txt"
}

# sensor_profile <practice|exam> switches the Sensor Pantai profile and waits
# for the sensor pod to come back (one rollout per switch).
#
# The exam profile refuses to start without the demo key ("profil exam
# memerlukan kunci demo (PANTAI_KEY_FILE)"). The key lives in the optional
# Secret sensor-pantai-key, mounted at /var/run/pantai. PANTAI_KEY_FILE is set
# only here, for the exam profile, and removed again for practice: how the
# vendor binary treats a PANTAI_KEY_FILE that points at a missing file under
# the practice profile is unknown, so practice never sees the variable. The
# patch also adds the volume and mount, so the switch works on a Deployment
# applied before the manifest carried them. Nothing here contains the key.
sensor_profile() {
  local profile="$1" stamp patch
  case "$profile" in practice|exam) ;; *) die "sensor_profile: practice or exam, not '$profile'" ;; esac
  stamp=$(date -u +%FT%TZ)
  if [ "$profile" = exam ]; then
    # Checked before anything changes: a ConfigMap left on "exam" without
    # the key would stop the sensor at its next restart.
    k get secret sensor-pantai-key >/dev/null 2>&1 || die "sensor_profile exam: Secret sensor-pantai-key is missing (the exam profile needs the demo key)"
  fi
  k patch configmap bnpb-config --type merge -p "{\"data\":{\"PANTAI_PROFILE\":\"$profile\"}}" >/dev/null
  if [ "$profile" = exam ]; then
    patch='{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":"'$stamp'"}},"spec":{"volumes":[{"name":"pantai-key","secret":{"secretName":"sensor-pantai-key","optional":true}}],"containers":[{"name":"sensor-pantai","env":[{"name":"PANTAI_KEY_FILE","value":"/var/run/pantai/key"}],"volumeMounts":[{"name":"pantai-key","mountPath":"/var/run/pantai","readOnly":true}]}]}}}}'
  else
    patch='{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":"'$stamp'"}},"spec":{"containers":[{"name":"sensor-pantai","env":[{"name":"PANTAI_KEY_FILE","$patch":"delete"}]}]}}}}'
  fi
  k patch deploy/sensor-pantai --type strategic -p "$patch" >/dev/null
  if ! k rollout status deploy/sensor-pantai --timeout=180s >/dev/null; then
    k logs -l app=sensor-pantai --tail=5 2>&1 | sed 's/^/sensor-pantai: /' | tee -a "$RUN/timeline.txt" >&2 || true
    # Never leave the cluster on a profile that does not start.
    [ "$profile" = practice ] || sensor_profile practice
    die "sensor-pantai did not become ready on profile $profile"
  fi
  echo "$stamp sensor-pantai profile $profile" | tee -a "$RUN/timeline.txt"
}

# p5-critical: evidence for "every critical sensor event reaches the Field
# Team receiver, 0 lost, p95 <= 5 s".
#
# Without CONFIRM it only evaluates the notifier histogram as it stands.
#
# With CONFIRM=yes it first produces the samples. The practice profile flags
# just three observations per sensor lifetime, and a restarted sensor serves
# the same observation IDs again, so each of CRITICAL_ROUNDS (default 7; 7 x 3
# = 21 >= MIN_SAMPLES 20) rounds:
#   1. purges the SENSOR_PANTAI HazardEvents (Aggregator Ops endpoint), so the
#      round's observations are new to the store (CRITICAL_PURGE=no skips it);
#   2. deletes the sensor pod and waits for its replacement to be Ready (the
#      sensor's virtual clock and stream start over);
#   3. waits CRITICAL_ROUND_WAIT seconds (default 200; the third critical
#      observation appears ~165 s after a sensor start), sampling the notifier
#      count every 10 s into p5/critical-rounds-samples.jsonl.
# $RUN/p5/critical-rounds.json then holds, per round, the start time, the
# number of critical readings the ingestors forwarded
# (coastal_critical_readings_total, summed over pods), the number they could
# not forward (coastal_critical_quarantined_total) and the notifier's new
# latency samples. "0 lost" is ingestor delta == notifier delta (== 3 per
# round on the practice profile; CRITICAL_PER_ROUND overrides, empty = unknown).
#
# PANTAI_EXAM=yes PANTAI_KEY_FILE=<local file> stores the demo key in the
# Secret sensor-pantai-key, runs the same thing on the exam profile, and
# switches back to practice afterwards (PANTAI_KEEP_EXAM=yes keeps exam).
# How many critical observations the exam profile emits is not known, so its
# rounds only require ingestor delta == notifier delta. CRITICAL_ROUNDS=0 with
# PANTAI_EXAM=yes switches the profile, waits PANTAI_EXAM_WARMUP (90 s) and
# evaluates without restarting the sensor again.
stage_p5_critical() {
  local rounds="${CRITICAL_ROUNDS:-7}" round_wait="${CRITICAL_ROUND_WAIT:-200}"
  local reference="${GATING_REFERENCE:-${LATENCY_FROM:-received_at}}" service="${SERVICE:-critical-notifier}"
  local per_round="${CRITICAL_PER_ROUND-3}" exam="${PANTAI_EXAM:-}"
  [ "${CONFIRM:-}" = yes ] || rounds=0
  if [ "$exam" = yes ]; then
    need_confirm p5-critical
    [ -n "${PANTAI_KEY_FILE:-}" ] && [ -r "${PANTAI_KEY_FILE:-}" ] && [ -s "${PANTAI_KEY_FILE:-}" ] \
      || die "PANTAI_EXAM=yes needs PANTAI_KEY_FILE=<readable local file with the demo key>; the exam profile does not start without it"
    # Create or replace; the key goes from the file to the API server only.
    k create secret generic sensor-pantai-key --from-file=key="$PANTAI_KEY_FILE" --dry-run=client -o yaml | k apply -f - >/dev/null
    [ -n "${CRITICAL_PER_ROUND+set}" ] || per_round=""
    sensor_profile exam
    if [ "$rounds" -eq 0 ]; then
      log "waiting ${PANTAI_EXAM_WARMUP:-90} s for critical observations to flow"
      sleep "${PANTAI_EXAM_WARMUP:-90}"
    fi
  fi

  # One reconnecting port-forward per notifier pod (the image has no shell).
  local urls="" port=18093 p pid forwards=()
  for p in $(k get pod -l consumer=critical --field-selector status.phase=Running -o name); do
    ( set +e; while :; do kubectl -n "$NS" port-forward "$p" "$port:8093" >/dev/null 2>&1; sleep 1; done ) &
    forwards+=($!); BG_PIDS+=($!)
    urls="$urls http://localhost:$port/metrics"; port=$((port + 1))
  done
  [ -n "$urls" ] || die "no critical-notifier pods (label consumer=critical)"
  local url
  for url in $urls; do
    for _ in $(seq 1 20); do curl -s -f -m 3 -o /dev/null "$url" && break; sleep 1; done
  done

  # Sum of one coastal_* counter over every ingestor pod and label set; "ERR"
  # when a pod could not be read (a delta built on it would be a guess).
  _p5c_ingestor_sum() {
    local pod value total=0
    for pod in $(k get pod -l app=coastal-ingestor --field-selector status.phase=Running -o name); do
      value=$(k exec "${pod#pod/}" -- wget -q -O - http://localhost:8087/metrics 2>/dev/null \
        | awk -v m="$1" '$1 == m || index($1, m "{") == 1 { s += $2 } END { printf "%d", s }') || { echo ERR; return 0; }
      total=$((total + value))
    done
    echo "$total"
  }
  # Latency samples the notifiers hold for the gating reference; "ERR" likewise.
  _p5c_notifier_count() {
    local u value total=0 try
    for u in $urls; do
      value=ERR
      for try in 1 2 3 4 5; do
        value=$(curl -s -f -m 5 "$u" | awk -v from="$reference" -v service="$service" '
          /^critical_notification_latency_seconds_count\{/ && index($0, "from=\"" from "\"") && index($0, "service=\"" service "\"") { s += $NF }
          END { printf "%d", s }') && break
        value=ERR; sleep 2
      done
      [ "$value" != ERR ] || { echo ERR; return 0; }
      total=$((total + value))
    done
    echo "$total"
  }

  if [ "$rounds" -gt 0 ]; then
    load_secrets; forward
    local n code started ready ended old_uids i0 q0 n0 i1 q1 n1 end
    : >"$RUN/p5/critical-rounds.jsonl"; : >"$RUN/p5/critical-rounds-samples.jsonl"
    for n in $(seq 1 "$rounds"); do
      started=$(date -u +%FT%TZ)
      i0=$(_p5c_ingestor_sum coastal_critical_readings_total); q0=$(_p5c_ingestor_sum coastal_critical_quarantined_total); n0=$(_p5c_notifier_count)
      code=skipped
      if [ "${CRITICAL_PURGE:-yes}" = yes ]; then
        code=$(curl -s -m 120 -o "$RUN/p5/critical-purge-last.json" -w '%{http_code}' -X POST \
          -H "Authorization: Bearer $OPS_TOKEN" "$AGG/internal/ops/purge?source=SENSOR_PANTAI") || code=000
        case "$code" in
          2*) ;;
          *) die "round $n: POST /internal/ops/purge?source=SENSOR_PANTAI answered HTTP $code (is the Aggregator with the Ops purge endpoint deployed? CRITICAL_PURGE=no runs without purging)" ;;
        esac
      fi
      old_uids=$(k get pod -l app=sensor-pantai -o jsonpath='{.items[*].metadata.uid}')
      k delete pod -l app=sensor-pantai --wait=false >/dev/null
      # Ready means a pod that is not one of the deleted ones: the old pod
      # still reports Ready for a moment while it terminates.
      ready=""
      for _ in $(seq 1 90); do
        if k get pod -l app=sensor-pantai -o jsonpath='{range .items[*]}{.metadata.uid}{"\t"}{.metadata.deletionTimestamp}{"\t"}{.status.containerStatuses[0].ready}{"\n"}{end}' 2>/dev/null \
          | awk -F'\t' -v old="$old_uids" '$1 != "" && $2 == "" && $3 == "true" && index(old, $1) == 0 { found = 1 } END { exit !found }'; then
          ready=$(date -u +%FT%TZ); break
        fi
        sleep 2
      done
      [ -n "$ready" ] || die "round $n: the new sensor-pantai pod did not become Ready within 180 s"
      log "p5-critical round $n/$rounds: purge HTTP $code, sensor restarted, waiting ${round_wait} s"
      end=$((SECONDS + round_wait))
      while [ $SECONDS -lt $end ]; do
        printf '{"t":"%s","round":%s,"notifier_samples":"%s"}\n' "$(date -u +%FT%TZ)" "$n" "$(_p5c_notifier_count)" >>"$RUN/p5/critical-rounds-samples.jsonl"
        sleep 10
      done
      ended=$(date -u +%FT%TZ)
      i1=$(_p5c_ingestor_sum coastal_critical_readings_total); q1=$(_p5c_ingestor_sum coastal_critical_quarantined_total); n1=$(_p5c_notifier_count)
      printf '{"round":%s,"started_at":"%s","sensor_ready_at":"%s","ended_at":"%s","purge_http":"%s","ingestor":["%s","%s"],"quarantined":["%s","%s"],"notifier":["%s","%s"]}\n' \
        "$n" "$started" "$ready" "$ended" "$code" "$i0" "$i1" "$q0" "$q1" "$n0" "$n1" >>"$RUN/p5/critical-rounds.jsonl"
    done
    python3 - "$RUN/p5/critical-rounds.jsonl" "$RUN/p5/critical-rounds.json" "$per_round" "$reference" "$([ "$exam" = yes ] && echo exam || echo practice)" <<'PY' | tee -a "$RUN/timeline.txt" >&2
import json, sys, time
src, out, per_round, reference, profile = sys.argv[1:]
expected = int(per_round) if per_round.strip() else None

def delta(pair):
    # "ERR" (a pod could not be read) or a counter that went down (a pod
    # restarted) makes the delta unknown rather than a number to trust.
    try:
        before, after = int(pair[0]), int(pair[1])
    except ValueError:
        return None
    return after - before if after >= before else None

rounds = []
for line in open(src, encoding="utf-8"):
    r = json.loads(line)
    ingestor, notifier, quarantined = delta(r["ingestor"]), delta(r["notifier"]), delta(r["quarantined"])
    known = ingestor is not None and notifier is not None
    rounds.append({
        "round": r["round"], "started_at": r["started_at"], "sensor_ready_at": r["sensor_ready_at"], "ended_at": r["ended_at"],
        "purge_http": r["purge_http"],
        "ingestor_critical_delta": ingestor, "notifier_samples_delta": notifier, "critical_quarantined_delta": quarantined,
        "zero_lost": (known and ingestor == notifier and quarantined == 0 and (expected is None or ingestor == expected)) if known and quarantined is not None else None,
    })

def total(key):
    values = [r[key] for r in rounds]
    return None if any(v is None for v in values) else sum(values)

ingestor, notifier, quarantined = total("ingestor_critical_delta"), total("notifier_samples_delta"), total("critical_quarantined_delta")
expected_total = expected * len(rounds) if expected is not None else None
known = None not in (ingestor, notifier, quarantined)
result = {
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "sensor_profile": profile,
    "notifier_reference": reference,
    "expected_critical_per_round": expected,
    "rounds": rounds,
    "totals": {"ingestor_critical": ingestor, "notifier_samples": notifier, "critical_quarantined": quarantined, "expected": expected_total},
    # ingestor == notifier: everything the ingestor forwarded was delivered.
    # quarantined == 0: nothing flagged critical was set aside undelivered.
    # ingestor == expected: the ingestor saw every critical observation the
    # sensor emits per lifetime (known for the practice profile only).
    "zero_lost": (ingestor == notifier and quarantined == 0 and (expected_total is None or ingestor == expected_total)) if known else None,
}
json.dump(result, open(out, "w", encoding="utf-8"), indent=1)
print(f"p5-critical rounds: ingestor forwarded {ingestor}, notifier delivered {notifier}, critical quarantined {quarantined}, "
      f"expected {expected_total}, zero_lost {result['zero_lost']} -> {out}")
PY
  fi

  local rc=0
  NOTIFIER_METRICS_URLS="${urls# }" OUT_DIR="$RUN" GATING_REFERENCE="$reference" SERVICE="$service" \
    WAIT_SECONDS="${WAIT_SECONDS:-$([ "$rounds" -gt 0 ] && echo 10 || echo 120)}" bash ./scripts/m2-critical-latency.sh || rc=$?
  case $rc in
    0) ;;
    3) log "critical latency: too few critical events (rerun with CONFIRM=yes to produce them in rounds, or lower MIN_SAMPLES)" ;;
    *) log "critical latency check exited $rc" ;;
  esac
  for pid in "${forwards[@]}"; do kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; done
  pkill -f "port-forward pod/critical-notifier" 2>/dev/null || true
  if [ "$exam" = yes ] && [ "${PANTAI_KEEP_EXAM:-}" != yes ]; then
    sensor_profile practice
  fi
}

# notifier_metrics <file> appends every critical-notifier pod's /metrics to file
# (port-forward per pod; the image has no shell).
notifier_metrics() {
  : >"$1"
  local p pid
  for p in $(k get pod -l consumer=critical --field-selector status.phase=Running -o name); do
    kubectl -n "$NS" port-forward "$p" 18099:8093 >/dev/null 2>&1 & pid=$!
    for _ in $(seq 1 20); do curl -s -f -m 2 http://localhost:18099/metrics >>"$1" 2>/dev/null && break; sleep 0.5; done
    kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
  done
}

# cluster_snapshot prints one JSON line: pods not ready, deployments short of
# replicas, nodes not Ready, store partial flag.
cluster_snapshot() {
  local not_ready short nodes dist
  not_ready=$(k get pods --no-headers 2>/dev/null | awk '$3!="Running" && $3!="Completed" {print $1":"$3}' | paste -s -d, - || true)
  short=$(k get deploy --no-headers 2>/dev/null | awk '{split($2,a,"/"); if (a[1]!=a[2]) print $1":"$2}' | paste -s -d, - || true)
  nodes=$(kubectl get nodes --no-headers 2>/dev/null | awk '$2!="Ready" {print $1}' | paste -s -d, - || true)
  dist=$({ curl -s -m 5 -H "Authorization: Bearer $AUDIT_TOKEN" "$AGG/internal/audit/distribution" 2>/dev/null || true; } | python3 -c 'import json,sys
try: d=json.load(sys.stdin); print(json.dumps({"partial": d.get("partial"), "unavailable_units": d.get("unavailable_units")}))
except Exception: print("{\"error\": \"no answer\"}")')
  printf '{"t":"%s","pods_not_running":"%s","deployments_short":"%s","nodes_not_ready":"%s","store":%s}\n' "$(date -u +%FT%TZ)" "$not_ready" "$short" "$nodes" "$dist"
}

# p5-gameday: run Tester Agent practice scenarios (no key needed) one after
# another, hands off during GAMEDAY_WINDOW seconds after each, while the
# in-cluster probe and the critical-notifier histogram measure the SLOs.
# GAMEDAY_SCENARIOS="a b c" picks the scenarios (default: every practice
# scenario the agent lists). Requires the assistants' Tester Agent DaemonSet
# (kubectl apply -f <manifest from the spec>); the image stays theirs.
stage_p5_gameday() {
  need_confirm p5-gameday
  load_secrets; forward
  local dir="$RUN/p5/gameday" start name list window
  mkdir -p "$dir"
  kubectl -n tester-agent get daemonset tester-agent >/dev/null 2>&1 || die "Tester Agent is not installed (kubectl apply -f <manifest.yaml from the spec>)"
  kubectl -n tester-agent rollout status daemonset/tester-agent --timeout=120s >/dev/null || die "Tester Agent pods are not ready"
  window="${GAMEDAY_WINDOW:-300}"
  start=$(date -u +%FT%TZ)
  kubectl -n tester-agent exec daemonset/tester-agent -- /app list >"$dir/list.txt" 2>&1 || true
  list="${GAMEDAY_SCENARIOS:-$(grep -oE '^[a-z0-9][a-z0-9_-]+' "$dir/list.txt" | paste -s -d ' ' -)}"
  [ -n "$list" ] || die "no scenario names (see $dir/list.txt; set GAMEDAY_SCENARIOS)"
  log "game day: scenarios [$list], hands-off window ${window}s each"
  notifier_metrics "$dir/notifier-before.txt"
  cluster_snapshot >"$dir/metrics.jsonl"
  PROBE_LABEL="game day (in-cluster, per-request pod balancing)" probe_pod_start
  echo "[]" >"$dir/scenarios.json"; : >"$dir/agent-log.txt"
  for name in $list; do
    local s_at e_at
    s_at=$(date -u +%FT%TZ)
    log "scenario $name"
    kubectl -n tester-agent exec daemonset/tester-agent -- /app run practice "$name" >"$dir/scenario-$name.txt" 2>&1 || log "agent exited non-zero for $name (see scenario-$name.txt)"
    # Stimulus, not repair: restarting the vendor sensor makes it emit its
    # critical observations again (the practice profile has three, in the
    # first three minutes), so the alert path is measured during every fault.
    [ "${GAMEDAY_CRITICAL:-yes}" = yes ] && k delete pod -l app=sensor-pantai --wait=false >/dev/null 2>&1 || true
    # Hands off: only observe.
    local end=$((SECONDS + window))
    while [ $SECONDS -lt $end ]; do cluster_snapshot >>"$dir/metrics.jsonl"; sleep 15; done
    e_at=$(date -u +%FT%TZ)
    # Collect the agent log now, pod by pod: after a wipe the log of the pod
    # on that node is gone, and one unreachable pod fails a selector query.
    for p in $(kubectl -n tester-agent get pods -o name); do
      kubectl -n tester-agent logs "$p" --since-time="$s_at" --prefix >>"$dir/agent-log.txt" 2>/dev/null || true
    done
    python3 - "$dir/scenarios.json" "$name" "$s_at" "$e_at" "$dir/scenario-$name.txt" <<'PY'
import json, sys
path, name, s, e, out = sys.argv[1:]
items = json.load(open(path)); items.append({"name": name, "started_at": s, "ended_at": e, "agent_stdout": open(out, errors="replace").read()[:2000]})
json.dump(items, open(path, "w"), indent=1)
PY
  done
  sort -u "$dir/agent-log.txt" -o "$dir/agent-log.txt" 2>/dev/null || true
  notifier_metrics "$dir/notifier-after.txt"
  probe_pod_finish "$dir/probe.json"
  local nodes
  nodes=$(kubectl get nodes --no-headers | awk '$2!="Ready" {print $1}' | paste -s -d, -)
  python3 - "$dir/recovery.json" "$window" "$nodes" <<'PY'
import json, subprocess, sys
out, window, nodes = sys.argv[1:]
ns = "sistem-informasi-kebencanaan-bnpb"
deploys = subprocess.run(["kubectl", "-n", ns, "get", "deploy", "-o", "json"], capture_output=True, text=True).stdout
items = json.loads(deploys)["items"]
short = [d["metadata"]["name"] for d in items if (d["status"].get("availableReplicas") or 0) < (d["spec"].get("replicas") or 0)]
json.dump({"window_seconds": int(window), "deployments_available": not short, "deployments_short": short,
           "nodes_not_ready": [n for n in nodes.split(",") if n], "store_partial": None}, open(out, "w"), indent=1)
PY
  curl -s -m 10 -H "Authorization: Bearer $AUDIT_TOKEN" "$AGG/internal/audit/distribution" >"$dir/distribution-final.json" || true
  python3 - "$dir/recovery.json" "$dir/distribution-final.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
try: r["store_partial"] = json.load(open(sys.argv[2])).get("partial")
except Exception: r["store_partial"] = None
json.dump(r, open(sys.argv[1], "w"), indent=1)
PY
  python3 scripts/m2-p5-gameday.py "$dir"
  cp "$RUN/timeline.txt" "$dir/timeline.txt" 2>/dev/null || true
  if [ -n "$nodes" ]; then
    log "nodes still NotReady after the hands-off window: $nodes (a VM the agent powered off has to be started from the host: multipass start <node>)"
  fi
}

stage_summary() { python3 scripts/m2-summary.py "$RUN"; }

stage_restore() {
  for n in $(kubectl get nodes -o name | sed 's|node/||'); do
    mp 30 "$n" -- sudo sh -c 'rm -f /etc/bnpb-hold-k3s /etc/systemd/system/k3s-agent.service.d/bnpb-hold.conf; systemctl daemon-reload; timedatectl set-ntp true; systemctl restart systemd-timesyncd' >/dev/null 2>&1 &
  done
  wait
  k delete pod p1-probe --ignore-not-found --wait=false >/dev/null
  k delete configmap p1-probe-scripts --ignore-not-found >/dev/null
  k set env deploy/auth ACCESS_TOKEN_TTL- >/dev/null 2>&1 || true
  if k get configmap bnpb-config -o jsonpath='{.data.PVMBG_SCHEMA_ADAPTERS}' | grep -q .; then
    k patch configmap bnpb-config --type json -p '[{"op":"remove","path":"/data/PVMBG_SCHEMA_ADAPTERS"}]' >/dev/null
    k rollout restart deploy/aggregator >/dev/null
  fi
  kubectl apply -k deploy/kustomize/overlays/cluster >/dev/null
  echo "restored: NTP on, hold flags removed, probe gone, auth TTL default, adapter off, overlay re-applied (replicas, sensor)" >&2
}

stage_set_audit_token() {
  [ -n "${NEW_AUDIT_TOKEN:-}" ] || die "set NEW_AUDIT_TOKEN to the assistants' Audit API token (it is not printed or stored)"
  k patch secret bnpb-secrets --type merge -p "{\"data\":{\"AUDIT_TOKEN\":\"$(printf %s "$NEW_AUDIT_TOKEN" | base64 | tr -d '\n')\"}}" >/dev/null
  k rollout restart deploy/aggregator >/dev/null
  k rollout status deploy/aggregator --timeout=300s >/dev/null
  echo "AUDIT_TOKEN replaced and Aggregator restarted" >&2
}

# ------------------------------------------------------------------ main

[ $# -gt 0 ] || { sed -n '2,/^set -euo/p' "$0" | sed '$d'; exit 2; }
command -v kubectl >/dev/null || die "kubectl not found"
if [ "$1" = all ]; then set -- prep p1 p2-spread p2-failover p3 p4 p5-quarantine p5-outage p5-shedding p5-critical summary; fi
if [ "$1" = prep ]; then RUN=""; else use_run; fi
while [ $# -gt 0 ]; do
  stage="$1"; shift
  arg=""
  if [ "$stage" = dataset ] && [ $# -gt 0 ] && [ -f "$1" ]; then arg="$1"; shift; fi
  fn="stage_${stage//-/_}"
  declare -F "$fn" >/dev/null || die "unknown stage $stage"
  [ "$stage" = prep ] || log "stage $stage"
  case "$stage" in summary|restore|clocks|dataset) ;; *) check_clocks ;; esac
  # Run the stage in a subshell with errexit really on (it is ignored on the
  # left of ||), and its own EXIT trap so its background loops stop with it.
  set +e
  # shellcheck disable=SC2086
  ( set -e; trap cleanup EXIT; "$fn" $arg )
  rc=$?
  set -e
  if [ "$stage" = prep ]; then RUN=$(ls -1d "$RUNS"/m2-* | sort | tail -1); fi
  stage_done "$stage" "$rc"
done
echo "run folder: $RUN" >&2
