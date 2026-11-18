#!/usr/bin/env python3
"""P4 rehearsal against the in-cluster Feed Satelit stand-in.

Drives services/satellite-feed through the situations P4 grades and records
what the satellite-ingestor did:

  live      a minute at normal speed
  burst     60x for BURST seconds, with two cut connections and the leader
            pod force-deleted while fires are being recognised
  recover   time until the committed offset reaches the feed's latest offset
  forward   a fast-forward so late arrivals pass the accepted lateness and old
            fires close
  compare   the stored SATELIT HazardEvents against scripts/m2-p4-reference.py,
            an independent implementation of the fire definition

Everything is written to OUT (default $RUN/p4/sim). The feed and the Audit API
are reached through port-forwards the runner opened:
  FEED=http://localhost:18088  AGG=http://localhost:18090
Tokens come from the environment (FEED_ADMIN_TOKEN, AUDIT_TOKEN, OPS_TOKEN).
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request

NS = os.environ.get("NAMESPACE", "sistem-informasi-kebencanaan-bnpb")
FEED = os.environ.get("FEED", "http://localhost:18088")
AGG = os.environ.get("AGG", "http://localhost:18090")
OUT = sys.argv[1]
LIVE = int(os.environ.get("P4_LIVE", "45"))
BURST = int(os.environ.get("P4_BURST", "180"))
BURST_SPEED = float(os.environ.get("P4_BURST_SPEED", "60"))
FORWARD = int(os.environ.get("P4_FORWARD", "240"))
FORWARD_SPEED = float(os.environ.get("P4_FORWARD_SPEED", "300"))
LATENESS_MIN = int(os.environ.get("P4_LATENESS_MINUTES", "720"))
SEED = int(os.environ.get("P4_SEED", "4031"))
os.makedirs(OUT, exist_ok=True)


def now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def log(message):
    line = f"{now()} {message}"
    print(line, flush=True)
    with open(f"{OUT}/timeline.txt", "a") as handle:
        handle.write(line + "\n")


def http(method, url, body=None, headers=None, timeout=20):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
    try:
        return json.loads(raw)
    except ValueError:
        return raw.decode(errors="replace")


def admin(path, body=None):
    return http("POST", FEED + path, body or {}, {"X-Admin-Token": os.environ["FEED_ADMIN_TOKEN"]})


def session():
    return http("GET", FEED + "/v1/session", headers={"Authorization": "Bearer " + os.environ["FEED_TOKEN"]})


def kubectl(*args, timeout=30):
    return subprocess.run(["kubectl", "-n", NS, *args], capture_output=True, text=True, timeout=timeout).stdout


def pods():
    return [p.split("/")[1] for p in kubectl("get", "pod", "-l", "app=satellite-ingestor", "--field-selector", "status.phase=Running", "-o", "name").split()]


def pod_get(pod, path):
    try:
        return kubectl("exec", pod, "--", "wget", "-q", "-T", "4", "-O", "-", f"http://localhost:8086{path}", timeout=10)
    except subprocess.TimeoutExpired:
        return ""


def statuses():
    out = {}
    for pod in pods():
        try:
            out[pod] = json.loads(pod_get(pod, "/status"))
        except ValueError:
            pass
    return out


def leader_status():
    for pod, status in statuses().items():
        if status.get("leader"):
            return pod, status
    return None, {}


def metrics(pod):
    values = {}
    for line in pod_get(pod, "/metrics").splitlines():
        if line.startswith("satellite_"):
            name, _, value = line.rpartition(" ")
            try:
                values[name] = float(value)
            except ValueError:
                pass
    return values


# ---------------------------------------------------------------- sampler
samples = []          # one line per 5 s: feed session + every pod's status
histograms = {}       # pod -> last cumulative metrics seen (a killed pod keeps its last sample)
stop = threading.Event()


def sampler():
    while not stop.is_set():
        entry = {"t": now(), "epoch": time.time()}
        try:
            entry["feed"] = session()
        except Exception as error:  # the feed may be restarting
            entry["feed_error"] = str(error)
        entry["pods"] = statuses()
        for pod in entry["pods"]:
            values = metrics(pod)
            if values:
                histograms[pod] = values
        samples.append(entry)
        with open(f"{OUT}/watch.jsonl", "a") as handle:
            handle.write(json.dumps(entry) + "\n")
        stop.wait(5)


def lag():
    """(latest feed offset, committed offset of the leader)."""
    latest = session().get("latest_offset", 0)
    _, status = leader_status()
    return latest, status.get("committed_offset", -1)


def wait_caught_up(limit, label):
    started = time.time()
    while time.time() - started < limit:
        try:
            latest, committed = lag()
        except Exception:
            time.sleep(1)
            continue
        if committed >= latest >= 0:
            seconds = time.time() - started
            log(f"{label}: caught up at offset {committed} after {seconds:.1f} s")
            return seconds
        time.sleep(1)
    log(f"{label}: NOT caught up after {limit} s (latest {latest}, committed {committed})")
    return None


def quantile(buckets, q):
    """Upper bucket bound holding quantile q of a cumulative histogram."""
    total = buckets[-1][1] if buckets else 0
    if not total:
        return None
    for bound, count in buckets:
        if count >= q * total:
            return bound
    return float("inf")


def latency_summary(before):
    merged, count, total = {}, 0.0, 0.0
    for pod, values in histograms.items():
        base = before.get(pod, {})
        for name, value in values.items():
            delta = value - base.get(name, 0.0)
            if name.startswith('satellite_event_to_store_seconds_bucket{le="'):
                bound = name.split('"')[1]
                merged[bound] = merged.get(bound, 0.0) + delta
            elif name == "satellite_event_to_store_seconds_count":
                count += delta
            elif name == "satellite_event_to_store_seconds_sum":
                total += delta
    buckets = sorted(((float("inf") if b == "+Inf" else float(b)), c) for b, c in merged.items())
    return {
        "events": int(count),
        "mean_seconds": round(total / count, 3) if count else None,
        "p50_upper_bound_seconds": quantile(buckets, 0.50),
        "p95_upper_bound_seconds": quantile(buckets, 0.95),
        "p99_upper_bound_seconds": quantile(buckets, 0.99),
        "buckets": [[("+Inf" if b == float("inf") else b), int(c)] for b, c in buckets],
        "note": "from satellite_event_to_store_seconds: reading a feed event off the socket until its fires are stored by the Aggregator and the offset is committed; quantiles are histogram bucket upper bounds",
    }


def main():
    result = {"started_at": now(), "config": {"live_s": LIVE, "burst_s": BURST, "burst_speed": BURST_SPEED,
                                               "forward_s": FORWARD, "forward_speed": FORWARD_SPEED,
                                               "accepted_lateness_minutes": LATENESS_MIN, "seed": SEED}}
    # A fresh feed session: the ingestor notices offsets restarting and starts over.
    purge = http("POST", AGG + "/internal/ops/purge?source=SATELIT", headers={"Authorization": "Bearer " + os.environ["OPS_TOKEN"]}, timeout=120)
    log(f"purged previous SATELIT events: {purge}")
    log(f"feed reset: {admin('/admin/reset', {'profile': 'rehearsal-' + str(int(time.time())), 'seed': SEED})}")
    admin("/admin/speed", {"speed": 1, "duration_seconds": 0})
    threading.Thread(target=sampler, daemon=True).start()

    log(f"phase live: speed 1 for {LIVE} s")
    time.sleep(LIVE)
    result["live"] = {"caught_up_s": wait_caught_up(30, "live")}
    before = {pod: metrics(pod) for pod in pods()}

    log(f"phase burst: speed {BURST_SPEED:g} for {BURST} s")
    admin("/admin/speed", {"speed": BURST_SPEED, "duration_seconds": 0})
    burst_started = time.time()
    actions = []
    time.sleep(BURST * 0.22)
    admin("/admin/drop")
    actions.append({"t": now(), "action": "feed cut every open stream"})
    log("burst: feed dropped the connection")
    time.sleep(BURST * 0.22)
    victim, status = leader_status()
    if victim:
        kubectl("delete", "pod", victim, "--grace-period=0", "--force", "--wait=false")
        actions.append({"t": now(), "action": f"force-deleted leader pod {victim}", "committed_offset": status.get("committed_offset"), "active_fires": status.get("working_set", {}).get("ActiveFires")})
        log(f"burst: force-deleted leader {victim} at committed offset {status.get('committed_offset')} with {status.get('working_set', {}).get('ActiveFires')} active fires")
    time.sleep(BURST * 0.22)
    admin("/admin/drop")
    actions.append({"t": now(), "action": "feed cut every open stream"})
    log("burst: feed dropped the connection again")
    time.sleep(max(0, BURST - (time.time() - burst_started)))
    admin("/admin/speed", {"speed": 1, "duration_seconds": 0})
    latest_at_end, committed_at_end = lag()
    log(f"burst over: feed latest offset {latest_at_end}, committed {committed_at_end}, behind by {latest_at_end - committed_at_end}")
    recovered = wait_caught_up(180, "after burst")
    result["burst"] = {"actions": actions, "latest_offset_at_end": latest_at_end, "committed_at_end": committed_at_end,
                       "behind_at_end_events": latest_at_end - committed_at_end, "seconds_to_catch_up": recovered,
                       "max_lag_events": max((p.get("lag_events", 0) for s in samples for p in s.get("pods", {}).values() if p.get("leader")), default=0)}
    time.sleep(6)  # one more sampler round so the histograms include the tail
    result["latency_burst"] = latency_summary(before)

    log(f"phase forward: speed {FORWARD_SPEED:g} for {FORWARD} s (late arrivals, closing)")
    admin("/admin/speed", {"speed": FORWARD_SPEED, "duration_seconds": 0})
    time.sleep(FORWARD)
    # Freeze the feed so the reference and the store are read at one offset.
    admin("/admin/speed", {"speed": 0, "duration_seconds": 0})
    result["forward"] = {"seconds_to_catch_up": wait_caught_up(600, "after forward")}
    time.sleep(6)

    stop.set()
    final_session = session()
    pod, status = leader_status()
    result["final"] = {"feed": final_session, "leader": pod, "status": status}
    json.dump(status, open(f"{OUT}/status-final.json", "w"), indent=1)

    log("comparing the stored events with the independent reference")
    compare = subprocess.run([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "m2-p4-reference.py"),
                              "--log-url", FEED + "/admin/log", "--admin-token", os.environ["FEED_ADMIN_TOKEN"],
                              "--until-offset", str(status.get("committed_offset", 0)),
                              "--lateness-minutes", str(LATENESS_MIN),
                              "--audit-url", AGG, "--audit-token", os.environ["AUDIT_TOKEN"],
                              "--out", f"{OUT}/reference.json", "--compare-out", f"{OUT}/compare.json",
                              "--save-log", f"{OUT}/feed-log.ndjson"],
                             capture_output=True, text=True, timeout=600)
    open(f"{OUT}/compare.txt", "w").write(compare.stdout + compare.stderr)
    print(compare.stdout[-3000:])
    result["compare_exit"] = compare.returncode
    counters = status.get("cumulative", {})
    result["counts"] = {"late_detections": counters.get("late", 0), "low_confidence_ignored": counters.get("low_confidence", 0),
                        "offset_gaps": counters.get("offset_gaps", 0), "closed_fires": counters.get("closed_fires", 0),
                        "closed_singles": counters.get("closed_singles", 0), "single_detections_now": status.get("working_set", {}).get("Singles"),
                        "active_fires_now": status.get("working_set", {}).get("ActiveFires")}
    result["finished_at"] = now()
    json.dump(result, open(f"{OUT}/result.json", "w"), indent=1)
    log(f"done: compare exit {compare.returncode}, burst catch-up {recovered}, counts {result['counts']}")
    # Leave the feed running at normal speed for the live part of the demo.
    admin("/admin/speed", {"speed": 1, "duration_seconds": 0})
    return 0 if compare.returncode == 0 and recovered is not None and recovered <= 60 else 1


if __name__ == "__main__":
    sys.exit(main())
