#!/usr/bin/env python3
"""M2 P3.6 evidence: no field report answered 202 is lost or processed twice.

1. Sends --count reports with unique report_id values, --workers at a time,
   spread over every Client API entry point (--base-urls).
2. Part way through (--kill-at fraction) it force-deletes one Client API pod
   and one Aggregator pod (the receivers can die right after answering 202).
3. A client that gets no answer re-sends the same report_id, like a field
   device on a bad link; the report_id makes the submission idempotent.
4. Re-sends --resend already-accepted reports to show they are not stored
   twice.
5. Waits, then asks the Ops lookup (POST /internal/ops/field-reports/lookup)
   for every report that was answered 202: each must be stored, processed,
   with exactly one state transition. With --logs it also counts "field
   report drained" lines per report_id (the downstream effect).

Credentials from the environment: FIELD_TEAM_CLIENT_SECRET, OPS_TOKEN.
"""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import glob
import json
import os
import random
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone


def post(url: str, body: dict, token: str | None, timeout: float) -> tuple[int, dict, float]:
    data = json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read() or b"{}"), time.monotonic() - started
    except urllib.error.HTTPError as error:
        try:
            payload = json.loads(error.read() or b"{}")
        except ValueError:
            payload = {}
        return error.code, payload, time.monotonic() - started
    except Exception as error:
        return 0, {"error": str(error)}, time.monotonic() - started


def kill(namespace: str, app: str) -> str:
    pods = subprocess.check_output(["kubectl", "-n", namespace, "get", "pods", "-l", f"app={app}", "-o", "jsonpath={.items[*].metadata.name}"]).decode().split()
    victim = random.choice(pods)
    subprocess.run(["kubectl", "-n", namespace, "delete", "pod", victim, "--grace-period=0", "--force", "--wait=false"], capture_output=True)
    return victim


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-urls", required=True)
    parser.add_argument("--aggregator", default="http://localhost:18090")
    parser.add_argument("--namespace", default="sistem-informasi-kebencanaan-bnpb")
    parser.add_argument("--count", type=int, default=400)
    parser.add_argument("--workers", type=int, default=40)
    parser.add_argument("--kill-at", type=float, default=0.4)
    parser.add_argument("--resend", type=int, default=25)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--settle", type=float, default=45.0)
    parser.add_argument("--logs", help="directory of collected aggregator logs")
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)

    secret, ops_token = os.environ.get("FIELD_TEAM_CLIENT_SECRET"), os.environ.get("OPS_TOKEN")
    if not secret or not ops_token:
        print("FIELD_TEAM_CLIENT_SECRET and OPS_TOKEN are required", file=sys.stderr)
        return 2
    bases = [url.strip() for url in args.base_urls.split(",") if url.strip()]
    status, body, _ = post(f"{bases[0]}/auth/token", {"client_id": "field-team-client", "client_secret": secret}, None, args.timeout)
    if status != 200:
        print(f"token request failed: {status} {body}", file=sys.stderr)
        return 1
    token = {"value": body["access_token"]}
    token_lock = threading.Lock()

    def refresh_token() -> None:
        with token_lock:
            status, body, _ = post(f"{bases[0]}/auth/token", {"client_id": "field-team-client", "client_secret": secret}, None, args.timeout)
            if status == 200:
                token["value"] = body["access_token"]

    run = uuid.uuid4().hex[:8]
    started_at = datetime.now(timezone.utc)
    reports = [{
        "report_id": f"p3-{run}-{i:05d}", "hazard_type": random.choice(["SEISMIC", "VOLCANIC", "WILDFIRE", "COASTAL"]),
        "severity": random.choice(["NORMAL", "WASPADA", "SIAGA", "AWAS"]), "area_name": "Uji Lapangan",
        "latitude": round(random.uniform(-10, 5), 4), "longitude": round(random.uniform(95, 140), 4),
        "occurred_at": started_at.strftime("%Y-%m-%dT%H:%M:%SZ"), "description": "P3.6 evidence run",
    } for i in range(args.count)]

    attempts_log = []
    log_lock = threading.Lock()
    kills = []
    done = {"n": 0}

    def submit(report: dict) -> dict:
        tries = []
        for attempt in range(1, 8):
            base = bases[(hash(report["report_id"]) + attempt) % len(bases)]
            status, body, seconds = post(f"{base}/field-reports", report, token["value"], args.timeout)
            if status == 401:
                refresh_token()
                continue
            tries.append({"status": status, "seconds": round(seconds, 3), "error": body.get("error") if status != 202 else None})
            if status == 202:
                break
            time.sleep(0.5 * attempt)
        with log_lock:
            done["n"] += 1
            if not kills and done["n"] >= args.count * args.kill_at:
                kills.append({"at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), "client_api": kill(args.namespace, "client-api"), "aggregator": kill(args.namespace, "aggregator")})
        return {"report_id": report["report_id"], "tries": tries, "accepted": bool(tries) and tries[-1]["status"] == 202}

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(submit, reports))

    accepted = [r for r in results if r["accepted"]]
    first_try_202 = [r["tries"][0]["seconds"] for r in results if r["tries"] and r["tries"][0]["status"] == 202]
    resend_ids = [r["report_id"] for r in accepted[: args.resend]]
    resend_statuses = collections.Counter()
    for report_id in resend_ids:
        report = next(item for item in reports if item["report_id"] == report_id)
        status, _, _ = post(f"{bases[0]}/field-reports", report, token["value"], args.timeout)
        resend_statuses[status] += 1

    time.sleep(args.settle)
    stored = {}
    accepted_ids = [r["report_id"] for r in accepted]
    for offset in range(0, len(accepted_ids), 1000):
        status, body, _ = post(f"{args.aggregator}/internal/ops/field-reports/lookup", {"report_ids": accepted_ids[offset:offset + 1000]}, ops_token, 30)
        if status != 200:
            print(f"lookup failed: {status} {body}", file=sys.stderr)
            return 1
        for item in body["items"]:
            stored[item["report_id"]] = item

    drained = collections.Counter()
    if args.logs:
        seen = set()
        for path in glob.glob(os.path.join(args.logs, "aggregator-*.log")):
            for line in open(path, encoding="utf-8", errors="replace"):
                if line in seen or '"field report drained"' not in line:
                    continue
                seen.add(line)
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                if str(entry.get("report_id", "")).startswith(f"p3-{run}-"):
                    drained[entry["report_id"]] += 1

    statuses = collections.Counter(item["status"] for item in stored.values())
    result = {
        "run": run,
        "started_at": started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reports_sent": args.count,
        "answered_202": len(accepted),
        "never_answered_202": [r["report_id"] for r in results if not r["accepted"]],
        "resubmissions_after_no_answer": sum(len(r["tries"]) - 1 for r in results),
        "non_202_answers": dict(collections.Counter(t["status"] for r in results for t in r["tries"] if t["status"] != 202)),
        "first_try_202_seconds": {"p50": round(statistics.median(first_try_202), 3), "p95": round(sorted(first_try_202)[int(len(first_try_202) * 0.95) - 1], 3), "max": round(max(first_try_202), 3)} if first_try_202 else None,
        "pods_killed": kills,
        "resend_of_accepted": {"count": len(resend_ids), "statuses": dict(resend_statuses)},
        "lookup": {
            "accepted_and_stored": sum(1 for i in accepted_ids if i in stored),
            "accepted_but_missing": [i for i in accepted_ids if i not in stored],
            "status_counts": dict(statuses),
            "state_transitions_per_report": dict(collections.Counter(item["attempts"] for item in stored.values())),
        },
        "downstream_effect_log_lines": {
            "source": args.logs,
            "reports_with_one_line": sum(1 for v in drained.values() if v == 1),
            "reports_with_more_than_one": sorted(k for k, v in drained.items() if v > 1),
            "reports_without_line": sum(1 for i in accepted_ids if drained.get(i, 0) == 0),
            "note": "log lines from a force-deleted pod may be lost by the collector; the stored state above is authoritative",
        } if args.logs else None,
    }
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2)
        handle.write("\n")
    print(json.dumps({k: v for k, v in result.items() if k != "never_answered_202"}, indent=2))
    lost = result["lookup"]["accepted_but_missing"]
    twice = [k for k, v in result["lookup"]["state_transitions_per_report"].items() if k != 1]
    return 0 if not lost and not twice and statuses.get("processed", 0) == len(accepted) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
