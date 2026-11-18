#!/usr/bin/env python3
"""Sample per-node CockroachDB store metrics for M2 Problem 1 evidence.

Every CockroachDB pod exposes Prometheus metrics at /_status/vars (no SQL
login needed). This script port-forwards each `app=cockroachdb` pod, reads
the store gauges and counters, and writes one JSON document:

* per node: replicas, leaseholders, ranges, live bytes, under-replicated and
  unavailable ranges, and the store's own write/query rate estimates;
* with --samples > 1: per-interval deltas of raft commands applied, SQL
  inserts, and disk write bytes, i.e. the write load each node carried while
  something (a catalog import, a burst) was running.

Numbers are copied from the metrics endpoint; nothing is estimated here
except rates, which are counter deltas divided by the elapsed seconds.

Examples:
    python3 scripts/m2-store-stats.py --out artifacts/m2/p1/store-before.json
    python3 scripts/m2-store-stats.py --samples 60 --interval 2 \\
        --label catalog-import --out artifacts/m2/p1/load-during-import.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import ssl
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

GAUGES = (
    "replicas",
    "replicas_leaseholders",
    "ranges",
    "livebytes",
    "capacity_used",
    "ranges_underreplicated",
    "ranges_unavailable",
    "rebalancing_writespersecond",
    "rebalancing_queriespersecond",
)
COUNTERS = (
    "raft_commandsapplied",
    "sql_insert_count",
    "sys_host_disk_write_bytes",
    "range_adds",
    "range_removes",
)
LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+0-9.eE]+|NaN)$')


def kubectl(args: list[str], kubeconfig: str | None) -> list[str]:
    command = ["kubectl"]
    if kubeconfig:
        command += ["--kubeconfig", kubeconfig]
    return command + args


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def pods(namespace: str, kubeconfig: str | None) -> list[dict]:
    raw = subprocess.check_output(kubectl(["-n", namespace, "get", "pods", "-l", "app=cockroachdb", "-o", "json"], kubeconfig))
    items = []
    for pod in json.loads(raw)["items"]:
        ready = any(c.get("ready") for c in pod.get("status", {}).get("containerStatuses", []))
        items.append({"pod": pod["metadata"]["name"], "k8s_node": pod["spec"].get("nodeName", ""), "ready": ready})
    return sorted(items, key=lambda item: item["pod"])


def scrape(port: int) -> dict[str, float]:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE  # cluster CA is not on this host; metrics are public
    with urllib.request.urlopen(f"https://127.0.0.1:{port}/_status/vars", context=context, timeout=10) as response:
        text = response.read().decode()
    values: dict[str, float] = {}
    for line in text.splitlines():
        match = LINE.match(line)
        if not match:
            continue
        name, labels, value = match.groups()
        if name in GAUGES or name in COUNTERS:
            # Store-level metrics carry store/node_id labels; sum over stores.
            values[name] = values.get(name, 0.0) + float(value)
        # Other metrics label peers by node_id; only the local store's own
        # replicas gauge names this node.
        if name == "replicas" and labels and 'node_id="' in labels:
            values["node_id"] = int(labels.split('node_id="', 1)[1].split('"', 1)[0])
    return values


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--namespace", default="sistem-informasi-kebencanaan-bnpb")
    parser.add_argument("--kubeconfig", default=os.environ.get("KUBECONFIG"))
    parser.add_argument("--samples", type=int, default=1)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--label", default="snapshot")
    parser.add_argument("--out", default="-")
    args = parser.parse_args(argv)

    targets = pods(args.namespace, args.kubeconfig)
    forwards = []
    try:
        for target in targets:
            if not target["ready"]:
                continue
            port = free_port()
            process = subprocess.Popen(
                kubectl(["-n", args.namespace, "port-forward", f"pod/{target['pod']}", f"{port}:8080"], args.kubeconfig),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            forwards.append((target, port, process))
        time.sleep(2.5)

        samples = []
        for index in range(args.samples):
            moment = time.time()
            nodes = []
            for target, port, _ in forwards:
                try:
                    values = scrape(port)
                    nodes.append({**target, "reachable": True, **values})
                except Exception as error:  # a pod can die mid-sample during failure tests
                    nodes.append({**target, "reachable": False, "error": str(error)})
            for target in targets:
                if not target["ready"]:
                    nodes.append({**target, "reachable": False, "error": "pod not ready"})
            samples.append({"t": moment, "nodes": nodes})
            if index + 1 < args.samples:
                time.sleep(max(0.0, args.interval - (time.time() - moment)))
    finally:
        for _, _, process in forwards:
            process.terminate()

    first, last = samples[0], samples[-1]
    elapsed = max(last["t"] - first["t"], 1e-9)
    summary = []
    totals = {"replicas": 0.0, "replicas_leaseholders": 0.0, "livebytes": 0.0}
    for node in last["nodes"]:
        if node.get("reachable"):
            for key in totals:
                totals[key] += node.get(key, 0.0)
    for node in last["nodes"]:
        row = {key: node.get(key) for key in ("pod", "k8s_node", "reachable", "node_id") if key in node}
        if not node.get("reachable"):
            row["error"] = node.get("error")
            summary.append(row)
            continue
        for key in GAUGES:
            row[key] = node.get(key)
        for key in ("replicas", "replicas_leaseholders", "livebytes"):
            if totals[key]:
                row[f"share_of_{key}"] = round(node.get(key, 0.0) / totals[key], 4)
        if len(samples) > 1:
            before = next((n for n in first["nodes"] if n["pod"] == node["pod"] and n.get("reachable")), None)
            if before:
                row["rates_per_second"] = {key: round((node.get(key, 0.0) - before.get(key, 0.0)) / elapsed, 2) for key in COUNTERS}
        summary.append(row)

    rates = [row.get("rates_per_second", {}).get("raft_commandsapplied") for row in summary if row.get("rates_per_second")]
    result = {
        "label": args.label,
        "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "CockroachDB /_status/vars per pod",
        "samples": len(samples),
        "interval_seconds": args.interval,
        "elapsed_seconds": round(elapsed, 2) if len(samples) > 1 else 0,
        "nodes": summary,
    }
    if rates and len(rates) > 1 and max(rates) > 0:
        result["write_imbalance_max_over_mean"] = round(max(rates) / (sum(rates) / len(rates)), 3)
    if args.samples > 1:
        result["series"] = [
            {"t": round(sample["t"] - first["t"], 2), "nodes": {n["pod"]: {k: n.get(k) for k in ("raft_commandsapplied", "sql_insert_count", "replicas_leaseholders", "replicas")} for n in sample["nodes"] if n.get("reachable")}}
            for sample in samples
        ]
    text = json.dumps(result, indent=2)
    if args.out == "-":
        print(text)
    else:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(text + "\n")
        print(f"wrote {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
