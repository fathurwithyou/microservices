#!/usr/bin/env python3
"""Summarise per-node write load inside the busiest window of a store-stats series.

Reads a file written by scripts/m2-store-stats.py with --samples > 1 and
reports, for the window of --seconds with the largest total raft commands
applied, each node's raft-commands-per-second and share of the total, plus
SQL inserts per second at each gateway. Prints JSON.
"""
import argparse
import json
import sys


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file")
    parser.add_argument("--seconds", type=float, default=20.0)
    args = parser.parse_args(argv)
    data = json.load(open(args.file, encoding="utf-8"))
    series = data["series"]
    pods = sorted({pod for sample in series for pod in sample["nodes"]})
    best = None
    for i, start in enumerate(series):
        end = next((s for s in series[i + 1:] if s["t"] - start["t"] >= args.seconds), None)
        if end is None:
            break
        common = [p for p in pods if p in start["nodes"] and p in end["nodes"]]
        total = sum(end["nodes"][p]["raft_commandsapplied"] - start["nodes"][p]["raft_commandsapplied"] for p in common)
        if best is None or total > best[0]:
            best = (total, start, end, common)
    if best is None:
        print("series shorter than window", file=sys.stderr)
        return 1
    total, start, end, common = best
    elapsed = end["t"] - start["t"]
    node_of = {n["pod"]: n.get("k8s_node") for n in data["nodes"]}
    rows = []
    for pod in common:
        raft = end["nodes"][pod]["raft_commandsapplied"] - start["nodes"][pod]["raft_commandsapplied"]
        inserts = (end["nodes"][pod].get("sql_insert_count") or 0) - (start["nodes"][pod].get("sql_insert_count") or 0)
        rows.append({
            "pod": pod, "k8s_node": node_of.get(pod),
            "raft_commands_per_second": round(raft / elapsed, 1),
            "share_of_raft_commands": round(raft / total, 3) if total else None,
            "sql_inserts_per_second_as_gateway": round(inserts / elapsed, 1),
            "leaseholders_at_end": end["nodes"][pod].get("replicas_leaseholders"),
        })
    rates = [r["raft_commands_per_second"] for r in rows]
    print(json.dumps({
        "source_file": args.file, "label": data.get("label"),
        "window_seconds": round(elapsed, 1), "window_offset_seconds": [start["t"], end["t"]],
        "nodes": rows,
        "max_over_mean": round(max(rates) / (sum(rates) / len(rates)), 3) if sum(rates) else None,
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
