#!/usr/bin/env python3
"""M2 P2 evidence: who polled the upstreams and who stored the results.

Input is a directory of collected logs:
  aggregator-<pod>.log / aggregator-<pod>.previous.log   (Aggregator pods)
  bmkg.log, pvmbg.log                                    (upstream mocks)

Duplicated lines (the collector overlaps its windows) are dropped. Reports:

* upstream: every data request the mocks served (not /health, /metrics),
  attributed to the Aggregator pod whose "outbound call" log carries the same
  correlation ID; the caller timeline as contiguous runs per pod; and whether
  two pods ever called inside one poll interval (should never happen).
* writes: every "poll batch persisted" line with holder and fence; the fence
  must never go backwards in time, i.e. once a newer lease term has stored a
  batch, no older term stores again.
* leadership: "leader acquired" / lost / renew-failed lines in time order.
* with --event TIME: last write of the term in force before TIME, first
  write of the next term after it, and the gap between them.

    python3 scripts/m2-p2-analyze.py artifacts/m2/p2/logs --since 2026-11-15T18:40:00Z \\
        --event 2026-11-15T18:45:00Z --out artifacts/m2/p2/failover.json
"""

from __future__ import annotations

import argparse
import collections
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

DATA_OPS = {"GET /seismic-events", "GET /tsunami-warnings", "GET /volcanic-activities", "GET /volcano-status", "GET /eruptions"}


def parse_time(value: str) -> datetime:
    value = value.replace("Z", "+00:00")
    match = re.match(r"(.*\.\d{6})\d*(\+.*)", value)
    if match:
        value = match.group(1) + match.group(2)
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def iso(moment: datetime | None) -> str | None:
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z" if moment else None


def read_lines(path: str):
    seen = set()
    with open(path, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line.startswith("{") or line in seen:
                continue
            seen.add(line)
            try:
                yield json.loads(line)
            except ValueError:
                continue


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("logs")
    parser.add_argument("--since")
    parser.add_argument("--until")
    parser.add_argument("--event", help="UTC time of the injected fault")
    parser.add_argument("--interval", type=float, default=3.0, help="poll interval in seconds")
    parser.add_argument("--label", default="p2")
    parser.add_argument("--out", default="-")
    args = parser.parse_args(argv)
    since = parse_time(args.since) if args.since else None
    until = parse_time(args.until) if args.until else None

    def in_window(moment: datetime) -> bool:
        return (since is None or moment >= since) and (until is None or moment <= until)

    caller_of: dict[str, set[str]] = collections.defaultdict(set)
    writes, leadership = [], []
    for path in sorted(glob.glob(os.path.join(args.logs, "aggregator-*.log"))):
        pod = os.path.basename(path)[len("aggregator-"):].split(".")[0]
        for entry in read_lines(path):
            message = entry.get("msg")
            if message == "outbound call" and entry.get("correlation_id"):
                caller_of[entry["correlation_id"]].add(pod)
                continue
            if "time" not in entry:
                continue
            moment = parse_time(entry["time"])
            if not in_window(moment):
                continue
            if message == "poll batch persisted":
                writes.append({"t": moment, "pod": pod, "holder": entry.get("holder"), "fence": entry.get("fence"), "source": entry.get("source"), "events": entry.get("events")})
            elif message in ("leader acquired", "leader lease lost", "leader renew failed", "leader acquire failed", "leader revalidation failed"):
                leadership.append({"t": iso(moment), "pod": pod, "msg": message, "fence": entry.get("fence"), "error": (entry.get("error") or "")[:160] or None})

    requests = []
    for mock in ("bmkg", "pvmbg"):
        path = os.path.join(args.logs, f"{mock}.log")
        if not os.path.exists(path):
            continue
        for entry in read_lines(path):
            if entry.get("msg") != "request completed" or entry.get("operation") in ("GET /health", "GET /metrics"):
                continue
            if not str(entry.get("operation", "")).startswith("GET /") or "admin" in str(entry.get("operation")):
                continue
            moment = parse_time(entry["time"])
            if not in_window(moment):
                continue
            pods = caller_of.get(entry.get("correlation_id", ""), set())
            requests.append({"t": moment, "mock": mock, "op": entry["operation"], "pod": ",".join(sorted(pods)) or None})
    requests.sort(key=lambda item: item["t"])
    writes.sort(key=lambda item: item["t"])
    leadership.sort(key=lambda item: item["t"])

    runs = []
    for request in requests:
        if request["pod"] is None:
            continue
        if runs and runs[-1]["pod"] == request["pod"]:
            runs[-1]["until"] = request["t"]
            runs[-1]["requests"] += 1
        else:
            runs.append({"pod": request["pod"], "from": request["t"], "until": request["t"], "requests": 1})
    concurrent = []
    for previous, current in zip(runs, runs[1:]):
        gap = (current["from"] - previous["until"]).total_seconds()
        if gap < args.interval * 0.5:
            concurrent.append({"from_pod": previous["pod"], "to_pod": current["pod"], "gap_seconds": round(gap, 3), "at": iso(current["from"])})
    multi = [r for r in requests if r["pod"] and "," in r["pod"]]

    regressions = []
    newest = None
    for write in writes:
        fence = write["fence"] or 0
        if newest is not None and fence < newest["fence"]:
            regressions.append({"at": iso(write["t"]), "pod": write["pod"], "fence": fence, "after_fence": newest["fence"]})
        if newest is None or fence > newest["fence"]:
            newest = {"fence": fence}
    terms = []
    for write in writes:
        if terms and terms[-1]["fence"] == write["fence"] and terms[-1]["pod"] == write["pod"]:
            terms[-1]["until"] = write["t"]
            terms[-1]["batches"] += 1
            terms[-1]["events"] += write["events"] or 0
        else:
            terms.append({"pod": write["pod"], "holder": write["holder"], "fence": write["fence"], "from": write["t"], "until": write["t"], "batches": 1, "events": write["events"] or 0})

    result = {
        "label": args.label,
        "window": [args.since, args.until],
        "upstream": {
            "data_requests": len(requests),
            "attributed": sum(1 for r in requests if r["pod"]),
            "unattributed": sum(1 for r in requests if not r["pod"]),
            "requests_attributed_to_two_pods": len(multi),
            "caller_runs": [{**run, "from": iso(run["from"]), "until": iso(run["until"])} for run in runs],
            "two_callers_within_half_interval": concurrent,
        },
        "writes": {
            "batches": len(writes),
            "terms": [{**term, "from": iso(term["from"]), "until": iso(term["until"])} for term in terms],
            "fence_regressions": regressions,
        },
        "leadership": leadership,
    }
    if args.event:
        event = parse_time(args.event)
        before = [w for w in writes if w["t"] <= event]
        after = [w for w in writes if w["t"] > event]
        old_fence = before[-1]["fence"] if before else None
        first_new = next((w for w in after if w["fence"] != old_fence), None)
        last_old = next((w for w in reversed(writes) if w["fence"] == old_fence), None) if old_fence is not None else None
        first_upstream_after = next((r for r in requests if r["t"] > event and r["pod"] and (not before or r["pod"] != before[-1]["pod"])), None)
        result["event"] = {
            "at": args.event,
            "term_before": {"fence": old_fence, "pod": before[-1]["pod"] if before else None, "last_write": iso(last_old["t"]) if last_old else None},
            "next_term": {"fence": first_new["fence"], "pod": first_new["pod"], "first_write": iso(first_new["t"])} if first_new else None,
            "first_upstream_call_by_new_pod": {"at": iso(first_upstream_after["t"]), "pod": first_upstream_after["pod"]} if first_upstream_after else None,
            "seconds_event_to_first_new_write": round((first_new["t"] - event).total_seconds(), 2) if first_new else None,
            "old_term_wrote_after_event": bool(last_old and last_old["t"] > event and first_new and last_old["t"] > first_new["t"]),
        }
    text = json.dumps(result, indent=2)
    if args.out == "-":
        print(text)
    else:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(text + "\n")
        print(f"wrote {args.out}", file=sys.stderr)
    ok = not regressions and not concurrent and not multi
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
