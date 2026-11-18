#!/usr/bin/env python3
"""M2 P2.3 evidence: every upstream BMKG event since --since is stored exactly once.

Input is the BMKG mock's own answer (fetched by scripts/m2-demo.sh from
inside an Aggregator pod) and the Audit API (every page of
/internal/audit/events?source=BMKG). Reports mock events, how many are
stored, which are missing, and duplicate source_ref_ids.

    AUDIT_TOKEN=... python3 scripts/m2-p2-gap-check.py --mock mock.json --since 2026-11-15T18:25:00Z \\
        --window-start 2026-11-15T18:38:00Z --window-end 2026-11-15T18:40:30Z --out failover-gap-check.json
"""
import argparse
import collections
import json
import os
import re
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from importlib import import_module  # noqa: E402

crossread = import_module("m2-p1-crossread")


def event_rows(document):
    if isinstance(document, dict):
        if "event_id" in document:
            yield document
        for value in document.values():
            yield from event_rows(value)
    elif isinstance(document, list):
        for value in document:
            yield from event_rows(value)


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mock", required=True)
    parser.add_argument("--aggregator", default="http://localhost:18090")
    parser.add_argument("--since", required=True)
    parser.add_argument("--window-start")
    parser.add_argument("--window-end")
    parser.add_argument("--out", default="-")
    args = parser.parse_args(argv)
    token = os.environ.get("AUDIT_TOKEN")
    if not token:
        print("AUDIT_TOKEN is required", file=sys.stderr)
        return 2
    mock = {row["event_id"]: row for row in event_rows(json.load(open(args.mock, encoding="utf-8")))}
    stored = collections.Counter()
    cursor, pages = None, 0
    while True:
        url = f"{args.aggregator}/internal/audit/events?source=BMKG&limit=1000&since={args.since}"
        if cursor:
            url += f"&cursor={cursor}"
        status, body, _ = crossread.call(url, token=token, timeout=60)
        if status != 200:
            print(f"audit events: {status} {body}", file=sys.stderr)
            return 1
        pages += 1
        for item in body.get("items", []):
            stored[item["source_ref_id"]] += 1
        cursor = body.get("next_cursor")
        if not cursor:
            break
    missing = sorted(set(mock) - set(stored))

    def parse(value):
        value = re.sub(r"(\.\d{6})\d+", r"\1", str(value).replace("Z", "+00:00"))
        return datetime.fromisoformat(value)

    def in_window(row):
        moment = row.get("occurred_at") or row.get("published_at")
        if not (moment and args.window_start and args.window_end):
            return False
        return parse(args.window_start) <= parse(moment) <= parse(args.window_end)

    result = {
        "check": f"every BMKG mock event since {args.since} is stored exactly once",
        "audit_pages": pages, "audit_bmkg_since": sum(stored.values()),
        "mock_events": len(mock),
        "mock_events_in_fault_window": sum(1 for row in mock.values() if in_window(row)) if args.window_start else None,
        "stored_matching": sum(1 for event_id in mock if event_id in stored),
        "missing": len(missing), "missing_sample": missing[:10],
        "duplicate_source_ref_ids": sum(1 for count in stored.values() if count > 1),
    }
    text = json.dumps(result, indent=2)
    if args.out == "-":
        print(text)
    else:
        open(args.out, "w", encoding="utf-8").write(text + "\n")
        print(text)
    return 0 if not missing and not result["duplicate_source_ref_ids"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
