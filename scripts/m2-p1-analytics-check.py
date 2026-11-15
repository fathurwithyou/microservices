#!/usr/bin/env python3
"""M2 P1.7 evidence: dashboard analytics must equal a direct Audit API count.

1. Calls GET /analytics on the Client API over a time window.
2. Pages through every event of GET /internal/audit/events (all sources).
3. Recounts per category (TSUNAMI = SEISMIC with potential_tsunami=true or a
   warning_id), per severity, and per area_name, keeping only events whose
   occurred_at is inside the window and whose ingested_at is not after the
   analytics response, so events arriving during the audit walk are ignored.
4. Reports every difference. Exit status 0 means the numbers match.

Credentials come from the environment: FIELD_TEAM_CLIENT_SECRET, AUDIT_TOKEN.

Example:
    python3 scripts/m2-p1-analytics-check.py --out artifacts/m2/p1/analytics-vs-audit.json
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import sys
import urllib.parse
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from importlib import import_module

crossread = import_module("m2-p1-crossread")


def category(event: dict) -> str:
    kind = event["hazard_type"]
    attributes = event.get("attributes") or {}
    if kind == "SEISMIC" and (attributes.get("potential_tsunami") is True or (attributes.get("warning_id") or "") != ""):
        return "TSUNAMI"
    return kind


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--client-api", default="http://localhost:18081")
    parser.add_argument("--aggregator", default="http://localhost:18090")
    parser.add_argument("--since", default="2000-01-01T00:00:00Z")
    parser.add_argument("--until", default="2030-01-01T00:00:00Z")
    parser.add_argument("--label", default="analytics-vs-audit")
    parser.add_argument("--out", default="-")
    args = parser.parse_args(argv)

    secret, audit_token = os.environ.get("FIELD_TEAM_CLIENT_SECRET"), os.environ.get("AUDIT_TOKEN")
    if not secret or not audit_token:
        print("FIELD_TEAM_CLIENT_SECRET and AUDIT_TOKEN are required", file=sys.stderr)
        return 2
    status, body, _ = crossread.call(f"{args.client_api}/auth/token", body={"client_id": "field-team-client", "client_secret": secret})
    if status != 200:
        print(f"token request failed: {status}", file=sys.stderr)
        return 1
    token = body["access_token"]

    query = urllib.parse.urlencode({"since": args.since, "until": args.until})
    status, analytics, seconds = crossread.call(f"{args.client_api}/analytics?{query}", token=token, timeout=30)
    answered_at = datetime.now(timezone.utc)
    if status != 200:
        print(f"analytics status {status}: {analytics}", file=sys.stderr)
        return 1

    truth = crossread.audit_ground_truth(args.aggregator, audit_token, 30)
    since, until = crossread.parse_time(args.since), crossread.parse_time(args.until)
    counted = [
        event for event in truth
        if since <= crossread.parse_time(event["occurred_at"]) < until and crossread.parse_time(event["ingested_at"]) <= answered_at
    ]
    expected = {
        "hazard_types": collections.Counter(category(event) for event in counted),
        "severities": collections.Counter(event["severity"] for event in counted),
        "areas": collections.Counter(event["area_name"] for event in counted),
    }
    actual = {
        "hazard_types": {row["hazard_type"]: row["count"] for row in analytics.get("hazard_types") or []},
        "severities": {row["severity"]: row["count"] for row in analytics.get("severities") or []},
        "areas": {row["area_name"]: row["count"] for row in analytics.get("areas") or []},
    }
    differences = {}
    for group in expected:
        keys = set(expected[group]) | set(actual[group])
        diff = {key: {"analytics": actual[group].get(key, 0), "audit": expected[group].get(key, 0)} for key in sorted(keys) if actual[group].get(key, 0) != expected[group].get(key, 0)}
        if diff:
            differences[group] = diff
    timeline_total = sum(row["count"] for row in analytics.get("time_buckets") or [])
    result = {
        "label": args.label,
        "window": [args.since, args.until],
        "analytics_answered_at": answered_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "analytics_seconds": round(seconds, 3),
        "analytics_partial": analytics.get("partial"),
        "analytics_unavailable_units": analytics.get("unavailable_units"),
        "audit_events_read": len(truth),
        "audit_events_counted": len(counted),
        "analytics_total_by_category": sum(actual["hazard_types"].values()),
        "analytics_total_by_severity": sum(actual["severities"].values()),
        "analytics_total_timeline": timeline_total,
        "per_category": {"analytics": actual["hazard_types"], "audit": dict(expected["hazard_types"])},
        "per_severity": {"analytics": actual["severities"], "audit": dict(expected["severities"])},
        "areas_compared": len(set(expected["areas"]) | set(actual["areas"])),
        "differences": differences,
        "match": not differences and timeline_total == len(counted),
    }
    text = json.dumps(result, indent=2)
    if args.out == "-":
        print(text)
    else:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(text + "\n")
        print(f"wrote {args.out}", file=sys.stderr)
    return 0 if result["match"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
