#!/usr/bin/env python3
"""M2 P1.3/P1.4 evidence: read "latest N events across every hazard type".

Walks GET /hazards on the Client API page by page (page_size, page_token)
and checks that

* every page answers 200 (never 5xx, never hangs past --timeout);
* the concatenated pages are strictly ordered by occurred_at desc, then
  hazard_id desc, with no duplicates;
* each page's `partial`/`unavailable_units` flags are recorded, so a run
  during a node failure shows explicitly-marked incomplete answers.

With --audit-token it also builds ground truth from the Aggregator Audit API
(every event, all sources) and checks that no event older than the run start
that falls inside the covered time window is missing from the pages.

Credentials are read from the environment, never from arguments:
  FIELD_TEAM_CLIENT_SECRET (required), AUDIT_TOKEN (optional).

Example:
    python3 scripts/m2-p1-crossread.py --pages 20 --out artifacts/m2/p1/crossread.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone


def call(url: str, *, token: str | None = None, body: dict | None = None, timeout: float = 10.0) -> tuple[int, dict, float]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method="POST" if body is not None else "GET")
    request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read() or b"{}")
            return response.status, payload, time.monotonic() - started
    except urllib.error.HTTPError as error:
        try:
            payload = json.loads(error.read() or b"{}")
        except ValueError:
            payload = {}
        return error.code, payload, time.monotonic() - started
    except Exception as error:  # timeout, connection reset
        return 0, {"error": str(error)}, time.monotonic() - started


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def sort_key(item: dict) -> tuple:
    return (parse_time(item["occurred_at"]), item["hazard_id"])


def audit_ground_truth(base: str, token: str, timeout: float, as_of: str | None = None) -> list[dict]:
    """Page through every Audit API event; with as_of, from that store snapshot."""
    events, cursor = [], None
    while True:
        query = {"limit": "1000"}
        if as_of:
            query["as_of"] = as_of
        if cursor:
            query["cursor"] = cursor
        status, body, _ = call(f"{base}/internal/audit/events?{urllib.parse.urlencode(query)}", token=token, timeout=timeout)
        if status != 200:
            raise RuntimeError(f"audit events status {status}: {body}")
        events.extend(body.get("items", []))
        cursor = body.get("next_cursor")
        if not cursor:
            return events


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--client-api", default="http://localhost:18081")
    parser.add_argument("--aggregator", default="http://localhost:18090")
    parser.add_argument("--page-size", type=int, default=50)
    parser.add_argument("--pages", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--label", default="crossread")
    parser.add_argument("--out", default="-")
    args = parser.parse_args(argv)

    secret = os.environ.get("FIELD_TEAM_CLIENT_SECRET")
    if not secret:
        print("FIELD_TEAM_CLIENT_SECRET is required", file=sys.stderr)
        return 2
    status, body, _ = call(f"{args.client_api}/auth/token", body={"client_id": "field-team-client", "client_secret": secret}, timeout=args.timeout)
    if status != 200:
        print(f"token request failed: {status} {body}", file=sys.stderr)
        return 1
    token = body["access_token"]

    run_started = datetime.now(timezone.utc)
    pages, items, page_token = [], [], ""
    for number in range(1, args.pages + 1):
        query = {"page_size": str(args.page_size)}
        if page_token:
            query["page_token"] = page_token
        status, body, seconds = call(f"{args.client_api}/hazards?{urllib.parse.urlencode(query)}", token=token, timeout=args.timeout)
        hazards = body.get("hazards") or []
        pages.append({
            "page": number, "status": status, "seconds": round(seconds, 3), "items": len(hazards),
            "partial": body.get("partial"), "unavailable_units": body.get("unavailable_units"),
            "first": hazards[0]["occurred_at"] if hazards else None, "last": hazards[-1]["occurred_at"] if hazards else None,
            "error": body.get("error") if status != 200 else None,
        })
        items.extend(hazards)
        page_token = body.get("next_page_token") or ""
        if status != 200 or not page_token:
            break

    ids = [item["hazard_id"] for item in items]
    keys = [sort_key(item) for item in items]
    ordered = all(keys[i] > keys[i + 1] for i in range(len(keys) - 1))
    result = {
        "label": args.label,
        "run_started_at": run_started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "request": f"GET /hazards?page_size={args.page_size}&page_token=...",
        "pages_requested": args.pages,
        "pages": pages,
        "items": len(items),
        "duplicates": len(ids) - len(set(ids)),
        "strictly_ordered_occurred_at_desc_hazard_id_desc": ordered,
        "all_pages_200": all(page["status"] == 200 for page in pages),
        "any_partial": any(page["partial"] for page in pages),
        "max_page_seconds": max((page["seconds"] for page in pages), default=None),
        "hazard_types_seen": sorted({item.get("hazard_type", "") for item in items}),
        "sources_seen": sorted({item.get("source", "") for item in items}),
    }

    audit_token = os.environ.get("AUDIT_TOKEN")
    if audit_token and items:
        truth = audit_ground_truth(args.aggregator, audit_token, args.timeout * 3)
        newest, oldest = keys[0][0], keys[-1][0]
        # Events ingested after the run started may legitimately be absent.
        expected = {
            event["hazard_id"] for event in truth
            if oldest < parse_time(event["occurred_at"]) <= newest and parse_time(event["ingested_at"]) < run_started
        }
        missing = sorted(expected - set(ids))
        result["ground_truth"] = {
            "source": "GET /internal/audit/events (all sources, every page)",
            "audit_events_total": len(truth),
            "window": [oldest.strftime("%Y-%m-%dT%H:%M:%SZ"), newest.strftime("%Y-%m-%dT%H:%M:%SZ")],
            "expected_in_window": len(expected),
            "missing_from_pages": len(missing),
            "missing_sample": missing[:10],
        }

    text = json.dumps(result, indent=2)
    if args.out == "-":
        print(text)
    else:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(text + "\n")
        print(f"wrote {args.out}", file=sys.stderr)
    ok = result["all_pages_200"] and ordered and result["duplicates"] == 0 and result.get("ground_truth", {}).get("missing_from_pages", 0) == 0
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
