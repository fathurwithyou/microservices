#!/usr/bin/env python3
"""M2 P3.1/P3.2/P3.4/P3.5: verify the PVMBG breaking-schema run from its own files.

Reads the files scripts/m2-p3-schema.sh wrote into --dir (parked listing,
failure detail, unprocessed audit listing, listing after replay, timeline) and
the Audit API, then reports:

* records parked while the mock was in breaking mode (window taken from the
  timeline), with reason, retryable flag and status;
* whether any listing exposes payloads (only the Ops detail may);
* what happened to each parked record after the adapter + replay;
* events stored with the adapter marker, and duplicate source_ref_ids.

    AUDIT_TOKEN=... python3 scripts/m2-p3-verify.py --dir artifacts/m2/runs/<id>/p3
"""
import argparse
import collections
import json
import os
import re
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from importlib import import_module  # noqa: E402

crossread = import_module("m2-p1-crossread")


def load(directory, name):
    with open(os.path.join(directory, name), encoding="utf-8") as handle:
        return json.load(handle)


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir")
    parser.add_argument("--brief", action="store_true", help="print the key numbers of an existing --file and exit")
    parser.add_argument("--file")
    parser.add_argument("--aggregator", default="http://localhost:18090")
    args = parser.parse_args(argv)
    if args.brief:
        d = json.load(open(args.file, encoding="utf-8"))
        print(json.dumps({k: d.get(k) for k in ("parked_in_breaking_window", "status_after_replay",
                                                 "recovered_events_with_adapter", "duplicate_source_ref_ids")}))
        return 0
    if not args.dir:
        parser.error("--dir is required")
    token = os.environ.get("AUDIT_TOKEN")
    if not token:
        print("AUDIT_TOKEN is required", file=sys.stderr)
        return 2
    timeline = open(os.path.join(args.dir, "schema-timeline.txt"), encoding="utf-8").read()
    start = re.search(r"^(\S+) PVMBG mode breaking", timeline, re.M).group(1)
    end = re.search(r"^(\S+) PVMBG mode normal", timeline, re.M).group(1)
    # Records already in flight when the mode flips back arrive a few seconds later.
    end_padded = (datetime.fromisoformat(end.replace("Z", "+00:00")) + timedelta(seconds=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    parked = load(args.dir, "ops-failures-parked.json")["items"]
    after = {item["message_id"]: item for item in load(args.dir, "ops-failures-after.json")["items"]}
    detail = load(args.dir, "ops-failure-detail.json")
    unprocessed = load(args.dir, "audit-unprocessed.json")
    window = [i for i in parked if start <= i["received_at"][:19] + "Z" <= end_padded]

    since = (datetime.fromisoformat(start.replace("Z", "+00:00")) - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    events, cursor = [], None
    while True:
        url = f"{args.aggregator}/internal/audit/events?source=PVMBG&limit=1000&since={since}" + (f"&cursor={cursor}" if cursor else "")
        status, body, _ = crossread.call(url, token=token, timeout=60)
        if status != 200:
            print(f"audit events: {status} {body}", file=sys.stderr)
            return 1
        events += body.get("items", [])
        cursor = body.get("next_cursor")
        if not cursor:
            break
    adapted = [e for e in events if (e.get("attributes") or {}).get("schema_adapter") == "pvmbg-breaking-v3"]
    refs = collections.Counter(e["source_ref_id"] for e in events)
    result = {
        "breaking_window": [start, end],
        "parked_in_breaking_window": len(window),
        "parked_reasons": dict(collections.Counter(i["reason"][:70] for i in window)),
        "parked_retryable_flags": dict(collections.Counter(str(i.get("retryable")) for i in window)),
        "parked_status": dict(collections.Counter(i["status"] for i in window)),
        "listing_exposes_payload": any("payload" in i or "payload_base64" in i for i in parked),
        "audit_unprocessed_exposes_payload": any("payload" in i or "payload_base64" in i for i in unprocessed.get("items", [])),
        "ops_detail_has_payload": "payload" in detail or "payload_base64" in detail,
        "ops_detail_keys": sorted(detail.keys()),
        "status_after_replay": dict(collections.Counter(after.get(i["message_id"], {}).get("status", "gone") for i in window)),
        "recovered_events_with_adapter": len(adapted),
        "recovered_sample": [{**{k: a[k] for k in ("source_ref_id", "severity", "occurred_at", "ingested_at")},
                              "reported_at_source": a["attributes"].get("reported_at_source")} for a in adapted[:3]],
        "pvmbg_events_since_window": len(events),
        "duplicate_source_ref_ids": sum(1 for v in refs.values() if v > 1),
    }
    with open(os.path.join(args.dir, "schema-verification.json"), "w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
