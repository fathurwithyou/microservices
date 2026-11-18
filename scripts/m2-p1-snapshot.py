#!/usr/bin/env python3
"""Snapshot every HazardEvent id through the Audit API (M2 P1.4 evidence).

Writes counts per source, a SHA-256 over the sorted hazard_ids, the sorted
id list itself (to diff two snapshots), and, with --catalog, the checksum of
the catalog rows (source_ref_id of BMKG rows that start with --prefix)
against the reference computed from the catalog file.

    AUDIT_TOKEN=... python3 scripts/m2-p1-snapshot.py --catalog output/catalog/earthquake-catalog-100k.ndjson \
        --out artifacts/m2/p1/snapshot-before.json
    python3 scripts/m2-p1-snapshot.py --compare artifacts/m2/p1/snapshot-before.json artifacts/m2/p1/snapshot-after.json
"""
import argparse, collections, hashlib, json, os, sys
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from importlib import import_module
crossread = import_module("m2-p1-crossread")


def digest(values):
    return hashlib.sha256("\n".join(sorted(values)).encode()).hexdigest()


def main(argv):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--aggregator", default="http://localhost:18090")
    p.add_argument("--catalog")
    p.add_argument("--label", default="snapshot")
    p.add_argument("--out")
    p.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"))
    a = p.parse_args(argv)
    if a.compare:
        before, after = (json.load(open(f)) for f in a.compare)
        lost = sorted(set(before["hazard_ids"]) - set(after["hazard_ids"]))
        print(json.dumps({"before": a.compare[0], "after": a.compare[1],
                          "before_count": before["total"], "after_count": after["total"],
                          "lost": len(lost), "lost_sample": lost[:10],
                          "gained": len(set(after["hazard_ids"]) - set(before["hazard_ids"])),
                          "catalog_before": before.get("catalog"), "catalog_after": after.get("catalog")}, indent=2))
        return 0 if not lost else 1
    token = os.environ.get("AUDIT_TOKEN")
    if not token:
        print("AUDIT_TOKEN is required", file=sys.stderr)
        return 2
    events = crossread.audit_ground_truth(a.aggregator, token, 60)
    ids = [e["hazard_id"] for e in events]
    result = {"label": a.label, "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
              "source": "GET /internal/audit/events, every page", "total": len(ids), "unique": len(set(ids)),
              "per_source": dict(collections.Counter(e["source"] for e in events)), "sha256_sorted_hazard_ids": digest(ids)}
    if a.catalog:
        reference = sorted({json.loads(line)["event_id"] for line in open(a.catalog) if line.strip()})
        wanted = set(reference)
        bmkg = [e["source_ref_id"] for e in events if e["source"] == "BMKG"]
        # The catalog shares source BMKG with live polling, so the catalog's
        # rows are the stored BMKG ids that appear in the catalog file. Their
        # checksum equals the reference only if every catalog row is stored.
        stored = sorted(set(bmkg) & wanted)
        missing = sorted(wanted - set(stored))
        result["catalog"] = {"file": a.catalog, "reference_rows": len(reference), "reference_sha256": digest(reference),
                             "stored_rows": len(stored), "stored_sha256": digest(stored), "missing": len(missing),
                             "missing_sample": missing[:10], "match": digest(reference) == digest(stored),
                             "bmkg_rows_total": len(bmkg), "bmkg_sha256_all": digest(bmkg),
                             "note": "bmkg_sha256_all equals GET /internal/audit/checksum?source=BMKG; it also covers live BMKG polling"}
    result["hazard_ids"] = sorted(ids)
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    json.dump(result, open(a.out, "w"), indent=0)
    print(json.dumps({k: v for k, v in result.items() if k != "hazard_ids"}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
