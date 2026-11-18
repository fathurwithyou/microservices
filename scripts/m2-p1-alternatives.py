#!/usr/bin/env python3
"""M2 P1.2: compare the chosen partitioning with two alternatives on the same data.

For each scheme, every row is assigned to one of --nodes nodes and the per-node
share and max/mean ratio are reported, for the catalog and for the one-region
burst:

* hash of hazard_id (chosen): hazard_id = uuid5(URL, "BMKG:" + event_id), the
  same key the Aggregator stores; node = first hex digit mod N (range splits
  on the hex prefix);
* partition by region (area_name): node = sha1(region_name) mod N;
* range by time: N equal-count slices of the catalog's occurred_at history.

    python3 scripts/m2-p1-alternatives.py --catalog catalog.ndjson --burst burst.ndjson --out alternative-partitioning.json
"""
import argparse
import bisect
import collections
import hashlib
import json
import sys
import uuid


def rows(path):
    return [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()] if path else []


def stats(assignments, nodes):
    counts = collections.Counter(assignments)
    total = sum(counts.values()) or 1
    shares = {f"node-{i}": round(counts.get(i, 0) / total, 4) for i in range(nodes)}
    return {"per_node_share": shares, "max_over_mean": round(max(counts.values(), default=0) / (total / nodes), 3)}


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--burst")
    parser.add_argument("--nodes", type=int, default=4)
    parser.add_argument("--out", default="-")
    args = parser.parse_args(argv)
    catalog, burst, n = rows(args.catalog), rows(args.burst), args.nodes
    times = sorted(r["occurred_at"] for r in catalog)
    edges = [times[len(times) * i // n] for i in range(1, n)] if times else []
    schemes = {
        "hash_of_hazard_id (chosen)": lambda r: int(str(uuid.uuid5(uuid.NAMESPACE_URL, "BMKG:" + r["event_id"]))[0], 16) % n,
        "partition_by_region (area_name)": lambda r: int(hashlib.sha1(r["region_name"].encode()).hexdigest(), 16) % n,
        "range_by_time (occurred_at, equal-count slices of the catalog)": lambda r: bisect.bisect_right(edges, r["occurred_at"]),
    }
    result = {"method": f"node = owner of the row's key under each scheme, {n} nodes; same rows for every scheme",
              "catalog_rows": len(catalog), "burst_rows": len(burst)}
    for name, assign in schemes.items():
        result[name] = {"catalog": stats([assign(r) for r in catalog], n)}
        if burst:
            result[name]["burst_one_region"] = stats([assign(r) for r in burst], n)
    text = json.dumps(result, indent=2)
    if args.out != "-":
        open(args.out, "w", encoding="utf-8").write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
