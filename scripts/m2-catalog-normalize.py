#!/usr/bin/env python3
"""Normalise an earthquake catalog of unknown shape for `aggregator import-catalog`.

The assistants' reference catalog arrives only after the deadline, so its
format is unknown. `import-catalog` accepts exactly the BMKG seismic fields:

    event_id, magnitude, depth_km, epicenter_lat, epicenter_lon,
    region_name, occurred_at (RFC3339), potential_tsunami (bool)

This script reads CSV/TSV, a JSON array, an object wrapping an array
(items/data/events/records/features), NDJSON, or GeoJSON (USGS style),
optionally gzip-compressed, finds each field by common aliases (English and
Indonesian), converts values, and writes NDJSON that import-catalog accepts.

Rules (all counted in the report, nothing silently guessed):
* event_id is copied verbatim as text: the Audit API checksum is computed
  over source_ref_id, so changing it would break the comparison with the
  assistants' checksum. Rows without an id are rejected unless
  --synthesize-ids is given.
* Times without a zone use --assume-tz (default UTC). Epoch seconds and
  milliseconds are accepted. Separate date and time columns are joined.
* "6,2", "10 km", "5.2 SR", "2.31 LS", "121.5 BT" are understood.
* Missing potential_tsunami becomes false; missing region_name becomes the
  nearest seismic zone from scripts/generate-earthquake-catalog.py, and the
  row carries region_name_source=nearest_zone (kept in attributes).
* Columns that are not mapped are kept as extra fields (stored in the event
  attributes), unless --drop-extra.
* Duplicate event_id values keep the first row.

The report holds the column mapping, counts, rejections with samples, the
skew profile (rows per region and per day; P1.2), and the reference checksum
using the Audit API algorithm: SHA-256 over the sorted unique event_ids
joined by newlines, no trailing newline. Pass --expected-checksum to compare
with the value the assistants publish.

Examples:
    python3 scripts/m2-catalog-normalize.py data/katalog.csv --inspect
    python3 scripts/m2-catalog-normalize.py data/katalog.csv \\
        --out output/catalog/assistant.ndjson --report artifacts/m2/catalog-report.json
    python3 scripts/m2-catalog-normalize.py data/katalog.csv --map occurred_at=waktu_utc \\
        --assume-tz +07:00 --out output/catalog/assistant.ndjson
"""

from __future__ import annotations

import argparse
import collections
import csv
import gzip
import hashlib
import io
import json
import math
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from importlib import import_module

TARGETS = ("event_id", "magnitude", "depth_km", "epicenter_lat", "epicenter_lon",
           "region_name", "occurred_at", "potential_tsunami")

# Aliases are compared after lower-casing and removing everything that is not
# a letter or digit, so "Origin Time", "origin_time" and "originTime" match.
ALIASES = {
    "event_id": ["eventid", "id", "evid", "eid", "event", "eventcode", "kode", "kodeevent", "idgempa",
                 "gempaid", "quakeid", "earthquakeid", "sourcerefid", "ids", "code", "no", "nomor"],
    "magnitude": ["magnitude", "mag", "magnitudo", "m", "mw", "ml", "mb", "ms", "sr", "magnitudesr"],
    "depth_km": ["depthkm", "depth", "kedalaman", "kedalamankm", "dalam", "depthinkm"],
    "epicenter_lat": ["epicenterlat", "lat", "latitude", "lintang", "epilat", "y"],
    "epicenter_lon": ["epicenterlon", "lon", "lng", "long", "longitude", "bujur", "epilon", "x"],
    "region_name": ["regionname", "region", "wilayah", "area", "areaname", "place", "lokasi", "location",
                    "provinsi", "province", "keterangan", "flinnengdahl", "regionnamebmkg", "daerah"],
    "occurred_at": ["occurredat", "time", "datetime", "origintime", "timestamp", "eventtime", "waktu",
                    "waktuutc", "datetimeutc", "originat", "tanggalwaktu", "otime", "utctime"],
    "potential_tsunami": ["potentialtsunami", "tsunami", "potensi", "potensitsunami", "tsunamipotential",
                          "berpotensitsunami", "istsunami"],
}
DATE_ALIASES = ["date", "tanggal", "tgl", "originDate".lower()]
TIME_ALIASES = ["time", "jam", "waktu", "origintime", "pukul"]
WRAPPERS = ("items", "data", "events", "records", "rows", "features", "catalog", "results", "gempa")
TRUE = {"true", "t", "yes", "y", "ya", "1", "berpotensi", "berpotensitsunami", "potensitsunami", "potential"}
FALSE = {"false", "f", "no", "n", "tidak", "0", "", "none", "null", "tidakberpotensi",
         "tidakberpotensitsunami", "nan", "-"}


def key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


# ---------------------------------------------------------------- reading

def open_text(path: str) -> str:
    raw = open(path, "rb").read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("cannot decode file")


def flatten_geojson(feature: dict) -> dict:
    row = dict(feature.get("properties") or {})
    if "id" in feature and "id" not in row:
        row["id"] = feature["id"]
    coordinates = (feature.get("geometry") or {}).get("coordinates") or []
    if len(coordinates) >= 2:
        row.setdefault("longitude", coordinates[0])
        row.setdefault("latitude", coordinates[1])
    if len(coordinates) >= 3:
        row.setdefault("depth", coordinates[2])
    return row


def read_rows(path: str) -> tuple[str, list[dict]]:
    text = open_text(path)
    stripped = text.lstrip()
    if stripped.startswith("[") or stripped.startswith("{"):
        try:
            document = json.loads(text)
        except json.JSONDecodeError:
            document = None
        if document is not None:
            if isinstance(document, dict):
                for wrapper in WRAPPERS:
                    if isinstance(document.get(wrapper), list):
                        document = document[wrapper]
                        break
                else:
                    document = [document]
            rows = [flatten_geojson(item) if isinstance(item, dict) and item.get("type") == "Feature" else item
                    for item in document]
            return "json", [row for row in rows if isinstance(row, dict)]
        rows = []
        for number, line in enumerate(text.splitlines(), start=1):
            if line.strip():
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as error:
                    raise ValueError(f"line {number} is not JSON: {error}") from None
        return "ndjson", [flatten_geojson(r) if r.get("type") == "Feature" else r for r in rows]
    sample = text[:65536]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel
    return f"csv(delimiter={dialect.delimiter!r})", list(csv.DictReader(io.StringIO(text), dialect=dialect))


# ---------------------------------------------------------------- mapping

def detect_mapping(columns: list[str], overrides: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    by_key = {}
    for column in columns:
        by_key.setdefault(key(column), column)
    mapping: dict[str, str] = {}
    for target in TARGETS:
        if target in overrides:
            if overrides[target] not in columns:
                raise SystemExit(f"--map {target}={overrides[target]}: column not found; columns are {columns}")
            mapping[target] = overrides[target]
            continue
        for alias in [key(target)] + ALIASES[target]:
            if alias in by_key and by_key[alias] not in mapping.values():
                mapping[target] = by_key[alias]
                break
    # Second pass: headers with a unit or zone suffix, e.g. "Waktu (WIB)",
    # "Kedalaman (km)", "Magnitude (SR)".
    for target in TARGETS:
        if target in mapping:
            continue
        for alias in [key(target)] + ALIASES[target]:
            if len(alias) < 3:
                continue
            match = next((column for k, column in by_key.items()
                          if k.startswith(alias) and column not in mapping.values()), None)
            if match:
                mapping[target] = match
                break
    split_time = {}
    if "occurred_at" not in mapping:
        date_column = next((by_key[a] for a in DATE_ALIASES if a in by_key), None)
        time_column = next((by_key[a] for a in TIME_ALIASES if a in by_key and by_key[a] != date_column), None)
        if date_column:
            split_time = {"date": date_column, "time": time_column or ""}
    return mapping, split_time


# ---------------------------------------------------------------- values

NUMBER = re.compile(r"[-+]?\d+(?:[.,]\d+)?(?:[eE][-+]?\d+)?")


def number(value, hemisphere: bool = False) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean is not a number")
    if isinstance(value, (int, float)):
        result = float(value)
    else:
        text = str(value).strip()
        match = NUMBER.search(text)
        if not match:
            raise ValueError(f"not a number: {value!r}")
        result = float(match.group(0).replace(",", "."))
        if hemisphere and re.search(r"\b(LS|S|BB|W)\b", text.upper()) and result > 0:
            result = -result
    if math.isnan(result) or math.isinf(result):
        raise ValueError("not a finite number")
    return result


def boolean(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = key(str(value))
    if text in TRUE:
        return True
    if text in FALSE:
        return False
    if "tidak" in text or text.startswith("no"):
        return False
    if "tsunami" in text or "berpotensi" in text:
        return True
    raise ValueError(f"not a boolean: {value!r}")


def parse_zone(value: str) -> timezone:
    value = value.strip().upper()
    names = {"UTC": 0, "Z": 0, "WIB": 7, "ASIA/JAKARTA": 7, "WITA": 8, "ASIA/MAKASSAR": 8, "WIT": 9, "ASIA/JAYAPURA": 9}
    if value in names:
        return timezone(timedelta(hours=names[value]))
    match = re.fullmatch(r"([+-])(\d{1,2}):?(\d{2})?", value)
    if not match:
        raise SystemExit(f"--assume-tz {value!r}: use UTC, WIB, WITA, WIT, or an offset like +07:00")
    hours, minutes = int(match.group(2)), int(match.group(3) or 0)
    sign = -1 if match.group(1) == "-" else 1
    return timezone(sign * timedelta(hours=hours, minutes=minutes))


FORMATS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S.%f",
           "%Y/%m/%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M",
           "%d-%m-%Y %H:%M", "%Y-%m-%d", "%d/%m/%Y", "%d %b %Y %H:%M:%S", "%d %B %Y %H:%M:%S")
ZONE_SUFFIX = re.compile(r"\s*\b(UTC|GMT|WIB|WITA|WIT)\b\s*$", re.IGNORECASE)


def moment(value, assume: timezone) -> tuple[datetime, bool]:
    """Return (UTC time, zone_was_assumed)."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = value / 1000.0 if value > 1e11 else float(value)
        return datetime.fromtimestamp(seconds, timezone.utc), False
    text = str(value).strip()
    if re.fullmatch(r"\d{9,13}(\.\d+)?", text):
        return moment(float(text), assume)
    zone = None
    suffix = ZONE_SUFFIX.search(text)
    if suffix:
        zone = parse_zone(suffix.group(1).replace("GMT", "UTC"))
        text = text[: suffix.start()].strip()
    iso = text.replace(" ", "T", 1) if re.match(r"\d{4}-\d{2}-\d{2} \d", text) else text
    iso = re.sub(r"Z$", "+00:00", iso)
    iso = re.sub(r"(\.\d{6})\d+", r"\1", iso)
    try:
        parsed = datetime.fromisoformat(iso)
    except ValueError:
        parsed = None
        for fmt in FORMATS:
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            raise ValueError(f"unrecognised time: {value!r}") from None
    assumed = False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=zone or assume)
        assumed = zone is None
    return parsed.astimezone(timezone.utc), assumed


def rfc3339(value: datetime) -> str:
    if value.microsecond:
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0") + "Z"
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def id_text(value) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def blank(value) -> bool:
    return value is None or (isinstance(value, str) and value.strip() in ("", "null", "NULL", "None", "NaN", "nan"))


# ---------------------------------------------------------------- main

def nearest_zone_finder():
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    zones = import_module("generate-earthquake-catalog").ZONES

    def nearest(lat: float, lon: float) -> str:
        return min(zones, key=lambda z: (z.lat - lat) ** 2 + ((z.lon - lon) * math.cos(math.radians(lat))) ** 2).region_name
    return nearest


def digest(ids) -> str:
    return hashlib.sha256("\n".join(sorted(ids)).encode()).hexdigest()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input")
    parser.add_argument("--out", help="NDJSON output for import-catalog")
    parser.add_argument("--report", help="JSON report path (default: <out>.report.json)")
    parser.add_argument("--map", action="append", default=[], metavar="TARGET=COLUMN",
                        help=f"force a column for a target field; targets: {', '.join(TARGETS)}")
    parser.add_argument("--assume-tz", help="zone for times without one (UTC, WIB, +07:00, ...); default: the zone "
                        "named in the time column header, e.g. 'Waktu (WIB)', else UTC")
    parser.add_argument("--synthesize-ids", action="store_true",
                        help="give rows without an id a deterministic id cat-<sha1 of the row> (checksum then cannot match the assistants')")
    parser.add_argument("--drop-extra", action="store_true", help="do not carry unmapped columns into attributes")
    parser.add_argument("--expected-checksum", help="assistants' SHA-256 to compare with")
    parser.add_argument("--inspect", action="store_true", help="print the detected mapping and three converted rows, write nothing")
    args = parser.parse_args(argv)

    overrides = {}
    for item in args.map:
        target, _, column = item.partition("=")
        if target not in TARGETS or not column:
            raise SystemExit(f"--map {item!r}: expected TARGET=COLUMN with TARGET in {TARGETS}")
        overrides[target] = column

    input_sha = hashlib.sha256(open(args.input, "rb").read()).hexdigest()
    detected_format, rows = read_rows(args.input)
    if not rows:
        print("no rows found", file=sys.stderr)
        return 1
    columns = list(dict.fromkeys(column for row in rows[:1000] for column in row))
    mapping, split_time = detect_mapping(columns, overrides)
    missing = [t for t in ("magnitude", "depth_km", "epicenter_lat", "epicenter_lon") if t not in mapping]
    if "occurred_at" not in mapping and not split_time:
        missing.append("occurred_at")
    if missing:
        print(f"cannot find required fields {missing}; columns are {columns}; use --map TARGET=COLUMN",
              file=sys.stderr)
        return 2
    if "event_id" not in mapping and not args.synthesize_ids:
        print(f"no id column found in {columns}; use --map event_id=COLUMN, or --synthesize-ids "
              "(the checksum will then not match the assistants')", file=sys.stderr)
        return 2
    time_header = mapping.get("occurred_at") or split_time.get("time") or split_time.get("date") or ""
    header_zone = re.search(r"\b(UTC|GMT|WIB|WITA|WIT)\b", time_header.upper())
    if not args.assume_tz:
        args.assume_tz = header_zone.group(1).replace("GMT", "UTC") if header_zone else "UTC"
    assume = parse_zone(args.assume_tz)
    nearest = nearest_zone_finder() if "region_name" not in mapping else None
    used = set(mapping.values()) | {c for c in split_time.values() if c}

    out_rows, seen = [], set()
    rejected = collections.Counter()
    reject_samples = collections.defaultdict(list)
    applied = collections.Counter()
    duplicates = 0

    def reject(index: int, reason: str, row: dict) -> None:
        rejected[reason] += 1
        if len(reject_samples[reason]) < 5:
            reject_samples[reason].append({"row": index, "values": {k: row.get(k) for k in list(row)[:12]}})

    for index, row in enumerate(rows, start=1):
        try:
            if "event_id" in mapping and not blank(row.get(mapping["event_id"])):
                event_id = id_text(row[mapping["event_id"]])
            elif args.synthesize_ids:
                canonical = json.dumps({k: row.get(k) for k in sorted(row)}, sort_keys=True, default=str)
                event_id = "cat-" + hashlib.sha1(canonical.encode()).hexdigest()[:20]
                applied["event_id_synthesized"] += 1
            else:
                reject(index, "missing event_id", row)
                continue
            if event_id in seen:
                duplicates += 1
                continue
            values = {}
            for target in ("magnitude", "depth_km", "epicenter_lat", "epicenter_lon"):
                raw = row.get(mapping[target])
                if blank(raw):
                    raise ValueError(f"missing {target}")
                values[target] = number(raw, hemisphere=target in ("epicenter_lat", "epicenter_lon"))
            if not -90 <= values["epicenter_lat"] <= 90 or not -180 <= values["epicenter_lon"] <= 180:
                raise ValueError("coordinates out of range")
            values["depth_km"] = abs(values["depth_km"])
            if "occurred_at" in mapping:
                raw_time = row.get(mapping["occurred_at"])
            else:
                raw_time = f"{row.get(split_time['date'], '')} {row.get(split_time['time'], '') if split_time['time'] else ''}".strip()
            if blank(raw_time):
                raise ValueError("missing occurred_at")
            occurred, assumed = moment(raw_time, assume)
            if assumed:
                applied["occurred_at_zone_assumed"] += 1
            out = {"event_id": event_id, **values}
            region = row.get(mapping["region_name"]) if "region_name" in mapping else None
            if blank(region):
                out["region_name"] = (nearest or nearest_zone_finder())(values["epicenter_lat"], values["epicenter_lon"])
                out["region_name_source"] = "nearest_zone"
                applied["region_name_from_nearest_zone"] += 1
            else:
                out["region_name"] = str(region).strip()
            out["occurred_at"] = rfc3339(occurred)
            tsunami = row.get(mapping["potential_tsunami"]) if "potential_tsunami" in mapping else None
            if blank(tsunami):
                out["potential_tsunami"] = False
                applied["potential_tsunami_defaulted_false"] += 1
            else:
                out["potential_tsunami"] = boolean(tsunami)
            if not args.drop_extra:
                for column, value in row.items():
                    if column in used or column in out or blank(value) or column is None:
                        continue
                    out[column] = value
        except ValueError as error:
            reject(index, str(error).split(":")[0], row)
            continue
        seen.add(event_id)
        out_rows.append(out)

    if args.inspect:
        print(json.dumps({"format": detected_format, "rows": len(rows), "columns": columns,
                          "mapping": mapping, "split_time": split_time or None, "assume_tz": args.assume_tz,
                          "sample": out_rows[:3], "rejected": dict(rejected)}, indent=2, ensure_ascii=False, default=str))
        return 0

    ids = [row["event_id"] for row in out_rows]
    per_region = collections.Counter(row["region_name"] for row in out_rows)
    per_day = collections.Counter(row["occurred_at"][:10] for row in out_rows)
    days = sorted(per_day.values())
    reference = digest(ids)
    report = {
        "input": {"path": args.input, "sha256": input_sha, "format": detected_format, "rows": len(rows), "columns": columns},
        "mapping": mapping, "split_time": split_time or None, "assume_tz": args.assume_tz,
        "output": {"path": args.out, "rows": len(out_rows)},
        "rejected": {"total": sum(rejected.values()), "by_reason": dict(rejected), "samples": reject_samples},
        "duplicate_event_ids_skipped": duplicates,
        "defaults_applied": dict(applied),
        "reference_checksum": {"algorithm": "sha256 over sorted unique event_id joined by \\n (Audit API rule)",
                               "count": len(ids), "sha256": reference,
                               "expected": args.expected_checksum,
                               "match": None if not args.expected_checksum else args.expected_checksum.lower() == reference},
        "skew": {
            "regions": len(per_region),
            "top_regions": per_region.most_common(10),
            "bottom_regions": per_region.most_common()[:-6:-1],
            "region_max_over_mean": round(max(per_region.values()) / (len(out_rows) / len(per_region)), 3) if per_region else None,
            "days": len(per_day),
            "busiest_days": per_day.most_common(5),
            "median_rows_per_day": days[len(days) // 2] if days else None,
            "occurred_at_range": [min(r["occurred_at"] for r in out_rows), max(r["occurred_at"] for r in out_rows)] if out_rows else None,
            "magnitude_range": [min(r["magnitude"] for r in out_rows), max(r["magnitude"] for r in out_rows)] if out_rows else None,
        },
    }
    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            for row in out_rows:
                handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=str) + "\n")
    report_path = args.report or (f"{args.out}.report.json" if args.out else None)
    if report_path:
        os.makedirs(os.path.dirname(report_path) or ".", exist_ok=True)
        json.dump(report, open(report_path, "w", encoding="utf-8"), indent=2, ensure_ascii=False, default=str)
    summary = {k: report[k] for k in ("mapping", "output", "duplicate_event_ids_skipped", "defaults_applied")}
    summary["rejected"] = report["rejected"]["by_reason"]
    summary["reference_checksum"] = report["reference_checksum"]
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    if args.expected_checksum and not report["reference_checksum"]["match"]:
        print("WARNING: reference checksum differs from --expected-checksum (rows rejected or ids changed?)", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
