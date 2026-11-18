#!/usr/bin/env python3
"""Generate a synthetic historical earthquake catalog for M2 Problem 1.

The output feeds `aggregator import-catalog`, so every row uses exactly the
fields read by the live BMKG seismic mapping:

    event_id, magnitude, depth_km, epicenter_lat, epicenter_lon,
    region_name, occurred_at (RFC3339 UTC), potential_tsunami

The official catalog comes from the course assistants after the deadline.
This generator stands in for it during practice runs, so it reproduces the
properties P1 tests against rather than uniform noise:

* Spatial skew: seismic zones carry realistic weights (Banda Sea, Molucca
  Sea and the Sumatra/Java trench dominate; Kalimantan is nearly quiet).
* Temporal skew: background events plus Omori-style aftershock sequences
  after large mainshocks, so some days hold thousands of rows.
* Gutenberg-Richter magnitudes (b ~ 1.0), zone-specific depth profiles, and
  potential_tsunami only for large, shallow, offshore events.

`--burst` writes a separate file for the "lonjakan gempa di satu wilayah"
criterion: many events in one zone inside a short window.

Output is deterministic for a given --seed. Standard library only.

Examples:
    python3 scripts/generate-earthquake-catalog.py
    python3 scripts/generate-earthquake-catalog.py --count 150000 --split 10000
    python3 scripts/generate-earthquake-catalog.py --burst --burst-zone laut-maluku \\
        --burst-count 5000 --burst-minutes 30
"""

from __future__ import annotations

import argparse
import bisect
import collections
import json
import math
import os
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Seismic zones
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Zone:
    key: str
    region_name: str
    lat: float
    lon: float
    lat_spread: float  # degrees, 1 sigma
    lon_spread: float
    weight: float  # relative share of background seismicity
    offshore: float  # probability an event in this zone is offshore
    # Depth mixture: (shallow, intermediate, deep) shares; must sum to 1.
    depth_mix: tuple[float, float, float]


# Weights are rough shares of Indonesian catalog activity, not exact counts.
ZONES: tuple[Zone, ...] = (
    Zone("aceh", "Aceh", 4.2, 95.8, 0.9, 0.9, 5.0, 0.55, (0.75, 0.25, 0.0)),
    Zone("sumatera-utara", "Sumatera Utara", 1.8, 98.5, 0.8, 0.8, 5.5, 0.45, (0.75, 0.25, 0.0)),
    Zone("sumatera-barat", "Sumatera Barat", -1.2, 99.6, 0.8, 0.7, 6.0, 0.55, (0.80, 0.20, 0.0)),
    Zone("mentawai", "Kepulauan Mentawai", -2.4, 99.8, 0.6, 0.6, 4.5, 0.95, (0.90, 0.10, 0.0)),
    Zone("bengkulu", "Bengkulu", -4.0, 101.6, 0.7, 0.6, 4.5, 0.65, (0.80, 0.20, 0.0)),
    Zone("lampung", "Lampung", -5.6, 104.4, 0.6, 0.7, 3.0, 0.60, (0.70, 0.30, 0.0)),
    Zone("selat-sunda", "Selat Sunda", -6.4, 105.3, 0.4, 0.4, 2.5, 0.90, (0.75, 0.25, 0.0)),
    Zone("jawa-barat", "Jawa Barat", -7.4, 107.4, 0.5, 0.9, 6.0, 0.55, (0.65, 0.35, 0.0)),
    Zone("jawa-tengah", "Jawa Tengah", -7.9, 110.0, 0.5, 0.9, 4.0, 0.55, (0.60, 0.35, 0.05)),
    Zone("jawa-timur", "Jawa Timur", -8.4, 112.8, 0.5, 1.0, 5.0, 0.60, (0.55, 0.35, 0.10)),
    Zone("bali", "Bali", -8.6, 115.2, 0.4, 0.5, 2.5, 0.60, (0.55, 0.35, 0.10)),
    Zone("ntb", "Nusa Tenggara Barat", -8.6, 117.2, 0.5, 0.9, 5.0, 0.55, (0.60, 0.30, 0.10)),
    Zone("ntt", "Nusa Tenggara Timur", -8.9, 121.8, 0.6, 1.6, 6.5, 0.60, (0.50, 0.30, 0.20)),
    Zone("laut-flores", "Laut Flores", -7.4, 121.0, 0.5, 1.4, 4.0, 1.00, (0.35, 0.25, 0.40)),
    Zone("laut-banda", "Laut Banda", -6.6, 128.8, 1.0, 2.0, 11.0, 1.00, (0.30, 0.35, 0.35)),
    Zone("maluku", "Maluku", -3.5, 128.5, 0.8, 1.3, 8.0, 0.75, (0.60, 0.35, 0.05)),
    Zone("laut-maluku", "Laut Maluku", 1.6, 126.4, 0.9, 0.7, 9.0, 1.00, (0.60, 0.35, 0.05)),
    Zone("maluku-utara", "Maluku Utara", 1.0, 127.8, 0.9, 0.7, 6.0, 0.65, (0.65, 0.30, 0.05)),
    Zone("sulawesi-utara", "Sulawesi Utara", 1.3, 124.6, 0.6, 1.0, 6.5, 0.70, (0.60, 0.35, 0.05)),
    Zone("sulawesi-tengah", "Sulawesi Tengah", -1.0, 120.6, 0.9, 0.9, 4.5, 0.35, (0.90, 0.10, 0.0)),
    Zone("sulawesi-selatan", "Sulawesi Selatan", -4.0, 120.0, 0.9, 0.6, 1.5, 0.35, (0.90, 0.10, 0.0)),
    Zone("sulawesi-barat", "Sulawesi Barat", -2.6, 118.9, 0.6, 0.4, 2.0, 0.40, (0.90, 0.10, 0.0)),
    Zone("papua-barat", "Papua Barat", -1.6, 133.2, 0.8, 1.2, 4.5, 0.45, (0.85, 0.15, 0.0)),
    Zone("papua", "Papua", -3.2, 138.4, 0.9, 2.0, 6.5, 0.35, (0.85, 0.15, 0.0)),
    Zone("laut-sulawesi", "Laut Sulawesi", 3.2, 122.6, 0.7, 1.2, 2.0, 1.00, (0.50, 0.40, 0.10)),
    Zone("samudra-hindia-jawa", "Samudra Hindia Selatan Jawa", -10.2, 110.5, 0.6, 3.5, 5.5, 1.00, (0.95, 0.05, 0.0)),
    Zone("kalimantan", "Kalimantan Timur", 0.8, 117.0, 1.2, 1.0, 0.4, 0.15, (1.0, 0.0, 0.0)),
)

ZONE_BY_KEY = {zone.key: zone for zone in ZONES}

# ---------------------------------------------------------------------------
# Distributions
# ---------------------------------------------------------------------------

M_MIN = 2.5
M_MAX = 8.6
# Gutenberg-Richter b-value. Indonesian subduction zones sit around 0.8-1.0;
# 0.9 keeps enough M7+ shallow offshore rows for the TSUNAMI dashboard
# category. Overridden by --b-value.
B_VALUE = 0.9
MAINSHOCK_MAGNITUDE = 6.0  # threshold that triggers an aftershock sequence


def gutenberg_richter(rng: random.Random, m_min: float = M_MIN, m_max: float = M_MAX) -> float:
    """Truncated Gutenberg-Richter magnitude, rounded to 0.1 like BMKG."""
    beta = B_VALUE * math.log(10)
    span = 1.0 - math.exp(-beta * (m_max - m_min))
    value = m_min - math.log(1.0 - rng.random() * span) / beta
    return round(min(value, m_max), 1)


def sample_depth(rng: random.Random, zone: Zone) -> float:
    shallow, intermediate, _ = zone.depth_mix
    roll = rng.random()
    if roll < shallow:
        depth = min(rng.expovariate(1 / 18.0) + 2.0, 70.0)
    elif roll < shallow + intermediate:
        depth = rng.uniform(70.0, 300.0)
    else:
        depth = rng.uniform(300.0, 650.0)
    return round(depth, 1)


def sample_point(
    rng: random.Random, zone: Zone, tighten: float = 1.0, center: tuple[float, float] | None = None
) -> tuple[float, float]:
    center_lat, center_lon = center if center else (zone.lat, zone.lon)
    lat = rng.gauss(center_lat, zone.lat_spread * tighten)
    lon = rng.gauss(center_lon, zone.lon_spread * tighten)
    # Keep inside the Indonesian catalog bounding box.
    lat = max(-11.5, min(6.5, lat))
    lon = max(94.0, min(141.5, lon))
    return round(lat, 4), round(lon, 4)


def tsunami_potential(rng: random.Random, zone: Zone, magnitude: float, depth: float) -> bool:
    if depth > 70.0 or rng.random() >= zone.offshore:
        return False
    if magnitude >= 7.0:
        return True
    # BMKG occasionally flags shallow offshore M6.5+ as potential.
    return magnitude >= 6.5 and rng.random() < 0.3


def omori_delay(rng: random.Random, c_hours: float = 0.05, p: float = 1.1, horizon_days: float = 60.0) -> timedelta:
    """Delay after a mainshock drawn from the modified Omori law."""
    horizon = horizon_days * 24.0
    # Inverse CDF of (t + c)^-p on [0, horizon].
    a = c_hours ** (1 - p)
    b = (horizon + c_hours) ** (1 - p)
    u = rng.random()
    hours = (a - u * (a - b)) ** (1 / (1 - p)) - c_hours
    return timedelta(hours=max(hours, 0.0))


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------


def rfc3339(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_row(
    rng: random.Random,
    zone: Zone,
    moment: datetime,
    magnitude: float,
    tighten: float = 1.0,
    center: tuple[float, float] | None = None,
) -> dict:
    depth = sample_depth(rng, zone)
    lat, lon = sample_point(rng, zone, tighten, center)
    return {
        "magnitude": magnitude,
        "depth_km": depth,
        "epicenter_lat": lat,
        "epicenter_lon": lon,
        "region_name": zone.region_name,
        "occurred_at": moment,
        "potential_tsunami": tsunami_potential(rng, zone, magnitude, depth),
        "_zone": zone.key,
    }


def generate_catalog(count: int, start: datetime, end: datetime, seed: int, aftershock_share: float) -> list[dict]:
    rng = random.Random(seed)
    span = (end - start).total_seconds()
    cumulative = []
    total = 0.0
    for zone in ZONES:
        total += zone.weight
        cumulative.append(total)

    def pick_zone() -> Zone:
        return ZONES[bisect.bisect_left(cumulative, rng.random() * total)]

    background_target = int(count * (1.0 - aftershock_share))
    rows: list[dict] = []
    mainshocks: list[dict] = []
    for _ in range(background_target):
        zone = pick_zone()
        moment = start + timedelta(seconds=rng.random() * span)
        row = make_row(rng, zone, moment, gutenberg_richter(rng))
        rows.append(row)
        if row["magnitude"] >= MAINSHOCK_MAGNITUDE:
            mainshocks.append(row)

    # Distribute the aftershock budget across mainshocks by 10^(b*M), so a
    # single M8 sequence dwarfs dozens of M6 ones (Bath/Utsu productivity).
    remaining = count - len(rows)
    if mainshocks and remaining > 0:
        productivity = [10 ** (B_VALUE * (shock["magnitude"] - MAINSHOCK_MAGNITUDE)) for shock in mainshocks]
        scale = remaining / sum(productivity)
        budgets = [int(p * scale) for p in productivity]
        for index in range(remaining - sum(budgets)):
            budgets[index % len(budgets)] += 1
        for shock, budget in zip(mainshocks, budgets):
            zone = ZONE_BY_KEY[shock["_zone"]]
            # Bath's law: the largest aftershock is ~1.2 below the mainshock.
            m_cap = max(M_MIN + 0.1, shock["magnitude"] - 1.2)
            epicenter = (shock["epicenter_lat"], shock["epicenter_lon"])
            for _ in range(budget):
                moment = shock["occurred_at"] + omori_delay(rng)
                if moment > end:
                    moment = end - timedelta(seconds=rng.random() * 3600)
                magnitude = gutenberg_richter(rng, M_MIN, m_cap)
                rows.append(make_row(rng, zone, moment, magnitude, tighten=0.15, center=epicenter))
    else:
        for _ in range(remaining):
            zone = pick_zone()
            moment = start + timedelta(seconds=rng.random() * span)
            rows.append(make_row(rng, zone, moment, gutenberg_richter(rng)))

    rows.sort(key=lambda row: row["occurred_at"])
    for index, row in enumerate(rows, start=1):
        row["event_id"] = f"hist-{row['occurred_at']:%Y%m%d}-{index:07d}"
    return rows


def generate_burst(zone: Zone, count: int, start: datetime, minutes: float, seed: int) -> list[dict]:
    rng = random.Random(seed)
    mainshock_mag = 7.2
    rows = [make_row(rng, zone, start, mainshock_mag, tighten=0.15)]
    window = minutes * 60.0
    for _ in range(count - 1):
        moment = start + timedelta(seconds=rng.random() * window)
        rows.append(make_row(rng, zone, moment, gutenberg_richter(rng, M_MIN, mainshock_mag - 1.0), tighten=0.15))
    rows.sort(key=lambda row: row["occurred_at"])
    for index, row in enumerate(rows, start=1):
        row["event_id"] = f"burst-{zone.key}-{start:%Y%m%d%H%M}-{index:06d}"
    return rows


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

FIELD_ORDER = (
    "event_id",
    "magnitude",
    "depth_km",
    "epicenter_lat",
    "epicenter_lon",
    "region_name",
    "occurred_at",
    "potential_tsunami",
)


def serialise(row: dict) -> dict:
    out = {key: row[key] for key in FIELD_ORDER}
    out["occurred_at"] = rfc3339(row["occurred_at"])
    return out


def write_rows(rows: list[dict], path: str, fmt: str) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        if fmt == "array":
            json.dump([serialise(row) for row in rows], handle, ensure_ascii=False, separators=(",", ":"))
            handle.write("\n")
        else:
            for row in rows:
                handle.write(json.dumps(serialise(row), ensure_ascii=False, separators=(",", ":")))
                handle.write("\n")


def write_output(rows: list[dict], out: str, fmt: str, split: int) -> list[str]:
    if split <= 0 or split >= len(rows):
        write_rows(rows, out, fmt)
        return [out]
    stem, ext = os.path.splitext(out)
    paths = []
    for part, offset in enumerate(range(0, len(rows), split), start=1):
        path = f"{stem}.part{part:03d}{ext}"
        write_rows(rows[offset : offset + split], path, fmt)
        paths.append(path)
    return paths


def summarise(rows: list[dict], paths: list[str]) -> dict:
    per_zone = collections.Counter(row["region_name"] for row in rows)
    per_year = collections.Counter(row["occurred_at"].year for row in rows)
    per_day = collections.Counter(row["occurred_at"].date() for row in rows)
    magnitude_bands = collections.Counter(
        f"M{int(row['magnitude'])}" if row["magnitude"] < 8 else "M8+" for row in rows
    )
    busiest_day, busiest_count = per_day.most_common(1)[0]
    return {
        "rows": len(rows),
        "files": paths,
        "first_occurred_at": rfc3339(rows[0]["occurred_at"]),
        "last_occurred_at": rfc3339(rows[-1]["occurred_at"]),
        "potential_tsunami": sum(1 for row in rows if row["potential_tsunami"]),
        "unique_event_ids": len({row["event_id"] for row in rows}),
        "busiest_day": {"date": busiest_day.isoformat(), "rows": busiest_count},
        "median_rows_per_active_day": sorted(per_day.values())[len(per_day) // 2],
        "per_region": dict(per_zone.most_common()),
        "per_year": dict(sorted(per_year.items())),
        "magnitude_bands": dict(sorted(magnitude_bands.items())),
    }


def parse_time(value: str) -> datetime:
    moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def main(argv: list[str]) -> int:
    global B_VALUE
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--count", type=int, default=100_000, help="rows in the catalog (default 100000)")
    parser.add_argument("--seed", type=int, default=4031, help="RNG seed; same seed gives the same file")
    parser.add_argument("--start", default="2008-01-01T00:00:00Z", help="first possible occurred_at (UTC)")
    parser.add_argument("--end", default="2026-11-01T00:00:00Z", help="last possible occurred_at (UTC)")
    parser.add_argument("--aftershock-share", type=float, default=0.35, help="fraction of rows that are aftershocks (0-0.9)")
    parser.add_argument("--b-value", type=float, default=B_VALUE, help="Gutenberg-Richter b-value (default 0.9)")
    parser.add_argument("--out", default="output/catalog/earthquake-catalog-100k.ndjson", help="output path")
    parser.add_argument("--format", choices=("ndjson", "array"), default="ndjson", help="NDJSON or one JSON array")
    parser.add_argument("--split", type=int, default=0, help="write parts of N rows each (keeps each import transaction small)")
    parser.add_argument("--burst", action="store_true", help="write a one-zone burst file instead of the catalog")
    parser.add_argument("--burst-zone", default="laut-maluku", choices=sorted(ZONE_BY_KEY), help="zone for --burst")
    parser.add_argument("--burst-count", type=int, default=5_000, help="rows in the burst")
    parser.add_argument("--burst-minutes", type=float, default=30.0, help="burst window length")
    parser.add_argument("--burst-start", default=None, help="burst start (UTC); default: now rounded to the minute")
    parser.add_argument("--summary", default=None, help="also write the summary JSON to this path")
    args = parser.parse_args(argv)
    if not 0.5 <= args.b_value <= 1.5:
        parser.error("--b-value must be between 0.5 and 1.5")
    B_VALUE = args.b_value

    if args.burst:
        if args.burst_count < 1:
            parser.error("--burst-count must be >= 1")
        start = parse_time(args.burst_start) if args.burst_start else datetime.now(timezone.utc).replace(second=0, microsecond=0)
        zone = ZONE_BY_KEY[args.burst_zone]
        rows = generate_burst(zone, args.burst_count, start, args.burst_minutes, args.seed)
        out = args.out
        if out == parser.get_default("out"):
            out = f"output/catalog/burst-{zone.key}-{start:%Y%m%dT%H%MZ}.ndjson"
    else:
        if args.count < 1:
            parser.error("--count must be >= 1")
        if not 0.0 <= args.aftershock_share <= 0.9:
            parser.error("--aftershock-share must be between 0 and 0.9")
        start, end = parse_time(args.start), parse_time(args.end)
        if end <= start:
            parser.error("--end must be after --start")
        rows = generate_catalog(args.count, start, end, args.seed, args.aftershock_share)
        out = args.out

    paths = write_output(rows, out, args.format, args.split)
    summary = summarise(rows, paths)
    if summary["unique_event_ids"] != summary["rows"]:
        print("event_id collision; refusing to continue", file=sys.stderr)
        return 1
    text = json.dumps(summary, indent=2, ensure_ascii=False)
    if args.summary:
        os.makedirs(os.path.dirname(args.summary) or ".", exist_ok=True)
        with open(args.summary, "w", encoding="utf-8") as handle:
            handle.write(text + "\n")
    print(text, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
