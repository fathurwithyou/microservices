#!/usr/bin/env python3
"""M2 Problem 4 reference calculator: "Definisi Kebakaran" straight from the
assignment PDF, independent of the ingestor.

Rules (PDF pages 7-9):
  1. a detection is identified by satellite|acq_date|acq_time|latitude|longitude
     (the feed's own tokens); a repeated detection counts once
  2. detection time = acq_date + acq_time (HHMM, not zero padded) in UTC
  3. confidence low is left out
  4. two detections are linked when distance <= 1.0 km AND |dt| <= 360 min;
     linking is transitive
  5. a linked set of >= 2 detections is one fire; singles are only counted
  severity: AWAS when >= 20 detections or total FRP >= 100 MW; SIAGA when
  >= 6 detections or total FRP >= 30 MW; otherwise WASPADA.

Inputs (one of):
  --log-url http://host:8088/admin/log --admin-token T   mock feed admin log
  --log-file events.ndjson                               same NDJSON shape
  --stream-url https://satellite.naufarrel.dev --token T reads SSE from offset 0
       (needs --until-offset and/or --seconds; --save-log keeps the capture)

Compare mode: --audit-url http://localhost:18090 --audit-token T
  exit 0 when the system matches the reference exactly, 1 otherwise.
  With --lateness-minutes L the verdict uses a second reference that applies
  the watermark policy, so differences caused by the declared lateness bound
  are separated from real bugs.

  python3 scripts/m2-p4-reference.py --self-test
"""
import argparse
import calendar
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

EARTH_RADIUS_KM = 6371.0088
LINK_KM = 1.0
LINK_MINUTES = 360
CONFIDENCE = {"l": 0, "low": 0, "n": 1, "nominal": 1, "h": 2, "high": 2}
KEY_FIELDS = ("satellite", "acq_date", "acq_time", "latitude", "longitude")


class Tok(str):
    """A JSON number kept as its lexical token (so -2.51230 keeps its zeros)."""


def loads_raw(text):
    return json.loads(text, parse_float=Tok, parse_int=Tok)


# ---------------------------------------------------------------- reference --

def key_of(data):
    if "key" in data and "satellite" not in data:
        return str(data["key"])
    return "|".join(str(data[f]) for f in KEY_FIELDS)


def acq_minutes(data):
    """Detection time as whole minutes since the Unix epoch (UTC)."""
    hhmm = str(data["acq_time"]).strip().replace(":", "").zfill(4)
    day = time.strptime(str(data["acq_date"]).strip(), "%Y-%m-%d")
    return calendar.timegm(day) // 60 + int(hhmm[:-2]) * 60 + int(hhmm[-2:])


def iso_minutes(minutes):
    return datetime.fromtimestamp(minutes * 60, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def apply_events(events, until=None, lateness=None):
    """Replays events in offset order. Returns (live detections by key, counters).

    lateness (minutes): a detection/update whose acquisition time is more than
    that far behind the maximum acquisition time seen so far is not admitted.
    Retracts always apply.
    """
    live = {}
    c = {"events": 0, "detection": 0, "update": 0, "retract": 0, "duplicate_detections": 0,
         "retract_unknown_key": 0, "not_admitted_events": 0, "last_offset": 0}
    refused = set()
    watermark = None
    for ev in sorted(events, key=lambda e: e["offset"]):
        if until is not None and ev["offset"] > until:
            break
        kind, data = ev["event"], ev["data"]
        if kind not in ("detection", "update", "retract"):
            continue
        c["events"] += 1
        c[kind] += 1
        c["last_offset"] = ev["offset"]
        key = key_of(data)
        if kind == "retract":
            if live.pop(key, None) is None:
                c["retract_unknown_key"] += 1
            continue
        minutes = acq_minutes(data)
        late = lateness is not None and watermark is not None and watermark - minutes > lateness
        if watermark is None or minutes > watermark:
            watermark = minutes
        if late:
            c["not_admitted_events"] += 1
            refused.add(key)
            continue
        if kind == "detection" and key in live:
            c["duplicate_detections"] += 1
        conf = str(data.get("confidence", "")).strip().lower()
        if conf not in CONFIDENCE:
            raise ValueError("offset %d: unknown confidence %r" % (ev["offset"], data.get("confidence")))
        live[key] = {
            "key": key, "satellite": str(data["satellite"]), "minutes": minutes,
            "lat": float(data["latitude"]), "lon": float(data["longitude"]),
            "confidence": CONFIDENCE[conf], "frp": Decimal(str(data.get("frp", "0"))),
        }
    c["not_admitted_keys"] = len(refused)
    c["not_admitted_keys_never_live"] = len([k for k in refused if k not in live])
    return live, c


def haversine_km(a, b):
    p1, p2 = math.radians(a["lat"]), math.radians(b["lat"])
    dp, dl = p2 - p1, math.radians(b["lon"] - a["lon"])
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(h)))


def severity_of(count, total_frp):
    if count >= 20 or total_frp >= 100:
        return "AWAS"
    if count >= 6 or total_frp >= 30:
        return "SIAGA"
    return "WASPADA"


def compute(live, counters=None):
    """Groups live detections into fires and builds the reference document."""
    points = [d for d in live.values() if d["confidence"] > 0]
    parent = list(range(len(points)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    # Spatial buckets a little larger than 1 km in both directions; every
    # candidate pair is still decided by the exact rule.
    max_lat = max([abs(p["lat"]) for p in points] + [0.0])
    cell_lat = 0.0091
    cell_lon = cell_lat / max(math.cos(math.radians(min(max_lat + 0.1, 89.0))), 1e-6)
    buckets = {}
    for i, p in enumerate(points):
        buckets.setdefault((math.floor(p["lat"] / cell_lat), math.floor(p["lon"] / cell_lon)), []).append(i)
    for (ca, cb), members in buckets.items():
        for da in (-1, 0, 1):
            for db in (-1, 0, 1):
                for j in buckets.get((ca + da, cb + db), ()):
                    for i in members:
                        if i < j and abs(points[i]["minutes"] - points[j]["minutes"]) <= LINK_MINUTES \
                                and haversine_km(points[i], points[j]) <= LINK_KM:
                            ri, rj = find(i), find(j)
                            if ri != rj:
                                parent[ri] = rj
    groups = {}
    for i in range(len(points)):
        groups.setdefault(find(i), []).append(points[i])

    fires, singles = [], 0
    for members in groups.values():
        if len(members) < 2:
            singles += 1
            continue
        total = sum((m["frp"] for m in members), Decimal(0))
        fires.append({
            "detection_keys": sorted(m["key"] for m in members),
            "detection_count": len(members),
            "satellites": sorted({m["satellite"] for m in members}),
            "max_confidence": "lnh"[max(m["confidence"] for m in members)],
            "total_frp_mw": float(total),
            "latitude": round(sum(m["lat"] for m in members) / len(members), 6),
            "longitude": round(sum(m["lon"] for m in members) / len(members), 6),
            "occurred_at": iso_minutes(min(m["minutes"] for m in members)),
            "severity": severity_of(len(members), total),
        })
    fires.sort(key=lambda f: (f["occurred_at"], f["detection_keys"][0]))
    by_severity = {s: len([f for f in fires if f["severity"] == s]) for s in ("WASPADA", "SIAGA", "AWAS")}
    return {
        "until_offset": (counters or {}).get("last_offset", 0),
        "detections_live": len(live),
        "low_confidence_ignored": len(live) - len(points),
        "singles": singles,
        "fire_count": len(fires),
        "fires_by_severity": by_severity,
        "multi_satellite_fires": len([f for f in fires if len(f["satellites"]) > 1]),
        "events": counters or {},
        "fires": fires,
    }


def reference(events, until=None, lateness=None):
    live, counters = apply_events(events, until, lateness)
    return compute(live, counters)


# -------------------------------------------------------------------- input --

def parse_log_line(line):
    rec = loads_raw(line)
    return {"offset": int(rec["offset"]), "event": str(rec["event"]), "data": rec["data"]}


def http_get(url, headers, timeout=30):
    req = urllib.request.Request(url, headers=headers)
    return urllib.request.urlopen(req, timeout=timeout)


def load_log_url(url, admin_token, until):
    events, nxt = [], 1
    while until is None or nxt <= until:
        sep = "&" if "?" in url else "?"
        with http_get("%s%sfrom=%d&limit=5000" % (url, sep, nxt), {"X-Admin-Token": admin_token or ""}) as resp:
            lines = [ln for ln in resp.read().decode("utf-8").splitlines() if ln.strip()]
        if not lines:
            break
        events.extend(parse_log_line(ln) for ln in lines)
        nxt = events[-1]["offset"] + 1
    return events


def load_log_file(path):
    with open(path, encoding="utf-8") as fh:
        return [parse_log_line(ln) for ln in fh if ln.strip()]


def read_stream(base, token, until, seconds, save_path):
    """Reads the SSE stream from the beginning. Reconnects with Last-Event-ID
    and drops anything already seen, so the capture is gapless or it says so."""
    base = base.rstrip("/")
    auth = {"Authorization": "Bearer " + (token or ""), "Accept": "text/event-stream"}
    try:
        with http_get(base + "/v1/session", auth) as resp:
            print("session: " + resp.read().decode("utf-8").strip(), file=sys.stderr)
    except urllib.error.HTTPError as err:
        sys.exit("feed refused /v1/session: HTTP %d %s" % (err.code, err.read().decode("utf-8", "replace").strip()))
    deadline = time.time() + seconds if seconds else None
    events, last, failures = [], 0, 0
    save = open(save_path, "w", encoding="utf-8") if save_path else None

    def finished():
        return (until is not None and last >= until) or (deadline is not None and time.time() >= deadline)

    try:
        while not finished():
            headers = dict(auth)
            if last:
                headers["Last-Event-ID"] = str(last)
            try:
                resp = http_get(base + "/v1/stream" + ("" if last else "?from=0"), headers, timeout=45)
            except urllib.error.HTTPError as err:
                body = err.read().decode("utf-8", "replace").strip()
                failures += 1
                if err.code in (400, 401) or failures > 10:
                    sys.exit("feed refused /v1/stream: HTTP %d %s" % (err.code, body))
                print("stream: HTTP %d %s; retrying" % (err.code, body), file=sys.stderr)
                time.sleep(min(2 * failures, 15))
                continue
            except OSError as err:
                failures += 1
                if failures > 10:
                    sys.exit("feed unreachable: %s" % err)
                time.sleep(min(2 * failures, 15))
                continue
            event_id = event_name = None
            data_lines = []
            try:
                with resp:
                    for raw in resp:
                        line = raw.decode("utf-8").rstrip("\r\n")
                        if line:
                            name, _, value = line.partition(":")
                            value = value[1:] if value.startswith(" ") else value
                            if name == "id":
                                event_id = value
                            elif name == "event":
                                event_name = value
                            elif name == "data":
                                data_lines.append(value)
                            continue
                        if event_name in ("detection", "update", "retract") and event_id is not None:
                            offset, text = int(event_id), "\n".join(data_lines)
                            if offset > last:
                                if offset != last + 1:
                                    print("GAP: offset %d follows %d" % (offset, last), file=sys.stderr)
                                events.append({"offset": offset, "event": event_name, "data": loads_raw(text)})
                                last, failures = offset, 0
                                if save:
                                    save.write('{"offset":%d,"event":"%s","data":%s,"released_at":"%s","virtual_released_at":""}\n' % (
                                        offset, event_name, text,
                                        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")))
                        event_id = event_name = None
                        data_lines = []
                        if finished():
                            break
            except Exception as err:  # dropped connection, read timeout, IncompleteRead
                print("stream interrupted after offset %d: %r" % (last, err), file=sys.stderr)
            if not finished():
                print("stream ended after offset %d; reconnecting" % last, file=sys.stderr)
                time.sleep(0.5)
    finally:
        if save:
            save.close()
    return events


def check_offsets(events):
    """Returns human-readable problems with the offset sequence."""
    problems, offsets = [], sorted(e["offset"] for e in events)
    if offsets and offsets[0] != 1:
        problems.append("first offset is %d, not 1" % offsets[0])
    gaps = sum(1 for a, b in zip(offsets, offsets[1:]) if b > a + 1)
    dups = sum(1 for a, b in zip(offsets, offsets[1:]) if b == a)
    if gaps:
        problems.append("%d gaps in the offset sequence" % gaps)
    if dups:
        problems.append("%d repeated offsets" % dups)
    return problems


# ------------------------------------------------------------------ compare --

def unwrap(value):
    """Attribute values may be native JSON or JSON-encoded strings."""
    if isinstance(value, str):
        text = value.strip()
        if text[:1] in ('[', '{', '"') or text in ("true", "false", "null") or text[:1].isdigit() or text[:1] == "-":
            try:
                return unwrap(json.loads(text))
            except ValueError:
                return value
    return value


def as_list(value):
    value = unwrap(value)
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return [str(v) for v in value]


def fetch_audit(base, token):
    base, items, cursor, partial = base.rstrip("/"), [], None, False
    while True:
        query = {"source": "SATELIT", "limit": "1000"}
        if cursor:
            query["cursor"] = cursor
        url = base + "/internal/audit/events?" + urllib.parse.urlencode(query)
        with http_get(url, {"Authorization": "Bearer " + (token or "")}, timeout=60) as resp:
            page = json.loads(resp.read().decode("utf-8"))
        items.extend(page.get("items") or [])
        partial = partial or bool(page.get("partial"))
        cursor = page.get("next_cursor")
        if not cursor:
            return items, partial


def normalise_item(item):
    attrs = unwrap(item.get("attributes")) or {}
    if not isinstance(attrs, dict):
        attrs = {}
    attrs = {k: unwrap(v) for k, v in attrs.items()}
    conf = str(attrs.get("max_confidence", "")).strip().lower()
    return {
        "hazard_id": item.get("hazard_id"),
        "source_ref_id": item.get("source_ref_id"),
        "hazard_type": item.get("hazard_type"),
        "status": str(attrs.get("status", "active")).strip().lower(),
        "detection_keys": sorted(as_list(attrs.get("detection_keys"))),
        "detection_count": attrs.get("detection_count"),
        "satellites": sorted(as_list(attrs.get("satellites"))),
        "max_confidence": "lnh"[CONFIDENCE[conf]] if conf in CONFIDENCE else conf,
        "total_frp_mw": attrs.get("total_frp_mw"),
        "latitude": item.get("latitude"),
        "longitude": item.get("longitude"),
        "occurred_at": item.get("occurred_at"),
        "severity": item.get("severity"),
    }


def parse_time(value):
    try:
        text = str(value).strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def field_differences(ref, sysf):
    diffs = {}

    def note(field, expected, got):
        diffs[field] = {"reference": expected, "system": got}

    if sysf["hazard_type"] != "WILDFIRE":
        note("hazard_type", "WILDFIRE", sysf["hazard_type"])
    if sysf["severity"] != ref["severity"]:
        note("severity", ref["severity"], sysf["severity"])
    if number(sysf["detection_count"]) != ref["detection_count"]:
        note("detection_count", ref["detection_count"], sysf["detection_count"])
    frp = number(sysf["total_frp_mw"])
    if frp is None or abs(frp - ref["total_frp_mw"]) > 0.01:
        note("total_frp_mw", ref["total_frp_mw"], sysf["total_frp_mw"])
    if sysf["max_confidence"] != ref["max_confidence"]:
        note("max_confidence", ref["max_confidence"], sysf["max_confidence"])
    if sysf["satellites"] != ref["satellites"]:
        note("satellites", ref["satellites"], sysf["satellites"])
    if parse_time(sysf["occurred_at"]) != parse_time(ref["occurred_at"]):
        note("occurred_at", ref["occurred_at"], sysf["occurred_at"])
    for axis in ("latitude", "longitude"):
        got = number(sysf[axis])
        if got is None or abs(got - ref[axis]) > 1e-4:
            note(axis, ref[axis], sysf[axis])
    return diffs


def compare(ref_doc, system_items):
    system = [normalise_item(it) for it in system_items]
    active = [s for s in system if s["status"] != "retracted"]
    ref_by_set = {frozenset(f["detection_keys"]): f for f in ref_doc["fires"]}
    ref_of_key = {k: i for i, f in enumerate(ref_doc["fires"]) for k in f["detection_keys"]}

    sys_by_set, duplicate_sets = {}, []
    for s in active:
        ks = frozenset(s["detection_keys"])
        if ks in sys_by_set:
            duplicate_sets.append({"source_ref_ids": [sys_by_set[ks]["source_ref_id"], s["source_ref_id"]],
                                   "detection_count": len(ks)})
        else:
            sys_by_set[ks] = s

    def repeated_refs(rows):
        seen = {}
        for s in rows:
            seen[s["source_ref_id"]] = seen.get(s["source_ref_id"], 0) + 1
        return sorted(str(r) for r, n in seen.items() if n > 1)

    duplicate_active_refs, duplicate_refs = repeated_refs(active), repeated_refs(system)

    matched, differing, missing, extra = 0, [], [], []
    for ks, ref in ref_by_set.items():
        sysf = sys_by_set.get(ks)
        if sysf is None:
            missing.append({"occurred_at": ref["occurred_at"], "severity": ref["severity"],
                            "detection_count": ref["detection_count"], "detection_keys": ref["detection_keys"]})
            continue
        matched += 1
        diffs = field_differences(ref, sysf)
        if diffs:
            differing.append({"source_ref_id": sysf["source_ref_id"], "detection_count": ref["detection_count"],
                              "first_key": ref["detection_keys"][0], "fields": diffs})
    for ks, sysf in sys_by_set.items():
        if ks in ref_by_set:
            continue
        related = sorted({ref_of_key[k] for k in ks if k in ref_of_key})
        relation = "disjoint"
        if related:
            ref_sets = [frozenset(ref_doc["fires"][i]["detection_keys"]) for i in related]
            if len(related) == 1 and ks < ref_sets[0]:
                relation = "subset"
            elif all(rs <= ks for rs in ref_sets):
                relation = "superset"
            else:
                relation = "overlap"
        extra.append({"source_ref_id": sysf["source_ref_id"], "severity": sysf["severity"],
                      "detection_count": len(ks), "relation": relation,
                      "keys_not_in_any_reference_fire": len([k for k in ks if k not in ref_of_key]),
                      "reference_fires": [{"occurred_at": ref_doc["fires"][i]["occurred_at"],
                                           "detection_count": ref_doc["fires"][i]["detection_count"],
                                           "first_key": ref_doc["fires"][i]["detection_keys"][0]} for i in related],
                      "detection_keys": sorted(ks)})
    exact = not (missing or extra or differing or duplicate_sets or duplicate_active_refs)
    return {
        "exact": exact,
        "reference_fires": len(ref_doc["fires"]),
        "system_events": len(system),
        "system_active": len(active),
        "system_retracted_tombstones": len(system) - len(active),
        "matched_by_key_set": matched,
        "matched_with_differences": len(differing),
        "missing_from_system": len(missing),
        "system_not_in_reference": len(extra),
        "system_not_in_reference_by_relation": {r: len([e for e in extra if e["relation"] == r])
                                                for r in ("subset", "superset", "overlap", "disjoint")},
        "duplicate_source_ref_id_active": duplicate_active_refs,
        "duplicate_source_ref_id_any_status": duplicate_refs,
        "duplicate_key_sets_active": duplicate_sets,
        "differences": differing,
        "missing": missing,
        "extra": extra,
    }


# ---------------------------------------------------------------- self-test --

def _ev(offset, kind, sat, date, hhmm, lat, lon, conf="n", frp="1.0"):
    text = '{"satellite":"%s","acq_date":"%s","acq_time":"%s","latitude":%s,"longitude":%s,"confidence":"%s","frp":%s}' % (
        sat, date, hhmm, lat, lon, conf, frp)
    return {"offset": offset, "event": kind, "data": loads_raw(text)}


def self_test():
    day = "2026-11-12"

    def run(events, **kw):
        return reference(events, **kw)

    # distance threshold: 0.00899 deg of latitude is 0.99964 km, 0.00900 is 1.00076 km
    doc = run([_ev(1, "detection", "N20", day, "243", "-2.00000", "113.00000"),
               _ev(2, "detection", "N20", day, "243", "-1.99101", "113.00000")])
    assert doc["fire_count"] == 1 and doc["singles"] == 0, doc
    assert doc["fires"][0]["detection_keys"] == ["N20|2026-11-12|243|-1.99101|113.00000",
                                                 "N20|2026-11-12|243|-2.00000|113.00000"], doc
    assert doc["fires"][0]["occurred_at"] == "2026-11-12T02:43:00Z", doc
    doc = run([_ev(1, "detection", "N20", day, "243", "-2.00000", "113.00000"),
               _ev(2, "detection", "N20", day, "243", "-1.99100", "113.00000")])
    assert doc["fire_count"] == 0 and doc["singles"] == 2, doc

    # time threshold: 360 min links, 361 does not; unpadded "5" is 00:05; midnight
    assert run([_ev(1, "detection", "N", day, "5", "1.00000", "100.00000"),
                _ev(2, "detection", "N21", day, "605", "1.00100", "100.00000")])["fire_count"] == 1
    assert run([_ev(1, "detection", "N", day, "5", "1.00000", "100.00000"),
                _ev(2, "detection", "N21", day, "606", "1.00100", "100.00000")])["fire_count"] == 0
    doc = run([_ev(1, "detection", "N", day, "2358", "1.00000", "100.00000"),
               _ev(2, "detection", "N20", "2026-11-13", "3", "1.00100", "100.00000")])
    assert doc["fire_count"] == 1 and doc["fires"][0]["satellites"] == ["N", "N20"], doc
    assert doc["fires"][0]["occurred_at"] == "2026-11-12T23:58:00Z", doc

    # chain: A-B and B-C linked, A-C too far in space and time
    chain = [_ev(1, "detection", "N", day, "100", "0.00000", "110.00000"),
             _ev(2, "detection", "N20", day, "600", "0.00800", "110.00000"),
             _ev(3, "detection", "N21", day, "1100", "0.01600", "110.00000")]
    doc = run(chain)
    assert doc["fire_count"] == 1 and doc["fires"][0]["detection_count"] == 3, doc
    assert abs(doc["fires"][0]["latitude"] - 0.008) < 1e-9, doc

    # merge through a bridge, then split by retracting it (both retract shapes)
    two = [_ev(1, "detection", "N", day, "100", "0.00000", "110.00000"),
           _ev(2, "detection", "N", day, "100", "0.00050", "110.00000"),
           _ev(3, "detection", "N", day, "100", "0.01500", "110.00000"),
           _ev(4, "detection", "N", day, "100", "0.01550", "110.00000")]
    assert run(two)["fire_count"] == 2
    bridge = _ev(5, "detection", "N20", day, "133", "0.00775", "110.00000")
    assert run(two + [bridge])["fire_count"] == 1
    by_key = {"offset": 6, "event": "retract", "data": {"key": "N20|2026-11-12|133|0.00775|110.00000"}}
    by_fields = {"offset": 6, "event": "retract", "data": loads_raw(
        '{"satellite":"N20","acq_date":"2026-11-12","acq_time":"133","latitude":0.00775,"longitude":110.00000}')}
    for retract in (by_key, by_fields):
        doc = run(two + [bridge, retract])
        assert doc["fire_count"] == 2 and doc["detections_live"] == 4, doc
    assert run(two + [bridge, by_key], until=5)["fire_count"] == 1

    # low confidence is ignored; an update promoting it creates the fire
    low = [_ev(1, "detection", "N", day, "100", "0.00000", "110.00000"),
           _ev(2, "detection", "N", day, "100", "0.00100", "110.00000", conf="low")]
    doc = run(low)
    assert (doc["fire_count"], doc["singles"], doc["low_confidence_ignored"]) == (0, 1, 1), doc
    doc = run(low + [_ev(3, "update", "N", day, "100", "0.00100", "110.00000", conf="nominal", frp="2.5")])
    assert doc["fire_count"] == 1 and doc["fires"][0]["total_frp_mw"] == 3.5, doc

    # duplicates count once
    doc = run([_ev(1, "detection", "N", day, "100", "0.00000", "110.00000"),
               _ev(2, "detection", "N", day, "100", "0.00100", "110.00000", conf="h"),
               _ev(3, "detection", "N", day, "100", "0.00000", "110.00000")])
    assert doc["fires"][0]["detection_count"] == 2 and doc["events"]["duplicate_detections"] == 1, doc
    assert doc["fires"][0]["max_confidence"] == "h" and doc["fires"][0]["severity"] == "WASPADA", doc

    # severity thresholds
    def cluster(n, frp):
        return [_ev(i + 1, "detection", "N", day, "100", "0.%05d" % (i * 10), "110.00000", frp=frp) for i in range(n)]
    assert run(cluster(5, "1.0"))["fires"][0]["severity"] == "WASPADA"
    assert run(cluster(6, "1.0"))["fires"][0]["severity"] == "SIAGA"
    assert run(cluster(19, "1.0"))["fires"][0]["severity"] == "SIAGA"
    assert run(cluster(20, "1.0"))["fires"][0]["severity"] == "AWAS"
    assert run(cluster(3, "9.99"))["fires"][0]["severity"] == "WASPADA"
    assert run(cluster(3, "10.0"))["fires"][0]["severity"] == "SIAGA"
    assert run(cluster(3, "33.33"))["fires"][0]["severity"] == "SIAGA"
    assert run(cluster(4, "25.0"))["fires"][0]["severity"] == "AWAS"

    # lateness: the third detection is 400 minutes behind the newest one seen
    late = [_ev(1, "detection", "N", day, "100", "0.00000", "110.00000"),
            _ev(2, "detection", "N", day, "900", "5.00000", "120.00000"),
            _ev(3, "detection", "N20", day, "220", "0.00100", "110.00000")]
    assert run(late)["fire_count"] == 1
    doc = run(late, lateness=360)
    assert doc["fire_count"] == 0 and doc["events"]["not_admitted_events"] == 1, doc
    assert run(late, lateness=480)["fire_count"] == 1

    # compare: attributes as a JSON string, values as JSON-encoded strings
    doc = run(chain)
    fire = doc["fires"][0]
    item = {"hazard_id": "hz-1", "source": "SATELIT", "source_ref_id": "fire-1", "hazard_type": "WILDFIRE",
            "severity": "WASPADA", "latitude": 0.008, "longitude": 110.0, "occurred_at": "2026-11-12T01:00:00Z",
            "attributes": json.dumps({"detection_count": "3", "satellites": json.dumps(["N", "N20", "N21"]),
                                      "max_confidence": "nominal", "total_frp_mw": "3.0", "status": "active",
                                      "detection_keys": json.dumps(fire["detection_keys"])})}
    assert compare(doc, [item])["exact"], compare(doc, [item])
    tomb = dict(item, source_ref_id="fire-0", attributes={"status": "retracted", "detection_keys": []})
    report = compare(doc, [item, tomb])
    assert report["exact"] and report["system_retracted_tombstones"] == 1, report
    wrong = dict(item, severity="SIAGA")
    report = compare(doc, [wrong])
    assert not report["exact"] and list(report["differences"][0]["fields"]) == ["severity"], report
    part = dict(item, attributes={"detection_keys": fire["detection_keys"][:2], "detection_count": 2})
    report = compare(doc, [part])
    assert report["missing_from_system"] == 1 and report["extra"][0]["relation"] == "subset", report
    report = compare(doc, [item, dict(item, hazard_id="hz-2")])
    assert report["duplicate_key_sets_active"] and report["duplicate_source_ref_id_active"] == ["fire-1"], report
    print("self-test: ok")


# --------------------------------------------------------------------- main --

def summarise_reference(label, doc):
    ev = doc["events"]
    print("%s (until offset %d)" % (label, doc["until_offset"]))
    print("  events           %d  (detection %d, update %d, retract %d; duplicates %d, retract of unknown key %d)" % (
        ev.get("events", 0), ev.get("detection", 0), ev.get("update", 0), ev.get("retract", 0),
        ev.get("duplicate_detections", 0), ev.get("retract_unknown_key", 0)))
    if ev.get("not_admitted_events"):
        print("  not admitted     %d events over the lateness bound (%d keys, %d never live)" % (
            ev["not_admitted_events"], ev["not_admitted_keys"], ev["not_admitted_keys_never_live"]))
    print("  detections live  %d  (low confidence ignored %d, singles %d)" % (
        doc["detections_live"], doc["low_confidence_ignored"], doc["singles"]))
    sev = doc["fires_by_severity"]
    print("  fires            %d  (WASPADA %d, SIAGA %d, AWAS %d; multi-satellite %d)" % (
        doc["fire_count"], sev["WASPADA"], sev["SIAGA"], sev["AWAS"], doc["multi_satellite_fires"]))


def summarise_comparison(label, rep):
    rel = rep["system_not_in_reference_by_relation"]
    print("%s: %s" % (label, "EXACT" if rep["exact"] else "DIFFERENT"))
    print("  reference fires %d | system active %d, retracted tombstones %d" % (
        rep["reference_fires"], rep["system_active"], rep["system_retracted_tombstones"]))
    print("  matched by key set %d (with differing fields %d) | missing from system %d" % (
        rep["matched_by_key_set"], rep["matched_with_differences"], rep["missing_from_system"]))
    print("  system fires not in reference %d (subset %d, superset %d, overlap %d, disjoint %d)" % (
        rep["system_not_in_reference"], rel["subset"], rel["superset"], rel["overlap"], rel["disjoint"]))
    print("  duplicate source_ref_id %d | duplicate key sets %d" % (
        len(rep["duplicate_source_ref_id_active"]), len(rep["duplicate_key_sets_active"])))
    fields = {}
    for d in rep["differences"]:
        for f in d["fields"]:
            fields[f] = fields.get(f, 0) + 1
    if fields:
        print("  differing fields: " + ", ".join("%s %d" % kv for kv in sorted(fields.items())))


def write_json(path, doc):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=1)
        fh.write("\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log-url", help="mock feed admin log, e.g. http://localhost:8088/admin/log")
    ap.add_argument("--admin-token", default=os.environ.get("FEED_MOCK_ADMIN_TOKEN", ""))
    ap.add_argument("--log-file", help="NDJSON capture (admin log or --save-log output)")
    ap.add_argument("--stream-url", help="feed base URL, e.g. https://satellite.naufarrel.dev")
    ap.add_argument("--token", default=os.environ.get("FEED_TOKEN", ""), help="feed bearer token")
    ap.add_argument("--seconds", type=float, help="stop reading the stream after this long")
    ap.add_argument("--save-log", help="write the stream capture here as NDJSON")
    ap.add_argument("--until-offset", type=int, help="apply events up to this offset only")
    ap.add_argument("--out", help="write the reference JSON here")
    ap.add_argument("--audit-url", help="aggregator base URL for compare mode")
    ap.add_argument("--audit-token", default=os.environ.get("AUDIT_TOKEN", ""))
    ap.add_argument("--compare-out", help="write the comparison JSON here (default: <out>.compare.json)")
    ap.add_argument("--lateness-minutes", type=float, help="also evaluate a watermark-policy reference")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        self_test()
        return 0
    if sum(bool(x) for x in (args.log_url, args.log_file, args.stream_url)) != 1:
        ap.error("give exactly one of --log-url, --log-file, --stream-url")
    if args.log_url:
        events = load_log_url(args.log_url, args.admin_token, args.until_offset)
    elif args.log_file:
        events = load_log_file(args.log_file)
    else:
        if args.until_offset is None and not args.seconds:
            ap.error("--stream-url needs --until-offset and/or --seconds")
        events = read_stream(args.stream_url, args.token, args.until_offset, args.seconds, args.save_log)
    for problem in check_offsets(events):
        print("WARNING: " + problem, file=sys.stderr)

    plain = reference(events, args.until_offset)
    output = dict(plain)
    late = None
    if args.lateness_minutes is not None:
        late = reference(events, args.until_offset, args.lateness_minutes)
        plain_sets = {frozenset(f["detection_keys"]) for f in plain["fires"]}
        late_sets = {frozenset(f["detection_keys"]) for f in late["fires"]}
        late["lateness_minutes"] = args.lateness_minutes
        late["fires_only_without_lateness_bound"] = len(plain_sets - late_sets)
        late["fires_only_with_lateness_bound"] = len(late_sets - plain_sets)
        output["lateness_reference"] = late
    if args.out:
        write_json(args.out, output)

    summarise_reference("reference", plain)
    if late:
        summarise_reference("reference with lateness bound %g min" % args.lateness_minutes, late)
        print("  fires changed by the bound: %d only without it, %d only with it" % (
            late["fires_only_without_lateness_bound"], late["fires_only_with_lateness_bound"]))
    if not args.audit_url:
        return 0

    items, partial = fetch_audit(args.audit_url, args.audit_token)
    if partial:
        print("WARNING: audit answered partial=true; the comparison may be incomplete", file=sys.stderr)
    report = {"audit_partial": partial, "until_offset": plain["until_offset"], "plain": compare(plain, items)}
    summarise_comparison("system vs reference", report["plain"])
    verdict = report["plain"]
    if late:
        report["lateness_minutes"] = args.lateness_minutes
        report["lateness"] = compare(late, items)
        summarise_comparison("system vs reference with lateness bound", report["lateness"])
        verdict = report["lateness"]
    compare_out = args.compare_out or (os.path.splitext(args.out)[0] + ".compare.json" if args.out else None)
    if compare_out:
        write_json(compare_out, report)
        print("comparison written to " + compare_out)
    return 0 if verdict["exact"] and not partial else 1


if __name__ == "__main__":
    sys.exit(main())
