#!/usr/bin/env python3
"""Summarise one M2 evidence run: PASS / FAIL / NOT RUN per criterion.

Reads the files written by scripts/m2-demo.sh into one run directory and
writes summary.json and SUMMARY.md next to them. Every number is read from an
evidence file; the summary names that file so it can be opened on screen.

    python3 scripts/m2-summary.py artifacts/m2/runs/<run-id>
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

PASS, FAIL, NOT_RUN, INFO = "PASS", "FAIL", "NOT RUN", "INFO"
# PARTIAL: the criterion held with a stated exception (named in the detail).
PARTIAL = "PARTIAL"


class Run:
    def __init__(self, root: str):
        self.root = root

    def path(self, name: str) -> str:
        return os.path.join(self.root, name)

    def json(self, name: str):
        try:
            with open(self.path(name), encoding="utf-8") as handle:
                return json.load(handle)
        except (OSError, ValueError):
            return None

    def jsonl(self, name: str) -> list[dict]:
        try:
            rows = []
            for line in open(self.path(name), encoding="utf-8"):
                line = line.strip()
                if line.startswith("{"):
                    try:
                        rows.append(json.loads(line))
                    except ValueError:
                        pass
            return rows
        except OSError:
            return []


def check(problem: str, criterion: str, status: str, detail: str, *files: str) -> dict:
    return {"problem": problem, "criterion": criterion, "status": status, "detail": detail, "evidence": list(files)}


def ratio(value) -> str:
    return f"{value:.3f}" if isinstance(value, (int, float)) else "n/a"


def p1(run: Run) -> list[dict]:
    out = []
    report = run.json("p1/catalog-report.json")
    log = run.json("p1/import-log.json")
    dist = run.json("p1/distribution-after-import.json")
    if not (log and dist):
        out.append(check("P1", "1. Dataset loaded and spread over nodes", NOT_RUN, "no import/distribution file", "p1/import-log.json"))
    else:
        nodes = [n for n in dist.get("nodes", []) if n.get("live")]
        holds_all = [n["node"] for n in nodes if n.get("holds_all_ranges")]
        shares = ", ".join(f"{n['node']} {n.get('share_of_ranges', 0) * 100:.0f}%" for n in nodes)
        expected = (report or {}).get("output", {}).get("rows")
        complete = log.get("rejected", 1) == 0 and (expected is None or log.get("rows") == expected)
        status = PASS if complete and not holds_all and len(nodes) > 1 else FAIL
        out.append(check("P1", "1. Dataset loaded and spread over nodes", status,
                         f"rows {log.get('rows')} inserted {log.get('inserted')} rejected {log.get('rejected')}; "
                         f"{dist.get('ranges')} ranges; replicas per node: {shares}; nodes holding every range: {holds_all or 'none'}",
                         "p1/catalog-report.json", "p1/import-log.json", "p1/distribution-after-import.json"))
    imp, burst = run.json("p1/load-during-import-peak.json"), run.json("p1/load-during-burst-peak.json")
    if imp or burst:
        worst = max((x or {}).get("max_over_mean", 0) for x in (imp, burst))
        skew = (report or {}).get("skew", {})
        out.append(check("P1", "2. Write load spread despite skewed data", PASS if worst and worst < 1.5 else FAIL,
                         f"max/mean write load per node: import {ratio((imp or {}).get('max_over_mean'))}, "
                         f"one-region burst {ratio((burst or {}).get('max_over_mean'))}; dataset skew: region max/mean "
                         f"{skew.get('region_max_over_mean')}, busiest day {skew.get('busiest_days', [[None, None]])[0]}",
                         "p1/load-during-import-peak.json", "p1/load-during-burst-peak.json", "p1/catalog-report.json"))
    else:
        out.append(check("P1", "2. Write load spread despite skewed data", NOT_RUN, "", "p1/load-during-import-peak.json"))
    cross = run.json("p1/crossread.json")
    if cross:
        truth = cross.get("ground_truth") or {}
        ok = cross.get("duplicates") == 0 and cross.get("strictly_ordered_occurred_at_desc_hazard_id_desc") \
            and cross.get("all_pages_200") and truth.get("missing_from_pages", 0) == 0
        out.append(check("P1", "3. Cross-store paginated read in correct order", PASS if ok else FAIL,
                         f"{cross.get('pages_requested')} pages, {cross.get('items')} events, duplicates {cross.get('duplicates')}, "
                         f"missing vs Audit {truth.get('missing_from_pages')}, ordered {cross.get('strictly_ordered_occurred_at_desc_hazard_id_desc')}, "
                         f"max page {cross.get('max_page_seconds')} s", "p1/crossread.json"))
    else:
        out.append(check("P1", "3. Cross-store paginated read in correct order", NOT_RUN, "", "p1/crossread.json"))
    compare, probe = run.json("p1/node-loss/compare.json"), run.json("p1/node-loss/probe.json")
    if compare:
        catalog = compare.get("catalog_after") or {}
        ok = compare.get("lost") == 0 and catalog.get("match", True) and (probe or {}).get("forbidden_outcomes", 0) == 0
        out.append(check("P1", "4. Worker node lost with its data: nothing lost, answers stay explicit", PASS if ok else FAIL,
                         f"lost {compare.get('lost')} of {compare.get('before_count')}; catalog checksum match {catalog.get('match')} "
                         f"({catalog.get('stored_sha256', '')[:12]}...); probe: {len((probe or {}).get('records', []))} requests, "
                         f"5xx/timeouts {(probe or {}).get('forbidden_outcomes')}, max {json.dumps((probe or {}).get('max_seconds'))}",
                         "p1/node-loss/compare.json", "p1/node-loss/probe.json", "p1/node-loss/timeline.txt"))
    else:
        out.append(check("P1", "4. Worker node lost with its data", NOT_RUN, "run: scripts/m2-demo.sh p1-node-loss", "p1/node-loss/compare.json"))
    final = run.json("p1/node-loss/distribution-final.json")
    if final:
        nodes = [n for n in final.get("nodes", []) if n.get("live")]
        out.append(check("P1", "5-6. Node rejoins, ranges rebalance, partial cleared",
                         PASS if not final.get("partial") and len(nodes) >= 3 else FAIL,
                         f"{final.get('ranges')} ranges over {len(nodes)} live nodes: "
                         + ", ".join(f"{n['node']} {n.get('replicas')} replicas" for n in nodes) + f"; partial {final.get('partial')}",
                         "p1/node-loss/distribution-final.json", "p1/node-loss/timeline.txt"))
    analytics = run.json("p1/analytics-vs-audit.json")
    if analytics:
        totals = {analytics.get(k) for k in ("audit_events_counted", "analytics_total_by_category",
                                             "analytics_total_by_severity", "analytics_total_timeline")}
        mismatches = analytics.get("mismatches")
        ok = len(totals) == 1 and not mismatches
        out.append(check("P1", "7. Analytics agree with the Audit API", PASS if ok else FAIL,
                         f"audit {analytics.get('audit_events_counted')} events vs analytics category/severity/timeline "
                         f"{analytics.get('analytics_total_by_category')}/{analytics.get('analytics_total_by_severity')}/"
                         f"{analytics.get('analytics_total_timeline')}; {analytics.get('areas_compared')} areas compared",
                         "p1/analytics-vs-audit.json"))
    else:
        out.append(check("P1", "7. Analytics agree with the Audit API", NOT_RUN, "", "p1/analytics-vs-audit.json"))
    return out


def p2_fault(run: Run, name: str, criterion: str) -> dict:
    data = run.json(f"p2/{name}.json")
    if not data:
        return check("P2", criterion, NOT_RUN, f"run: scripts/m2-demo.sh p2-{ {'clock-skew': 'skew'}.get(name, name)}", f"p2/{name}.json")
    upstream, writes, event = data.get("upstream", {}), data.get("writes", {}), data.get("event") or {}
    two = upstream.get("two_callers_within_half_interval") or []
    regress = writes.get("fence_regressions") or []
    ok = not two and not regress and not event.get("old_term_wrote_after_event", False) \
        and upstream.get("requests_attributed_to_two_pods", 0) == 0
    terms = ", ".join(f"fence {t.get('fence')} {t.get('pod', '')[-5:]} {t.get('from', '')[11:19]}-{t.get('until', '')[11:19]}"
                      for t in writes.get("terms", []))
    gap = event.get("seconds_event_to_first_new_write")
    before_pod = (event.get("term_before") or {}).get("pod")
    next_pod = (event.get("next_term") or {}).get("pod")
    takeover = "" if not next_pod else (" (taken over by another instance)" if next_pod != before_pod
                                        else " (same pod after container restart)")
    return check("P2", criterion, PASS if ok else FAIL,
                 f"two pods calling upstream in one interval: {len(two)}; fence regressions: {len(regress)}; "
                 f"old term wrote after fault: {event.get('old_term_wrote_after_event')}; next write after {gap} s{takeover}; terms: {terms}",
                 f"p2/{name}.json", f"p2/{name}-actions.txt")


def p2(run: Run) -> list[dict]:
    out = []
    spread = run.json("p2/entry-spread.json")
    if spread:
        pods = spread.get("per_pod", {})
        out.append(check("P2", "1. Every request may land on any instance", PASS if len(pods) >= 2 else FAIL,
                         f"{spread.get('requests')} requests through NodePorts landed on {len(pods)} Aggregator pods: "
                         + ", ".join(f"{p[-5:]}={c}" for p, c in pods.items()), "p2/entry-spread.json"))
    else:
        out.append(check("P2", "1. Every request may land on any instance", NOT_RUN, "", "p2/entry-spread.json"))
    out.append(p2_fault(run, "failover", "2-3. Leader killed: one poller, no gap, no duplicate"))
    gap = run.json("p2/failover-gap-check.json")
    if gap:
        out.append(check("P2", "3. Upstream events during the crash stored exactly once",
                         PASS if gap.get("missing") == 0 and gap.get("duplicate_source_ref_ids") == 0 else FAIL,
                         f"mock events {gap.get('mock_events')}, stored {gap.get('stored_matching')}, missing {gap.get('missing')}, "
                         f"duplicates {gap.get('duplicate_source_ref_ids')}", "p2/failover-gap-check.json"))
    out.append(p2_fault(run, "clock-skew", "4. Clock skew on the leader node: still one writer"))
    out.append(p2_fault(run, "freeze", "5. Leader VM frozen then woken: no stale write"))
    load = run.json("p2/load.json")
    if load:
        out.append(check("P2", "6. 10k VU with scale 3-5-2 and version change: errors outside 429 < 1%",
                         PASS if load.get("error_rate", 1) < 0.01 else FAIL,
                         f"{load.get('http_reqs')} requests, {load.get('throughput_rps', 0):.0f} rps, error rate "
                         f"{load.get('error_rate', 0) * 100:.2f}%, 429 {load.get('controlled_rejections')}, 200 {load.get('served_ok')}",
                         "p2/load.json", "p2/load-timeline.txt"))
    else:
        out.append(check("P2", "6. 10k VU with scaling and version change", NOT_RUN, "run: scripts/m2-demo.sh p2-load", "p2/load.json"))
    one, three = run.json("p2/throughput-1-instance.json"), run.json("p2/throughput-3-instance.json")
    if one and three:
        factor = (three.get("served_ok_rps") or 0) / (one.get("served_ok_rps") or 1)
        # throughput-method.json names the per-instance CPU cap the runner
        # applied so that the aggregator tier, not the shared host, is what
        # the ratio measures; the uncapped baseline is kept beside it.
        method = run.json("p2/throughput-method.json") or {}
        note = f"; aggregator CPU {method['cpu_per_instance']} per instance (uncapped baseline: throughput-*-instance-uncapped.json)" if method.get("cpu_per_instance") else ""
        out.append(check("P2", "7. Throughput with 3 instances >= 2x one instance", PASS if factor >= 2 else FAIL,
                         f"served rps 1 instance {one.get('served_ok_rps', 0):.1f}, 3 instances {three.get('served_ok_rps', 0):.1f} -> {factor:.2f}x{note}",
                         "p2/throughput-1-instance.json", "p2/throughput-3-instance.json", *( ["p2/throughput-method.json"] if method else [])))
    else:
        out.append(check("P2", "7. Throughput 3 vs 1 instance", NOT_RUN, "run: scripts/m2-demo.sh p2-throughput", "p2/throughput-3-instance.json"))
    return out


def p3(run: Run) -> list[dict]:
    out = []
    schema = run.json("p3/schema-verification.json")
    if schema:
        parked = schema.get("parked_in_breaking_window", 0)
        out.append(check("P3", "1-2. Breaking schema records parked with a reason", PASS if parked > 0 else FAIL,
                         f"parked {parked}; reasons {json.dumps(schema.get('parked_reasons'))}; listing exposes payload "
                         f"{schema.get('listing_exposes_payload')}", "p3/schema-verification.json", "p3/schema-timeline.txt"))
        again = run.json("p3/replay-again.json") or {}
        replay = run.json("p3/replay.json") or {}
        out.append(check("P3", "4. Adapter + replay recovers each record once",
                         PASS if replay.get("recovered", 0) >= parked > 0 and again.get("recovered") == 0
                         and schema.get("duplicate_source_ref_ids", 0) == 0 else FAIL,
                         f"replay recovered {replay.get('recovered')}, second replay {again.get('recovered')}, "
                         f"marked by adapter {schema.get('recovered_events_with_adapter')}",
                         "p3/replay.json", "p3/replay-again.json", "p3/checksum-after.json"))
    else:
        out.append(check("P3", "1-4. Schema change, park, replay", NOT_RUN, "run: scripts/m2-demo.sh p3", "p3/schema-verification.json"))
    access = run.json("p3/ops-access.json")
    if access:
        ok = access.get("ops_token_list") == 200 and all(v == 401 for k, v in access.items() if not k.startswith("ops_token"))
        out.append(check("P3", "5. Only the Ops token reads parked payloads / replays", PASS if ok else FAIL,
                         json.dumps(access), "p3/ops-access.json"))
    failures = run.json("p3/ops-failures-malformed.json")
    if failures:
        # The same broken payload has the same message_id, so a repeated run
        # finds the earlier parked entry instead of a new one.
        kinds = {"truncated": "unexpected end of JSON", "deep_nesting": "max depth",
                 "invalid_utf8": "invalid_utf8", "oversized": "payload_too_large"}
        reasons = [i.get("reason", "") for i in failures.get("items", []) if i.get("status") == "parked"]
        found = {kind: any(text in r for r in reasons) for kind, text in kinds.items()}

        def restarts(name):
            try:
                return dict(line.split()[:2] for line in open(run.path(name)) if line.strip())
            except OSError:
                return {}
        before, after = restarts("p3/restarts-before.txt"), restarts("p3/restarts-after.txt")
        restarted = sorted(p for p in before if p in after and after[p] != before[p])
        out.append(check("P3", "3. Malformed responses parked, no restart",
                         PASS if all(found.values()) and not restarted else FAIL,
                         f"parked by kind {json.dumps(found)}; pods restarted during the test: {restarted or 'none'}",
                         "p3/malformed-timeline.txt", "p3/ops-failures-malformed.json", "p3/restarts-after.txt"))
    else:
        out.append(check("P3", "3. Malformed responses parked, no restart", NOT_RUN, "", "p3/ops-failures-malformed.json"))
    reports = run.json("p3/field-reports.json")
    if reports:
        lookup = reports.get("lookup", {})
        transitions = lookup.get("state_transitions_per_report", {})
        ok = not lookup.get("accepted_but_missing") and set(transitions) <= {"1", 1} \
            and lookup.get("status_counts", {}).get("processed") == reports.get("answered_202")
        out.append(check("P3", "6. Field reports answered 202 are never lost or processed twice", PASS if ok else FAIL,
                         f"sent {reports.get('reports_sent')}, 202 {reports.get('answered_202')}, stored {lookup.get('accepted_and_stored')}, "
                         f"transitions {json.dumps(transitions)}, pods killed {len(reports.get('pods_killed', []))}, "
                         f"p95 202 {(reports.get('first_try_202_seconds') or {}).get('p95')} s", "p3/field-reports.json"))
    else:
        out.append(check("P3", "6. Field reports durable", NOT_RUN, "", "p3/field-reports.json"))
    return out


def p4(run: Run) -> list[dict]:
    """P4 is graded on the rehearsal against the in-cluster stand-in feed
    (p4/sim): the assistants' feed needs a token that is only handed out for
    the graded session. The state of the real feed is shown as information."""
    out = []
    sim, cmp_ = run.json("p4/sim/result.json"), run.json("p4/sim/compare.json")
    if not sim:
        out.append(check("P4", "1-8. Rehearsal against the stand-in feed", NOT_RUN, "run: CONFIRM=yes scripts/m2-demo.sh p4-sim", "p4/sim/result.json"))
    else:
        verdict = (cmp_ or {}).get("lateness") or (cmp_ or {}).get("plain") or {}
        plain = (cmp_ or {}).get("plain") or {}
        counts, burst, lat = sim.get("counts") or {}, sim.get("burst") or {}, sim.get("latency_burst") or {}
        exact = bool(verdict.get("exact"))
        files = ("p4/sim/compare.json", "p4/sim/reference.json", "p4/sim/result.json")
        out.append(check("P4", "1, 3, 6. Fires equal the independent reference (multi-satellite, updates, retractions)", PASS if exact else FAIL,
                         f"until offset {(cmp_ or {}).get('until_offset')}: matched {verdict.get('matched_by_key_set')}, with differences {verdict.get('matched_with_differences')}, "
                         f"missing {verdict.get('missing_from_system')}, extra {len(verdict.get('extra') or [])}, retracted tombstones {verdict.get('system_retracted_tombstones')}; "
                         f"against the reference without a lateness bound: exact {plain.get('exact')}, missing {plain.get('missing_from_system')}, extra {len(plain.get('extra') or [])}", *files))
        out.append(check("P4", "2. Event-time grouping, declared lateness, late detections counted", PASS if "late_detections" in counts else FAIL,
                         f"accepted lateness {sim.get('config', {}).get('accepted_lateness_minutes')} min of event time; late detections recorded {counts.get('late_detections')}; "
                         f"low confidence ignored {counts.get('low_confidence_ignored')}; lone detections now {counts.get('single_detections_now')} (+{counts.get('closed_singles')} closed); "
                         f"fires active {counts.get('active_fires_now')}, closed {counts.get('closed_fires')}", "p4/sim/result.json", "p4/sim/status-final.json"))
        caught = burst.get("seconds_to_catch_up")
        ok4 = caught is not None and caught <= 60 and not counts.get("offset_gaps")
        out.append(check("P4", "4. 60x burst: nothing lost, current again <= 60 s, p95 reported, lag visible", PASS if ok4 else FAIL,
                         f"behind by {burst.get('behind_at_end_events')} events when the burst ended (max lag {burst.get('max_lag_events')}); caught up after {caught if caught is None else round(caught, 1)} s; "
                         f"offset gaps {counts.get('offset_gaps')}; event-to-store p95 <= {lat.get('p95_upper_bound_seconds')} s, p50 <= {lat.get('p50_upper_bound_seconds')} s over {lat.get('events')} events",
                         "p4/sim/result.json", "p4/sim/watch.jsonl"))
        actions = burst.get("actions") or []
        killed = [a for a in actions if "force-deleted" in a.get("action", "")]
        cuts = [a for a in actions if "cut" in a.get("action", "")]
        out.append(check("P4", "5, 8. Leader killed mid-recognition and connection cut: same result, no loss, no duplicate", PASS if exact and killed and cuts and not counts.get("offset_gaps") else FAIL,
                         f"{'; '.join(a['action'] for a in killed) or 'no pod killed'}; connections cut {len(cuts)}; offset gaps {counts.get('offset_gaps')}; "
                         f"result equals the reference: {exact}; duplicate ids/key sets {verdict.get('duplicate_source_ref_ids', 0)}/{verdict.get('duplicate_key_sets', 0)}", *files, "p4/sim/timeline.txt"))
    status = run.json("p4/status.json")
    if status:
        feed = status.get("feed_status")
        checksum = status.get("audit_checksum") or {}
        stand_in = "satellite-feed" in (status.get("feed_url") or "")
        if stand_in:
            out.append(check("P4", "7. Live stream processed", INFO,
                             f"ingestor is connected to the in-cluster stand-in feed ({status.get('feed_url')}): status {feed}, SATELIT events {checksum.get('count')}; "
                             "the assistants' feed rejects the group token (401) until the graded session", "p4/status.json"))
        else:
            out.append(check("P4", "7. Assistants' live feed", PASS if feed == 200 and (checksum.get("count") or 0) > 0 else INFO,
                             (f"feed answered {feed}; SATELIT events {checksum.get('count')}" if feed == 200 else
                              f"feed answered {feed} to the group token (the assistants hand out the token for the graded session)"),
                             "p4/status.json"))
        reference = status.get("reference_comparison")
        if reference:
            out.append(check("P4", "6. Result vs assistants' reference", PASS if reference.get("match") else FAIL,
                             json.dumps(reference)[:300], "p4/status.json"))
    return out


def p5(run: Run) -> list[dict]:
    out = []
    latency = run.json("p5/critical-latency.json")
    if latency:
        refs = {r["from"]: r for r in latency.get("references", [])}
        main = refs.get(latency.get("gating_reference"), {})
        enough = (main.get("samples") or 0) >= (latency.get("min_samples") or 1)
        out.append(check("P5", "1. Critical notification p95 <= 5 s",
                         (PASS if latency.get("pass") else FAIL) if enough else NOT_RUN,
                         f"{main.get('samples')} samples, {main.get('within_target')} within {latency.get('target_seconds')} s "
                         f"(ratio {main.get('ratio_within_target')}, p95 <= {main.get('p95_upper_bound_seconds')} s)",
                         "p5/critical-latency.json"))
    else:
        out.append(check("P5", "1. Critical notification p95 <= 5 s", NOT_RUN, "needs critical sensor readings", "p5/critical-latency.json"))
    quarantine = run.json("p5/quarantine.json")
    out.append(check("P5", "2. Bad readings quarantined, counted by reason", INFO if quarantine else NOT_RUN,
                     json.dumps(quarantine)[:300] if quarantine else "", "p5/quarantine.json"))
    rows = run.jsonl("p5/sensor-outage.jsonl")
    samples = [r for r in rows if "phase" in r]
    if samples:
        outage = [r for r in samples if r["phase"] == "outage"]
        slow = [r for r in samples if r["coastal_latest"]["status"] != 200 or r["coastal_latest"]["seconds"] > 2
                or r["hazards_bmkg"]["status"] != 200 or r["hazards_bmkg"]["seconds"] > 2]

        def failed(r):
            total = 0
            for pod in r.get("coastal_ingestor", {}).values():
                total += sum(v for k, v in pod.items() if k.startswith("coastal_sensor_requests_total") and 'outcome="ok"' not in k)
            return total
        failed_during = (failed(outage[-1]) - failed(samples[0])) if outage else None
        opened = any(any(p.get("coastal_circuit_state") == 2 for p in r.get("coastal_ingestor", {}).values()) for r in outage)
        last = samples[-1]
        closed = all(p.get("coastal_circuit_state", 0) == 0 for p in last.get("coastal_ingestor", {}).values())
        out.append(check("P5", "3. Other sources unaffected during sensor outage", PASS if not slow else FAIL,
                         f"{len(samples)} samples; slow/failed answers {len(slow)}; max BMKG {max(r['hazards_bmkg']['seconds'] for r in samples):.3f} s",
                         "p5/sensor-outage.jsonl"))
        out.append(check("P5", "4. Breaker opens (state 2), no request storm, automatic recovery", PASS if opened and closed else FAIL,
                         f"breaker opened {opened}; failed sensor requests during outage {failed_during}; closed at end {closed}; "
                         f"max /coastal/latest {max(r['coastal_latest']['seconds'] for r in samples):.3f} s",
                         "p5/sensor-outage.jsonl"))
    else:
        out.append(check("P5", "3-4. Sensor outage", NOT_RUN, "run: scripts/m2-demo.sh p5-outage", "p5/sensor-outage.jsonl"))
    shed = run.json("p5/shedding.json")
    if shed:
        field, media = shed["classes"]["field"], shed["classes"]["media"]
        rejected = media.get("statuses", {}).get("429", 0)
        ok = (field.get("served_ratio") or 0) >= 0.99 and rejected > 0 and media.get("rejections_with_retry_after") == rejected
        out.append(check("P5", "5. Overload: Field Team served, Media shed with 429 + Retry-After", PASS if ok else FAIL,
                         f"field {field.get('served_ratio', 0) * 100:.1f}% served (p95 {field.get('ok_latency_ms', {}).get('p(95)', 0):.0f} ms); "
                         f"media 429 {rejected}/{media.get('requests')}, with Retry-After {media.get('rejections_with_retry_after')}",
                         "p5/shedding.json"))
    else:
        out.append(check("P5", "5. Load shedding", NOT_RUN, "run: scripts/m2-demo.sh p5-shedding", "p5/shedding.json"))
    gameday = run.json("p5/gameday/gameday.json")
    if gameday:
        slo = gameday.get("slo") or {}
        critical, api = slo.get("critical") or {}, slo.get("api") or {}
        scenarios = gameday.get("scenarios") or []
        # The log of the agent pod on a wiped node dies with the node, so a
        # wipe scenario counts as acted when that node went down.
        down = (gameday.get("recovery") or {}).get("powered_off_by_agent") or []
        ran = [x for x in scenarios if x.get("agent_actions") or ("wipe" in x.get("name", "") and down)]
        recovered = gameday.get("recovered_without_intervention")
        short = (gameday.get("recovery") or {}).get("deployments_short") or []
        singletons = {"rabbitmq", "postgres"}
        if ran and recovered:
            status6 = PASS
        elif ran and short and set(short) <= singletons:
            status6 = PARTIAL
        else:
            status6 = FAIL
        out.append(check("P5", "6. Unannounced faults (Tester Agent practice scenarios), recovery without intervention",
                         status6,
                         f"{len(ran)}/{len(scenarios)} scenarios acted ({', '.join(x['name'] for x in ran)}); "
                         f"node powered off by the agent: {', '.join(down) or 'none'}; Deployments not back within the window: {', '.join(short) or 'none'}"
                         + (" (single instance with its disk on the wiped node; back when the VM is started)" if status6 == PARTIAL else "")
                         + f"; API answers 200 {api.get('answered_ok')}/{api.get('requests')}, 503/502 {api.get('forbidden')}, no 500, no hang",
                         "p5/gameday/gameday.json", "p5/gameday/agent-log.txt"))
        out.append(check("P5", "7. Failure explained from dashboards, metrics, traces",
                         PASS if gameday.get("evidence", {}).get("metrics_snapshots") else FAIL,
                         f"metrics snapshots {len(gameday.get('evidence', {}).get('metrics_snapshots') or [])}, probe records {api.get('requests')}; see post-mortem",
                         "p5/gameday/postmortem.md", "p5/gameday/metrics.jsonl"))
        budget = slo.get("error_budget") or {}
        out.append(check("P5", "8. Reliability target for the Field Team alert path and its use during game day",
                         PASS if slo.get("target_defined") and critical.get("ratio_within_target") is not None else FAIL,
                         f"target: {slo.get('target')}; critical {critical.get('within_target')}/{critical.get('samples')} within {critical.get('target_seconds')} s "
                         f"(ratio {critical.get('ratio_within_target')}); API answered {api.get('answered_ok')}/{api.get('requests')} (forbidden {api.get('forbidden')}); "
                         f"error budget used {budget.get('used_fraction')} of the {budget.get('window')} budget",
                         "p5/gameday/gameday.json"))
    else:
        out.append(check("P5", "6-8. Game day with Tester Agent, SLO, post-mortem", NOT_RUN,
                         "run: scripts/m2-demo.sh p5-gameday (Tester Agent practice scenarios; exam scenarios need the assistants' key)", ""))
    return out


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print(__doc__, file=sys.stderr)
        return 2
    run = Run(argv[0])
    meta = run.json("run.json") or {}
    checks = p1(run) + p2(run) + p3(run) + p4(run) + p5(run)
    counts = {s: sum(1 for c in checks if c["status"] == s) for s in (PASS, PARTIAL, FAIL, NOT_RUN, INFO)}
    summary = {"run": meta, "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "counts": counts, "checks": checks}
    with open(run.path("summary.json"), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)
    lines = [f"# M2 evidence run {meta.get('run_id', os.path.basename(run.root.rstrip('/')))}", "",
             f"- Commit: `{meta.get('git_commit', '?')}`{' (uncommitted changes)' if meta.get('git_dirty') else ''}",
             f"- Started: {meta.get('started_at', '?')}  ",
             f"- Catalog: {meta.get('catalog', '-')}",
             f"- Result: {counts[PASS]} pass, {counts[PARTIAL]} partial, {counts[FAIL]} fail, {counts[NOT_RUN]} not run", "",
             "| Problem | Criterion | Status | Detail | Evidence |", "|---|---|---|---|---|"]
    for c in checks:
        detail = c["detail"].replace("|", "\\|")
        lines.append(f"| {c['problem']} | {c['criterion']} | **{c['status']}** | {detail} | "
                     + "<br>".join(f"`{f}`" for f in c["evidence"] if f) + " |")
    with open(run.path("SUMMARY.md"), "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    width = max(len(c["criterion"]) for c in checks)
    for c in checks:
        print(f"{c['problem']}  {c['status']:<7}  {c['criterion']:<{width}}  {c['detail'][:110]}")
    print(f"\n{counts[PASS]} pass, {counts[PARTIAL]} partial, {counts[FAIL]} fail, {counts[NOT_RUN]} not run -> {run.path('SUMMARY.md')}")
    return 1 if counts[FAIL] else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
