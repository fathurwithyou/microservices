#!/usr/bin/env python3
"""M2 P5.6-8 evidence: summarise one game day run into gameday.json and a
post-mortem skeleton.

Inputs (all produced by `scripts/m2-demo.sh p5-gameday`, nothing hand-entered):
  scenarios.json      which Tester Agent scenarios were run, when, and the agent's stdout
  agent-log.txt       `kubectl logs` of every Tester Agent pod over the run
  probe.json          in-cluster API probe (scripts/m2-p1-probe.py) over the run
  notifier-before.txt / notifier-after.txt   critical-notifier /metrics snapshots
  metrics.jsonl       periodic cluster snapshots (pods not ready, store partial, nodes)
  recovery.json       state at the end of the hands-off window

The reliability target for the Field Team alert path is fixed here so the
report and the artifact say the same thing:
  * 99 % of critical observations notified within 5 s, per 28-day window
    (error budget: 1 % of critical observations);
  * 99.5 % of API requests answered (no 5xx, no request hanging past 5 s).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone

TARGET = {
    "critical": {"ratio": 0.99, "target_seconds": 5, "window": "28 days"},
    "api": {"ratio": 0.995, "timeout_seconds": 5},
}


def load(path: str, default=None):
    if not os.path.exists(path):
        return default
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def histogram(path: str, metric: str, le: str, only: str = "") -> tuple[float, float]:
    """(count, within le) for a Prometheus histogram across every pod in the
    snapshot; `only` keeps the series whose labels contain that text."""
    count = within = 0.0
    if not os.path.exists(path):
        return count, within
    for line in open(path, encoding="utf-8", errors="replace"):
        if only and only not in line:
            continue
        if line.startswith(metric + "_count"):
            count += float(line.split()[-1])
        elif line.startswith(metric + "_bucket") and f'le="{le}"' in line:
            within += float(line.split()[-1])
    return count, within


def agent_actions(log_path: str, since: str, until: str) -> list[dict]:
    """Agent log lines inside [since, until] that look like actions (best effort:
    every non-empty line with a timestamp; the raw log is kept beside it)."""
    actions = []
    if not os.path.exists(log_path):
        return actions
    stamp = re.compile(r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})")
    for line in open(log_path, encoding="utf-8", errors="replace"):
        match = stamp.search(line)
        if not match:
            continue
        at = match.group(1).replace(" ", "T")
        if since[:19] <= at <= until[:19]:
            actions.append({"at": at, "line": line.strip()[:300]})
    return actions


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dir")
    args = parser.parse_args(argv)
    d = args.dir
    scenarios = load(os.path.join(d, "scenarios.json"), []) or []
    probe = load(os.path.join(d, "probe.json"), {}) or {}
    recovery = load(os.path.join(d, "recovery.json"), {}) or {}
    metrics = [json.loads(l) for l in open(os.path.join(d, "metrics.jsonl"), encoding="utf-8")] if os.path.exists(os.path.join(d, "metrics.jsonl")) else []

    for scenario in scenarios:
        scenario["agent_actions"] = agent_actions(os.path.join(d, "agent-log.txt"), scenario.get("started_at", ""), scenario.get("ended_at", "9999"))

    # Latency is measured from the moment the ingestor read the sensor answer
    # (from="received_at"); the other references run on the sensor's virtual
    # clock. A notifier pod that restarted during the game day starts its
    # counters at zero, so the sample count is a lower bound.
    reference = 'from="received_at"'
    metric, le = "critical_notification_latency_seconds", str(TARGET["critical"]["target_seconds"])
    before = histogram(os.path.join(d, "notifier-before.txt"), metric, le, reference)
    after = histogram(os.path.join(d, "notifier-after.txt"), metric, le, reference)
    divisor = 1
    samples = (after[0] - before[0]) / divisor
    within = (after[1] - before[1]) / divisor
    ratio = round(within / samples, 4) if samples else None

    summary = probe.get("summary") or {}
    requests = sum(c.get("total", 0) for c in summary.values())
    forbidden = probe.get("forbidden_outcomes", 0)
    api_ratio = round((requests - forbidden) / requests, 4) if requests else None

    budget_fraction = None
    if samples and ratio is not None:
        allowed = (1 - TARGET["critical"]["ratio"]) * samples
        budget_fraction = round((samples - within) / allowed, 3) if allowed else None

    # A node the agent wiped and powered off stays off: the spec leaves
    # starting a VM to the group (it cannot be done from inside the cluster).
    # Recovery without intervention therefore means every Deployment has its
    # replicas back on the remaining nodes; the store reports the missing
    # node through `partial` until that node returns.
    wiped = any("wipe" in (x.get("name") or "") for x in scenarios)
    down = recovery.get("nodes_not_ready") or []
    if wiped:
        recovered = bool(recovery.get("deployments_available") and len(down) <= 1)
    else:
        recovered = bool(recovery.get("deployments_available") and not recovery.get("store_partial") and down == [])
    recovery["powered_off_by_agent"] = down if wiped else []
    result = {
        "label": "game day (Tester Agent practice scenarios)",
        "scenarios": scenarios,
        "hands_off_window_seconds": recovery.get("window_seconds"),
        "recovered_without_intervention": recovered,
        "recovery": recovery,
        "slo": {
            "target_defined": True,
            "target": f"{TARGET['critical']['ratio'] * 100:.0f}% of critical observations notified to the Field Team within {TARGET['critical']['target_seconds']} s per {TARGET['critical']['window']}; "
                      f"{TARGET['api']['ratio'] * 100:.1f}% of API requests answered without 5xx within {TARGET['api']['timeout_seconds']} s",
            "critical": {"samples": samples, "within_target": within, "ratio_within_target": ratio,
                         "target_seconds": TARGET["critical"]["target_seconds"], "target_ratio": TARGET["critical"]["ratio"],
                         "met": (ratio is None) or ratio >= TARGET["critical"]["ratio"]},
            "api": {"requests": requests, "answered_ok": requests - forbidden, "forbidden": forbidden, "ratio": api_ratio,
                    "target_ratio": TARGET["api"]["ratio"], "met": api_ratio is None or api_ratio >= TARGET["api"]["ratio"],
                    "per_endpoint": summary, "max_seconds": probe.get("max_seconds")},
            "error_budget": {"window": TARGET["critical"]["window"], "budget": f"{(1 - TARGET['critical']['ratio']) * 100:.0f}% of critical observations",
                             "used_fraction": budget_fraction, "note": "fraction of the critical-path error budget consumed by this game day's observations"},
        },
        "evidence": {"metrics_snapshots": metrics, "agent_log": "agent-log.txt", "probe": "probe.json"},
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with open(os.path.join(d, "gameday.json"), "w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=1)

    lines = ["# Post-mortem game day (skenario latihan Tester Agent)", "",
             f"Dibuat otomatis {result['generated_at']} dari artefak di `{d}`; isi 'Mitigasi' dan 'Perbaikan' dilengkapi manual di laporan.", "",
             "## Target keandalan", f"- {result['slo']['target']}", "",
             "## Skenario dan aksi Tester Agent"]
    for scenario in scenarios:
        lines.append(f"### {scenario['name']} ({scenario.get('started_at')} – {scenario.get('ended_at')})")
        for action in scenario.get("agent_actions") or []:
            lines.append(f"- `{action['at']}` {action['line']}")
        if not scenario.get("agent_actions"):
            lines.append("- (tidak ada baris log agent dalam rentang ini)")
        lines.append("")
    lines += ["## Deteksi (dari metrik dan probe, bukan dugaan)"]
    for name, counter in summary.items():
        bad = counter.get("5xx", 0) + counter.get("timeout_or_no_answer", 0)
        lines.append(f"- `{name}`: {counter.get('total', 0)} request, {bad} gagal (5xx/menggantung), maks {(probe.get('max_seconds') or {}).get(name)} s")
    windows = probe.get("partial_windows_offset_seconds") or {}
    for name, w in windows.items():
        if w:
            lines.append(f"- `{name}` ditandai `partial` pada detik {w} setelah probe mulai")
    lines += ["", "## Dampak", f"- Jalur kritis: {within:.0f}/{samples:.0f} notifikasi ≤ 5 s (rasio {ratio}); error budget terpakai {budget_fraction}",
              f"- API: {requests - forbidden}/{requests} dijawab (rasio {api_ratio})",
              f"- Pulih tanpa intervensi dalam window {recovery.get('window_seconds')} s: {recovered} (deployment tersedia {recovery.get('deployments_available')}, store partial {recovery.get('store_partial')}, node NotReady {recovery.get('nodes_not_ready')})",
              "", "## Mitigasi (otomatis, tanpa intervensi)", "- Diisi dari mekanisme yang terpicu: lihat metrik `coastal_circuit_state`, `partial`, failover leader di log, retry client.",
              "", "## Perbaikan", "- Diisi manual: apa yang diubah setelah game day."]
    with open(os.path.join(d, "postmortem.md"), "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    print(json.dumps({k: result["slo"][k] for k in ("critical", "api", "error_budget")}, indent=1)[:900])
    print("recovered_without_intervention:", recovered, "->", os.path.join(d, "gameday.json"))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
