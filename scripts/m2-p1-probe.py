#!/usr/bin/env python3
"""M2 P1.4/P1.5 evidence: keep reading while a store node is lost or added.

Every --interval seconds, for --duration seconds, calls

* GET /hazards?page_size=50          (Client API, Field Team token)
* GET /analytics                     (Client API)
* GET /internal/audit/checksum?source=BMKG  (Aggregator Audit API)
* GET /internal/audit/distribution   (Aggregator Audit API)

with a hard per-request timeout, and records status, latency, and the
`partial` / `unavailable_units` markers. The summary counts 5xx answers and
requests that did not answer within the timeout (the two outcomes P1.4
forbids), and the periods during which answers were marked partial.

Credentials come from the environment: FIELD_TEAM_CLIENT_SECRET, AUDIT_TOKEN.

With --client-api-pods / --aggregator-pods (headless Service names) the probe
balances every request over the pods itself (see PodBalancer) instead of
going through the ClusterIP, so a pod on a node that just lost power costs one
connect timeout, never a hung request.

Example:
    python3 scripts/m2-p1-probe.py --duration 900 --out artifacts/m2/p1/probe-node-loss.json
"""

from __future__ import annotations

import argparse
import collections
import http.client
import json
import os
import random
import socket
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from importlib import import_module

crossread = import_module("m2-p1-crossread")


class PodBalancer:
    """Client-side balancing over the pods behind a headless Service.

    Kubernetes keeps routing a share of new ClusterIP connections to the pods
    of a node that lost power until the node is marked NotReady
    (node-monitor-grace-period, 40 s by default); those connections hang until
    the client gives up. A client that connects to the pod addresses itself
    can abandon a silent pod after connect_timeout, eject it for eject_seconds
    and move to the next one, the same way the Client API balances its gRPC
    calls per attempt. The read timeout is the remaining request budget, so a
    slow-but-alive pod is never mistaken for a dead one.
    """

    def __init__(self, host: str, port: int, connect_timeout: float, eject_seconds: float):
        self.host, self.port = host, port
        self.connect_timeout, self.eject_seconds = connect_timeout, eject_seconds
        self.known: list[str] = []
        self.resolved_at = 0.0
        self.ejected: dict[str, float] = {}

    def addresses(self) -> list[str]:
        if time.time() - self.resolved_at > 2:
            try:
                found = sorted({info[4][0] for info in socket.getaddrinfo(self.host, self.port, type=socket.SOCK_STREAM)})
                if found:
                    self.known = found
            except OSError:
                pass  # keep the last known set while DNS is unavailable
            self.resolved_at = time.time()
        now = time.time()
        healthy = [ip for ip in self.known if self.ejected.get(ip, 0) <= now]
        suspect = [ip for ip in self.known if self.ejected.get(ip, 0) > now]
        random.shuffle(healthy)
        return healthy + suspect

    def call(self, path: str, *, token: str | None, budget: float, body: dict | None = None) -> tuple[int, dict, float, list[dict]]:
        started = time.time()
        deadline = started + budget
        attempts: list[dict] = []
        payload = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        for ip in self.addresses():
            remaining = deadline - time.time()
            if remaining <= 0.05:
                break
            attempt_started = time.time()
            conn = http.client.HTTPConnection(ip, self.port, timeout=min(self.connect_timeout, remaining))
            try:
                conn.connect()
                conn.sock.settimeout(max(0.05, deadline - time.time()))
                conn.request("POST" if payload is not None else "GET", path, body=payload, headers=headers)
                response = conn.getresponse()
                raw = response.read()
                try:
                    data = json.loads(raw) if raw else {}
                except json.JSONDecodeError:
                    data = {"raw": raw[:300].decode(errors="replace")}
                if attempts:
                    data = dict(data) if isinstance(data, dict) else {"body": data}
                    data["_attempts"] = attempts + [{"ip": ip, "ok": True}]
                return response.status, data, time.time() - started, attempts
            except (OSError, http.client.HTTPException) as error:
                # Silent or broken pod: remember it and move on within the budget.
                self.ejected[ip] = time.time() + self.eject_seconds
                attempts.append({"ip": ip, "error": type(error).__name__, "seconds": round(time.time() - attempt_started, 3)})
            finally:
                conn.close()
        return 0, {"error": "no pod answered within the budget", "_attempts": attempts}, time.time() - started, attempts


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--client-api", default="http://localhost:18081")
    parser.add_argument("--aggregator", default="http://localhost:18090")
    parser.add_argument("--duration", type=float, default=600)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--label", default="probe")
    parser.add_argument("--out", required=True)
    parser.add_argument("--stop-file", help="finish early (and write the result) once this file exists")
    parser.add_argument("--client-api-pods", help="headless Service of the client-api pods (host[:port], default port 8080): balance per request over the pods instead of using --client-api")
    parser.add_argument("--aggregator-pods", help="headless Service of the aggregator pods (host[:port], default port 8090): balance per request over the pods instead of using --aggregator")
    parser.add_argument("--connect-timeout", type=float, default=1.0, help="per-pod connect timeout when balancing (seconds)")
    parser.add_argument("--eject-seconds", type=float, default=15.0, help="how long a pod that did not answer is skipped when balancing")
    args = parser.parse_args(argv)

    def balancer(spec: str | None, default_port: int) -> PodBalancer | None:
        if not spec:
            return None
        host, _, port = spec.partition(":")
        return PodBalancer(host, int(port or default_port), args.connect_timeout, args.eject_seconds)

    balancers = {"client": balancer(args.client_api_pods, 8080), "audit": balancer(args.aggregator_pods, 8090)}

    def request(kind: str, url: str, *, token: str | None, body: dict | None = None) -> tuple[int, dict, float]:
        """One request through the pod balancer of kind when configured, else through the Service URL."""
        chosen = balancers[kind]
        if chosen is None:
            return crossread.call(url, token=token, body=body, timeout=args.timeout)
        path = url.split("/", 3)[3] if url.count("/") >= 3 else ""
        status, data, seconds, _ = chosen.call("/" + path, token=token, budget=args.timeout, body=body)
        return status, data, seconds

    secret, audit_token = os.environ.get("FIELD_TEAM_CLIENT_SECRET"), os.environ.get("AUDIT_TOKEN")
    if not secret or not audit_token:
        print("FIELD_TEAM_CLIENT_SECRET and AUDIT_TOKEN are required", file=sys.stderr)
        return 2

    token = {"value": None}

    def client_token() -> str | None:
        status, body, _ = request("client", f"{args.client_api}/auth/token", token=None, body={"client_id": "field-team-client", "client_secret": secret})
        token["value"] = body.get("access_token") if status == 200 else None
        return token["value"]

    client_token()
    endpoints = [
        ("hazards", lambda: f"{args.client_api}/hazards?page_size=50", "client"),
        ("analytics", lambda: f"{args.client_api}/analytics", "client"),
        ("audit_checksum", lambda: f"{args.aggregator}/internal/audit/checksum?source=BMKG", "audit"),
        ("audit_distribution", lambda: f"{args.aggregator}/internal/audit/distribution", "audit"),
    ]
    records = []
    started = time.time()
    deadline = started + args.duration

    def flush() -> None:
        summary = collections.defaultdict(lambda: collections.Counter())
        latency = collections.defaultdict(float)
        partial_windows = collections.defaultdict(list)
        for record in records:
            name = record["endpoint"]
            status = record["status"]
            summary[name]["total"] += 1
            summary[name]["timeout_or_no_answer" if status == 0 else ("5xx" if status >= 500 else str(status))] += 1
            latency[name] = max(latency[name], record["seconds"])
            if record.get("partial"):
                windows = partial_windows[name]
                if windows and record["t"] - windows[-1][1] <= args.interval * 3 + args.timeout:
                    windows[-1][1] = record["t"]
                else:
                    windows.append([record["t"], record["t"]])
        retried = sum(1 for record in records if record.get("attempts"))
        result = {
            "label": args.label,
            "started_at": datetime.fromtimestamp(started, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "duration_seconds": round(time.time() - started, 1),
            "per_request_timeout_seconds": args.timeout,
            "pod_balancing": {"client_api": args.client_api_pods, "aggregator": args.aggregator_pods,
                              "connect_timeout_seconds": args.connect_timeout, "eject_seconds": args.eject_seconds,
                              "requests_that_skipped_a_silent_pod": retried} if (args.client_api_pods or args.aggregator_pods) else None,
            "summary": {name: dict(counter) for name, counter in summary.items()},
            "max_seconds": {name: round(value, 3) for name, value in latency.items()},
            "partial_windows_offset_seconds": {name: [[round(a, 1), round(b, 1)] for a, b in w] for name, w in partial_windows.items()},
            "forbidden_outcomes": sum(c.get("5xx", 0) + c.get("timeout_or_no_answer", 0) for c in summary.values()),
            "records": records,
        }
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        tmp = args.out + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=1)
            handle.write("\n")
        os.replace(tmp, args.out)

    last_flush = time.time()
    while time.time() < deadline and not (args.stop_file and os.path.exists(args.stop_file)):
        round_start = time.time()
        for name, url, kind in endpoints:
            auth = audit_token if kind == "audit" else (token["value"] or client_token())
            status, body, seconds = request(kind, url(), token=auth)
            if kind == "client" and status == 401:
                client_token()
                status, body, seconds = request(kind, url(), token=token["value"])
            record = {"t": round(round_start - started, 2), "endpoint": name, "status": status, "seconds": round(seconds, 3)}
            if isinstance(body, dict):
                if body.get("_attempts"):
                    # Pods that did not answer and were skipped on the way to this answer.
                    record["attempts"] = body["_attempts"]
                if "partial" in body:
                    record["partial"] = body.get("partial")
                if body.get("unavailable_units"):
                    record["unavailable_units"] = body["unavailable_units"]
                if name == "audit_checksum" and "count" in body:
                    record["count"] = body["count"]
                if name == "audit_distribution" and "nodes" in body:
                    record["nodes"] = {f"{n['node']}#{n['node_id']}": [n["live"], n["replicas"], n["leaseholders"]] for n in body["nodes"]}
                    record["ranges"] = body.get("ranges")
                if status >= 500 or status == 0:
                    record["error"] = str(body)[:300]
            records.append(record)
        if time.time() - last_flush > 10:
            flush()
            last_flush = time.time()
        time.sleep(max(0.0, args.interval - (time.time() - round_start)))
    flush()
    # Fallback for the runner: a finished container cannot be exec'd into, so
    # the whole result also goes to stdout as one compact line.
    with open(args.out, encoding="utf-8") as handle:
        print(json.dumps(json.load(handle), separators=(",", ":")))
    print(f"wrote {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
