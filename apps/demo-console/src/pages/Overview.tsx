import { useEffect, useState } from "react";
import { fetchOwnership, fetchQueues, fetchServiceHealth } from "../api/demoApi";
import { useSessions } from "../state/sessions";
import { sourceStatus } from "../api/clientApi";
import { sourceName } from "../lib/clients";
import type { HealthProbe, QueueSnapshot, SourceStatus } from "../types";

export default function Overview() {
  const { session } = useSessions();
  const [health, setHealth] = useState<HealthProbe[]>([]);
  const [queues, setQueues] = useState<QueueSnapshot[]>([]);
  const [ownership, setOwnership] = useState<Record<string, unknown> | null>(null);
  const [sources, setSources] = useState<SourceStatus[]>([]);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      const [healthResponse, queueResponse, ownershipResponse] = await Promise.all([
        fetchServiceHealth(),
        fetchQueues(),
        fetchOwnership(),
      ]);
      if (cancelled) return;
      setHealth(healthResponse.data?.services ?? []);
      setQueues(queueResponse.data?.queues ?? []);
      setOwnership(ownershipResponse.data);
      if (!healthResponse.ok) setError(healthResponse.error || "demo-api health unavailable");
    };
    void load();
    const timer = window.setInterval(() => void load(), 4000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    if (!session?.accessToken) return;
    let cancelled = false;
    const load = async () => {
      const [bmkg, pvmbg] = await Promise.all([
        sourceStatus(session.accessToken, "BMKG"),
        sourceStatus(session.accessToken, "PVMBG"),
      ]);
      if (!cancelled) {
        setSources([bmkg.data, pvmbg.data].filter(Boolean) as SourceStatus[]);
      }
    };
    void load();
    const timer = window.setInterval(() => void load(), 4000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [session?.accessToken]);

  return (
    <>
      <div className="page-head">
        <div>
          <h1>System overview</h1>
          <p>Health live, ownership Canonical Store, dan antrian fan-out. Restart/rebuild tetap dijalankan dari terminal sesuai runbook.</p>
        </div>
      </div>
      {error ? <p className="pill fail">{error}</p> : null}
      <section className="panel">
        <h2>Topology / health</h2>
        <div className="topology">
          {health.map((probe) => (
            <div className="node" key={probe.name}>
              <strong>{probe.name}</strong>
              <p className={`stat ${probe.status === "healthy" ? "" : probe.status === "outage" ? "warn" : "down"}`}>{probe.status}</p>
              {probe.optional ? <small>optional / portal profile</small> : <small>{probe.detail || "HTTP or TCP probe via demo-api"}</small>}
            </div>
          ))}
        </div>
      </section>
      <div className="grid cols-2" style={{ marginTop: "0.9rem" }}>
        <section className="panel">
          <h2>Source freshness</h2>
          {sources.length === 0 ? <p className="kicker">Login Tim Lapangan atau BNPB Ops untuk membaca /sources/status.</p> : sources.map((item) => (
            <p key={String(item.source)}>
              <strong>{sourceName(item.source)}</strong>{" "}
              <span className={`pill ${item.status === "healthy" ? "ok" : "watch"}`}>{item.status}</span>
              <span className="kicker"> last_success={item.last_success_at || "-"} stale_since={item.stale_since || "-"}</span>
            </p>
          ))}
        </section>
        <section className="panel">
          <h2>Fan-out queues</h2>
          <table>
            <thead><tr><th>Queue</th><th>Messages</th><th>Consumers</th></tr></thead>
            <tbody>
              {queues.map((queue) => (
                <tr key={queue.name}>
                  <td className="mono">{queue.name}</td>
                  <td className="stat" style={{ fontSize: "1.15rem" }}>{queue.messages}</td>
                  <td>{queue.consumers}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      </div>
      <section className="panel" style={{ marginTop: "0.9rem" }}>
        <h2>Storage ownership</h2>
        <p>{String(ownership?.statement ?? "Memuat pernyataan ownership...")}</p>
        <p className="kicker">canonical_store_owner={String(ownership?.canonical_store_owner ?? "-")} · demo_api_database_access={String(ownership?.demo_api_database_access ?? "-")} · demo_api_docker_access={String(ownership?.demo_api_docker_access ?? "-")}</p>
        <p className="trace">P4: UI hanya menampilkan health/timeline. Bukti rebuild independen dan JSONB coexistence berasal dari artifact E2E, bukan dari akses DB di browser.</p>
      </section>
    </>
  );
}
