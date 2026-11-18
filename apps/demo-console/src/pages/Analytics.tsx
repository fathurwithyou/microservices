import { useMemo, useState } from "react";
import { analytics } from "../api/clientApi";
import { useSessions } from "../state/sessions";
import type { AnalyticsBucket, AnalyticsResponse } from "../types";

const COLORS: Record<string, string> = {
  SEISMIC: "var(--orange)",
  TSUNAMI: "#2f6fd6",
  VOLCANIC: "var(--fail)",
  WILDFIRE: "#e0a23a",
  COASTAL: "#4aa3df",
};

export default function Analytics() {
  const { session } = useSessions();
  const [days, setDays] = useState(7);
  const [bucket, setBucket] = useState<"hour" | "day" | "week">("day");
  const [data, setData] = useState<AnalyticsResponse | null>(null);
  const [message, setMessage] = useState("");

  const load = async () => {
    if (!session) {
      setMessage("Login sebagai Media, Tim Lapangan, atau BNPB Ops terlebih dahulu.");
      return;
    }
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    const response = await analytics(session.accessToken, { since, bucket });
    if (!response.ok || !response.data || !Array.isArray(response.data.hazard_types)) {
      setMessage(response.error || `HTTP ${response.status}: jawaban bukan data analitik`);
      return;
    }
    // The API sends null for an empty list; the page always works on arrays.
    setData({
      ...response.data,
      severities: response.data.severities ?? [],
      areas: response.data.areas ?? [],
      time_buckets: response.data.time_buckets ?? [],
      unavailable_units: response.data.unavailable_units ?? [],
    });
    setMessage(`correlation ${response.correlationId} · ${response.durationMs} ms`);
  };

  const total = data?.hazard_types.reduce((sum, item) => sum + item.count, 0) ?? 0;

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Analytics</h1>
          <p>Agregasi HazardEvent per jenis bahaya, severity, dan wilayah dari Client API.</p>
        </div>
      </div>
      <div className="row" style={{ marginBottom: "0.8rem" }}>
        <select value={days} onChange={(event) => setDays(Number(event.target.value))}>
          <option value={1}>1 hari</option>
          <option value={7}>7 hari</option>
          <option value={30}>30 hari</option>
        </select>
        <select value={bucket} onChange={(event) => setBucket(event.target.value as "hour" | "day" | "week")}>
          <option value="hour">per jam</option>
          <option value="day">per hari</option>
          <option value="week">per minggu</option>
        </select>
        <button className="primary" type="button" onClick={load}>Muat</button>
        <span className="kicker">{message}</span>
      </div>

      {data?.partial ? (
        <div className="panel" style={{ borderColor: "var(--orange)" }}>
          <strong>Data belum lengkap.</strong> Unit yang tidak terjangkau:{" "}
          {data.unavailable_units.length > 0 ? data.unavailable_units.join(", ") : "tidak diketahui"}.
        </div>
      ) : null}
      {data?.as_of ? (
        <p className="kicker" style={{ marginBottom: "0.8rem" }}>
          Snapshot store {new Date(data.as_of).toLocaleTimeString("id-ID")} WIB; angka bisa tertinggal beberapa detik dari kejadian terbaru.
        </p>
      ) : null}

      <div className="grid cols-3" style={{ marginBottom: "0.8rem" }}>
        <div className="panel">
          <div className="kicker">Total kejadian</div>
          <div className="stat">{total}</div>
        </div>
        <div className="panel">
          <div className="kicker">Jenis bahaya</div>
          <div className="stat">{data?.hazard_types.length ?? 0}</div>
        </div>
        <div className="panel">
          <div className="kicker">Wilayah</div>
          <div className="stat">{data?.areas.length ?? 0}</div>
        </div>
      </div>

      <div className="grid cols-2">
        <section className="panel">
          <h3>Jumlah per jenis bahaya</h3>
          <Bars rows={(data?.hazard_types ?? []).map((item) => ({ label: item.hazard_type, value: item.count }))} />
        </section>
        <section className="panel">
          <h3>Jumlah per severity</h3>
          <Bars rows={(data?.severities ?? []).map((item) => ({ label: item.severity, value: item.count }))} />
        </section>
      </div>

      <section className="panel" style={{ marginTop: "0.8rem" }}>
        <h3>Jumlah per wilayah</h3>
        <table>
          <thead><tr><th>Wilayah</th><th>Jumlah</th></tr></thead>
          <tbody>
            {(data?.areas ?? []).map((item) => (
              <tr key={item.area_name}><td>{item.area_name}</td><td>{item.count}</td></tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="panel" style={{ marginTop: "0.8rem" }}>
        <h3>Tren waktu</h3>
        <Trend buckets={data?.time_buckets ?? []} />
      </section>
    </>
  );
}

function Bars({ rows }: { rows: Array<{ label: string; value: number }> }) {
  const max = Math.max(1, ...rows.map((row) => row.value));
  if (rows.length === 0) return <p className="kicker">Belum ada data.</p>;
  return (
    <div style={{ display: "grid", gap: "0.45rem" }}>
      {rows.map((row) => (
        <div key={row.label}>
          <div className="row" style={{ justifyContent: "space-between" }}>
            <span>{row.label}</span><span className="kicker">{row.value}</span>
          </div>
          <div style={{ height: 6, background: "var(--hover)", borderRadius: 3 }}>
            <div style={{ height: 6, width: `${(row.value / max) * 100}%`, background: COLORS[row.label] ?? "var(--accent)", borderRadius: 3 }} />
          </div>
        </div>
      ))}
    </div>
  );
}

function Trend({ buckets }: { buckets: AnalyticsBucket[] }) {
  const series = useMemo(() => {
    const byType = new Map<string, AnalyticsBucket[]>();
    for (const bucket of buckets) {
      const list = byType.get(bucket.hazard_type) ?? [];
      list.push(bucket);
      byType.set(bucket.hazard_type, list);
    }
    const labels = Array.from(new Set(buckets.map((bucket) => bucket.bucket_start))).sort();
    return { byType, labels };
  }, [buckets]);

  if (buckets.length === 0) return <p className="kicker">Belum ada data.</p>;
  const max = Math.max(1, ...buckets.map((bucket) => bucket.count));
  const width = 640;
  const height = 180;
  const step = series.labels.length > 1 ? width / (series.labels.length - 1) : width;

  return (
    <div>
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Tren hazard" style={{ width: "100%", height: "auto" }}>
        <line x1="0" y1={height} x2={width} y2={height} stroke="var(--border)" />
        {Array.from(series.byType.entries()).map(([type, points]) => {
          const ordered = points.slice().sort((a, b) => a.bucket_start.localeCompare(b.bucket_start));
          const path = ordered
            .map((point) => {
              const x = series.labels.indexOf(point.bucket_start) * step;
              const y = height - (point.count / max) * (height - 10);
              return `${x},${y}`;
            })
            .join(" ");
          return <polyline key={type} points={path} fill="none" stroke={COLORS[type] ?? "var(--accent)"} strokeWidth="2" />;
        })}
      </svg>
      <div className="row">
        {Array.from(series.byType.keys()).map((type) => (
          <span key={type} className="pill" style={{ color: COLORS[type] ?? "var(--accent)" }}>{type}</span>
        ))}
      </div>
    </div>
  );
}
