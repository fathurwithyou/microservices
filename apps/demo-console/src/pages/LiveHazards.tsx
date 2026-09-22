import { useState } from "react";
import { getHazard, listHazards } from "../api/clientApi";
import { canRequestRaw, severityTone } from "../lib/clients";
import { isSummaryOnly, requiredCanonicalFields } from "../lib/assertions";
import { useEvidence, statusFromOk } from "../state/evidence";
import { useSessions } from "../state/sessions";
import type { RawHazard, SummaryHazard } from "../types";

export default function LiveHazards() {
  const { session } = useSessions();
  const { record } = useEvidence();
  const [source, setSource] = useState("");
  const [hazardType, setHazardType] = useState("");
  const [raw, setRaw] = useState(false);
  const [hazards, setHazards] = useState<Array<SummaryHazard | RawHazard>>([]);
  const [selected, setSelected] = useState<SummaryHazard | RawHazard | null>(null);
  const [compare, setCompare] = useState<Array<SummaryHazard | RawHazard>>([]);
  const [message, setMessage] = useState("");

  const load = async () => {
    if (!session) {
      setMessage("Login salah satu client terlebih dahulu.");
      return;
    }
    const response = await listHazards(session.accessToken, {
      source: source || undefined,
      hazardType: hazardType || undefined,
      raw,
      pageSize: 50,
    });
    const first = response.data?.hazards[0];
    const assertion = response.status === 403
      ? { ok: true, expected: "Media raw request rejected with 403", actual: `HTTP ${response.status}` }
      : response.ok && response.data
        ? first
          ? requiredCanonicalFields(first)
          : { ok: true, expected: "HTTP 200 hazard list", actual: "empty list" }
        : { ok: false, expected: "HTTP 200 hazard list", actual: `HTTP ${response.status} ${response.error}` };
    record({
      problem: "GENERAL",
      criterion: raw ? "list hazards raw" : "list hazards summary",
      method: response.method,
      url: response.url,
      correlationId: response.correlationId,
      durationMs: response.durationMs,
      expected: assertion.expected,
      actual: assertion.actual,
      status: statusFromOk(response.ok ? assertion.ok : response.status === 403),
    });
    if (response.status === 403) {
      setMessage("Server menolak raw scope (403). Ini bukti P3, bukan penyembunyian UI.");
      setHazards([]);
      return;
    }
    if (!response.ok || !response.data) {
      setMessage(response.error || "gagal memuat hazards");
      return;
    }
    if (!raw) {
      const leaks = response.data.hazards.filter((hazard) => !isSummaryOnly(hazard as Record<string, unknown>).ok);
      if (leaks.length > 0) {
        record({
          problem: "P3",
          criterion: "summary must not include raw fields",
          method: response.method,
          url: response.url,
          correlationId: response.correlationId,
          durationMs: response.durationMs,
          expected: "no latitude/longitude/attributes",
          actual: `${leaks.length} leaked records`,
          status: "FAIL",
        });
      }
    }
    setHazards(response.data.hazards);
    setMessage(`${response.data.hazards.length} records · correlation ${response.correlationId}`);
  };

  const inspect = async (hazardId: string) => {
    if (!session) return;
    const response = await getHazard(session.accessToken, hazardId, raw);
    if (response.ok && response.data) {
      setSelected(response.data);
    }
  };

  const toggleCompare = (hazard: SummaryHazard | RawHazard) => {
    setCompare((current) => {
      if (current.some((item) => item.hazard_id === hazard.hazard_id)) {
        return current.filter((item) => item.hazard_id !== hazard.hazard_id);
      }
      return [...current, hazard].slice(-2);
    });
  };

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Live hazards</h1>
          <p>Filter sumber, ringkasan vs mentah, dan bandingkan dua payload. Scope ditegakkan di Client API.</p>
        </div>
      </div>
      <section className="panel">
        <div className="row">
          <select value={source} onChange={(event) => setSource(event.target.value)}>
            <option value="">Semua sumber</option>
            <option value="BMKG">BMKG</option>
            <option value="PVMBG">PVMBG</option>
          </select>
          <select value={hazardType} onChange={(event) => setHazardType(event.target.value)}>
            <option value="">Semua tipe</option>
            <option value="SEISMIC">SEISMIC</option>
            <option value="VOLCANIC">VOLCANIC</option>
          </select>
          <label>
            <input type="checkbox" checked={raw} onChange={(event) => setRaw(event.target.checked)} /> raw=true
          </label>
          <button className="primary" type="button" onClick={() => void load()}>Fetch</button>
          {session && !canRequestRaw(session.scopes) ? <span className="pill watch">Media: raw request will hit 403</span> : null}
        </div>
        <p className="kicker">{message}</p>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Source</th>
              <th>Type</th>
              <th>Severity</th>
              <th>Area</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {hazards.map((hazard) => (
              <tr key={hazard.hazard_id}>
                <td className="mono">{hazard.hazard_id}</td>
                <td>{hazard.source}</td>
                <td>{hazard.hazard_type}</td>
                <td><span className={`pill ${severityTone(hazard.severity)}`}>{hazard.severity}</span></td>
                <td>{hazard.area_name}</td>
                <td className="row">
                  <button type="button" onClick={() => void inspect(hazard.hazard_id)}>Detail</button>
                  <button type="button" onClick={() => toggleCompare(hazard)}>Compare</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
      {selected ? (
        <section className="panel" style={{ marginTop: "0.9rem" }}>
          <h2>Detail {selected.hazard_id}</h2>
          <pre>{JSON.stringify(selected, null, 2)}</pre>
        </section>
      ) : null}
      {compare.length === 2 ? (
        <section className="panel" style={{ marginTop: "0.9rem" }}>
          <h2>Compare</h2>
          <div className="compare">
            {compare.map((item) => (
              <pre key={item.hazard_id}>{JSON.stringify(item, null, 2)}</pre>
            ))}
          </div>
        </section>
      ) : null}
    </>
  );
}
