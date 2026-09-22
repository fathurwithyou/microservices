import { useEvidence } from "../state/evidence";

export default function EvidenceLog() {
  const { entries, download, clear } = useEvidence();
  return (
    <section className="panel" style={{ marginTop: "1rem" }}>
      <div className="page-head">
        <h2>Evidence recorder</h2>
        <div className="row">
          <button type="button" onClick={download}>Download JSON</button>
          <button type="button" onClick={clear}>Clear</button>
        </div>
      </div>
      {entries.length === 0 ? <p className="kicker">Belum ada assertion. Jalankan Scenario Lab agar status PASS/FAIL tercatat otomatis.</p> : (
        <table>
          <thead>
            <tr>
              <th>Status</th>
              <th>Problem</th>
              <th>Criterion</th>
              <th>HTTP</th>
              <th>Correlation</th>
            </tr>
          </thead>
          <tbody>
            {entries.slice(0, 12).map((entry) => (
              <tr key={entry.id}>
                <td><span className={`pill ${entry.status === "PASS" ? "ok" : entry.status === "FAIL" ? "fail" : "wait"}`}>{entry.status}</span></td>
                <td>{entry.problem}</td>
                <td>{entry.criterion}<div className="kicker">{entry.expected} → {entry.actual}</div></td>
                <td className="mono">{entry.method} {entry.url} ({entry.durationMs}ms)</td>
                <td className="mono">{entry.correlationId}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
