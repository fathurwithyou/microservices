import { useEffect, useState } from "react";
import { getArtifact, listArtifacts } from "../api/demoApi";
import { useEvidence } from "../state/evidence";

export default function TestEvidence() {
  const { entries, download } = useEvidence();
  const [names, setNames] = useState<string[]>([]);
  const [selected, setSelected] = useState("");
  const [content, setContent] = useState<unknown>(null);
  const [message, setMessage] = useState("Artifact JSON dihasilkan `make test-e2e` / `make test-load` dan dibaca demo-api dari direktori artifacts/.");

  useEffect(() => {
    void listArtifacts().then((response) => {
      setNames(response.data?.artifacts ?? []);
      if (!response.ok) setMessage(response.error || "demo-api artifacts unavailable");
    });
  }, []);

  const open = async (name: string) => {
    setSelected(name);
    const response = await getArtifact(name);
    setContent(response.data?.content ?? { error: response.error });
  };

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Test evidence</h1>
          <p>Live recorder di browser plus artifact k6/E2E. UI tidak mengklaim angka load test dari pengukuran browser.</p>
        </div>
        <button type="button" onClick={download}>Download live evidence</button>
      </div>
      <div className="grid cols-2">
        <section className="panel">
          <h2>Generated artifacts</h2>
          <p className="kicker">{message}</p>
          {names.length === 0 ? <p>Belum ada file di artifacts/. Jalankan make test-e2e atau make test-load.</p> : (
            <ul className="steps">
              {names.map((name) => (
                <li key={name}>
                  <button type="button" className={selected === name ? "primary" : ""} onClick={() => void open(name)}>{name}</button>
                </li>
              ))}
            </ul>
          )}
        </section>
        <section className="panel">
          <h2>Artifact content</h2>
          <pre>{content ? JSON.stringify(content, null, 2) : "Pilih artifact"}</pre>
        </section>
      </div>
      <section className="panel" style={{ marginTop: "0.9rem" }}>
        <h2>Live assertion ledger ({entries.length})</h2>
        <pre>{JSON.stringify(entries, null, 2)}</pre>
      </section>
    </>
  );
}
