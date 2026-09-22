import { useState } from "react";
import { listHazards, sourceStatus } from "../api/clientApi";
import { fetchOwnership, fetchQueues, fetchServiceHealth, runCrossCredentials, toggleOutage, toggleSchema } from "../api/demoApi";
import type { RecordedResponse } from "../api/http";
import { hasConfidenceLevel, httpExpected, isSummaryOnly, requiredCanonicalFields, splitByConfidence, statusIsStale } from "../lib/assertions";
import { isExpired } from "../lib/clients";
import { statusFromOk, useEvidence } from "../state/evidence";
import { useSessions } from "../state/sessions";
import type { RawHazard, Session } from "../types";

type ProblemId = "P1" | "P2" | "P3" | "P4" | "P5";

export default function ScenarioLab() {
  const [problem, setProblem] = useState<ProblemId>("P1");
  return (
    <>
      <div className="page-head">
        <div>
          <h1>Scenario Lab</h1>
          <p>Setiap langkah merekam HTTP nyata. Tidak ada tombol “pass” manual. P4/P5 lifecycle container tetap dijalankan dari terminal.</p>
        </div>
      </div>
      <div className="row" style={{ marginBottom: "0.8rem" }}>
        {(["P1", "P2", "P3", "P4", "P5"] as const).map((id) => (
          <button key={id} className={problem === id ? "primary" : ""} type="button" onClick={() => setProblem(id)}>{id}</button>
        ))}
      </div>
      {problem === "P1" ? <Problem1 /> : null}
      {problem === "P2" ? <Problem2 /> : null}
      {problem === "P3" ? <Problem3 /> : null}
      {problem === "P4" ? <Problem4 /> : null}
      {problem === "P5" ? <Problem5 /> : null}
    </>
  );
}

function Problem1() {
  const { session } = useSessions();
  const { record } = useEvidence();
  const [log, setLog] = useState<string[]>([]);
  const [before, setBefore] = useState<RawHazard[]>([]);
  const [after, setAfter] = useState<RawHazard[]>([]);
  const [busy, setBusy] = useState(false);

  const run = async () => {
    if (!requireRawSession(session, setLog)) return;
    setBusy(true);
    try {
      const baseline = await listHazards(session.accessToken, { source: "PVMBG", raw: true, pageSize: 50 });
      note(record, setLog, "P1", "1. baseline PVMBG HazardEvent", baseline, requiredCanonicalFields((baseline.data?.hazards[0] ?? emptyHazard()) as RawHazard));
      setBefore((baseline.data?.hazards ?? []) as RawHazard[]);
      const toggle = await toggleSchema(true);
      note(record, setLog, "P1", "2. POST schema-version via demo-api, no restart", toggle, httpExpected(toggle.status, [200]));
      const evolved = await waitFor(20_000, async () => {
        const latest = await listHazards(session.accessToken, { source: "PVMBG", raw: true, pageSize: 50 });
        const raws = (latest.data?.hazards ?? []) as RawHazard[];
        if (raws.some(hasConfidenceLevel)) return latest;
        return null;
      });
      if (!evolved?.data) {
        note(record, setLog, "P1", "3. confidence_level appears without redeploy", evolved ?? missing("GET", "/api/hazards?source=PVMBG&raw=true"), { ok: false, expected: "attributes.confidence_level present", actual: "not observed within 20s" });
        return;
      }
      const split = splitByConfidence(evolved.data.hazards as RawHazard[]);
      setAfter(evolved.data.hazards as RawHazard[]);
      note(record, setLog, "P1", "3. unknown field forwarded to attributes", evolved, {
        ok: split.withField.length > 0,
        expected: "new PVMBG records include attributes.confidence_level",
        actual: `${split.withField.length} with field, ${split.without.length} without`,
      });
      note(record, setLog, "P1", "4/P4.2 old and new records readable side by side", evolved, {
        ok: split.withField.length > 0 && (split.without.length > 0 || before.some((item) => !hasConfidenceLevel(item))),
        expected: "pre-change and post-change records coexist",
        actual: `before=${before.length} without=${split.without.length} with=${split.withField.length}`,
      });
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="panel">
      <h2>P1 Schema evolution</h2>
      <ol className="steps">
        <li>Ambil baseline PVMBG raw dan pastikan HazardEvent kanonik valid.</li>
        <li>Picu POST /admin/schema-version lewat demo-api tanpa restart service.</li>
        <li>Tunggu poll berikutnya sampai attributes.confidence_level muncul.</li>
        <li>Tampilkan record lama tanpa field dan record baru dengan field berdampingan.</li>
      </ol>
      <button className="primary" disabled={busy} type="button" onClick={() => void run()}>{busy ? "Running..." : "Run P1"}</button>
      <LogLines lines={log} />
      <div className="compare">
        <div>
          <h3>Before / without confidence_level</h3>
          <pre>{JSON.stringify(before.filter((item) => !hasConfidenceLevel(item)).slice(0, 2), null, 2)}</pre>
        </div>
        <div>
          <h3>After / with <span className="highlight">confidence_level</span></h3>
          <pre>{JSON.stringify(after.filter(hasConfidenceLevel).slice(0, 2), null, 2)}</pre>
        </div>
      </div>
    </section>
  );
}

function Problem2() {
  const { session } = useSessions();
  const { record } = useEvidence();
  const [log, setLog] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);

  const run = async () => {
    if (!requireRawSession(session, setLog)) return;
    setBusy(true);
    try {
      const outageOn = await toggleOutage(true);
      note(record, setLog, "P2", "enable PVMBG outage", outageOn, httpExpected(outageOn.status, [200]));
      const stale = await waitFor(15_000, async () => {
        const status = await sourceStatus(session.accessToken, "PVMBG");
        if (status.ok && status.data && status.data.status === "stale") return status;
        return null;
      });
      note(record, setLog, "P2", "PVMBG marked stale with stale_since", stale ?? missing("GET", "/api/sources/status?source=PVMBG"), statusIsStale(stale?.data?.status ?? "", stale?.data?.stale_since));
      const started = performance.now();
      const [bmkg, volcanic] = await Promise.all([
        listHazards(session.accessToken, { source: "BMKG", pageSize: 20 }),
        listHazards(session.accessToken, { source: "PVMBG", pageSize: 20 }),
      ]);
      const elapsed = Math.round(performance.now() - started);
      note(record, setLog, "P2", "BMKG still served during PVMBG outage", bmkg, httpExpected(bmkg.status, [200]));
      note(record, setLog, "P2", "volcanic reads last store data, not 500/hang", volcanic, {
        ok: volcanic.status === 200 && elapsed < 8000,
        expected: "HTTP 200 from canonical store, no client hang",
        actual: `HTTP ${volcanic.status} parallel ${elapsed}ms`,
      });
      const outageOff = await toggleOutage(false);
      note(record, setLog, "P2", "disable outage without restart", outageOff, httpExpected(outageOff.status, [200]));
      const healthy = await waitFor(15_000, async () => {
        const status = await sourceStatus(session.accessToken, "PVMBG");
        if (status.ok && status.data?.status === "healthy") return status;
        return null;
      });
      note(record, setLog, "P2", "PVMBG returns healthy", healthy ?? missing("GET", "/api/sources/status?source=PVMBG"), {
        ok: healthy?.data?.status === "healthy",
        expected: "status=healthy",
        actual: healthy?.data?.status ?? "timeout",
      });
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="panel">
      <h2>P2 Outage and graceful degradation</h2>
      <ol className="steps">
        <li>Nyalakan outage PVMBG.</li>
        <li>Tunggu /sources/status stale + stale_since.</li>
        <li>Query BMKG dan PVMBG secara paralel; keduanya tidak 500 dan tidak menggantung.</li>
        <li>Matikan outage; status kembali healthy tanpa restart.</li>
      </ol>
      <p className="kicker">p95 &lt; 300ms dan load 50 VU/60s tidak diukur di browser. Buka Test Evidence setelah `make test-load` / `make test-load-isolation`.</p>
      <button className="primary" disabled={busy} type="button" onClick={() => void run()}>{busy ? "Running..." : "Run P2"}</button>
      <LogLines lines={log} />
    </section>
  );
}

function Problem3() {
  const { sessions, refresh } = useSessions();
  const { record } = useEvidence();
  const [log, setLog] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);

  const run = async () => {
    const media = sessions["media-client"];
    const field = sessions["field-team-client"];
    if (!media || !field) {
      setLog(["Login Media dan Tim Lapangan sebelum menjalankan P3."]);
      return;
    }
    setBusy(true);
    try {
      const matrix = await runCrossCredentials();
      note(record, setLog, "P3", "cross-domain credentials rejected 401/403", matrix, {
        ok: Boolean(matrix.data?.passed),
        expected: "BMKG key ≠ PVMBG token",
        actual: matrix.data?.cases.map((item) => `${item.name}:${item.status_code}`).join("; ") ?? matrix.error,
      });
      const mediaRaw = await listHazards(media.accessToken, { raw: true });
      note(record, setLog, "P3", "Media raw request rejected by server", mediaRaw, httpExpected(mediaRaw.status, [403]));
      const mediaSummary = await listHazards(media.accessToken, { raw: false, pageSize: 10 });
      const leak = (mediaSummary.data?.hazards ?? []).find((hazard) => !isSummaryOnly(hazard as Record<string, unknown>).ok);
      note(record, setLog, "P3", "Media summary omits raw fields", mediaSummary, {
        ok: mediaSummary.ok && !leak,
        expected: "summary-only payload",
        actual: leak ? `leaked ${leak.hazard_id}` : "no raw fields",
      });
      setLog((lines) => [...lines, `Menunggu access token Tim Lapangan kedaluwarsa (${field.expiresAt})...`]);
      await waitUntilExpiry(field.expiresAt);
      const expired = await listHazards(field.accessToken, { pageSize: 1 });
      note(record, setLog, "P3", "expired access token rejected", expired, httpExpected(expired.status, [401]));
      const rotated = await refresh("field-team-client");
      note(record, setLog, "P3", "refresh without re-login; old refresh rejected", missing("POST", "/api/auth/refresh"), {
        ok: rotated.ok && rotated.previousRefreshRejected === true,
        expected: "new token pair, previous refresh 401",
        actual: rotated.message + (rotated.previousRefreshRejected ? " old refresh 401" : " old refresh still valid"),
      });
      if (rotated.accessToken) {
        const usable = await listHazards(rotated.accessToken, { pageSize: 1 });
        note(record, setLog, "P3", "new access token usable", usable, httpExpected(usable.status, [200]));
      }
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="panel">
      <h2>P3 Trust domains</h2>
      <ol className="steps">
        <li>Matriks kredensial BMKG↔PVMBG melalui demo-api (secret tidak tampil).</li>
        <li>Media meminta raw=true → 403 dari server.</li>
        <li>Media summary tidak mengandung field mentah.</li>
        <li>TTL alami → 401 → refresh tanpa login ulang → refresh lama ditolak.</li>
      </ol>
      <p className="kicker">Identitas {Object.keys(sessions).length}/3 aktif. Secret login tidak persisten.</p>
      <button className="primary" disabled={busy} type="button" onClick={() => void run()}>{busy ? "Running..." : "Run P3"}</button>
      <LogLines lines={log} />
    </section>
  );
}

function Problem4() {
  const { record } = useEvidence();
  const [log, setLog] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);

  const run = async () => {
    setBusy(true);
    try {
      const health = await fetchServiceHealth();
      note(record, setLog, "P4", "independent service health probes", health, {
        ok: (health.data?.services ?? []).some((item) => item.name === "client-api" && item.status === "healthy"),
        expected: "each component probeable separately",
        actual: (health.data?.services ?? []).map((item) => `${item.name}:${item.status}`).join(", "),
      });
      const ownership = await fetchOwnership();
      note(record, setLog, "P4", "canonical store owned by aggregator only", ownership, {
        ok: ownership.data?.canonical_store_owner === "aggregator" && ownership.data?.demo_api_database_access === false,
        expected: "demo-api has no DB URL / Docker socket",
        actual: JSON.stringify({
          owner: ownership.data?.canonical_store_owner,
          db: ownership.data?.demo_api_database_access,
          docker: ownership.data?.demo_api_docker_access,
        }),
      });
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="panel">
      <h2>P4 Microservice boundaries</h2>
      <ol className="steps">
        <li>Probe health per container dari Overview (live).</li>
        <li>Dari terminal: <code>docker compose up -d --no-deps --build pvmbg</code> lalu pastikan aggregator/client-api tetap hidup dan query BMKG berhasil.</li>
        <li>Coexistence JSONB dibuktikan di P1 before/after dan artifact SQL E2E.</li>
      </ol>
      <button className="primary" disabled={busy} type="button" onClick={() => void run()}>{busy ? "Running..." : "Capture P4 probes"}</button>
      <LogLines lines={log} />
    </section>
  );
}

function Problem5() {
  const { record } = useEvidence();
  const [log, setLog] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);

  const snapshot = async (label: string) => {
    const queues = await fetchQueues();
    note(record, setLog, "P5", label, queues, {
      ok: (queues.data?.queues ?? []).length >= 2,
      expected: "dashboard + notifier queues visible without producer knowing consumers",
      actual: (queues.data?.queues ?? []).map((item) => `${item.name}:msg=${item.messages},c=${item.consumers}`).join("; "),
    });
  };

  const run = async () => {
    setBusy(true);
    try {
      await snapshot("queue snapshot before operator action");
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="panel">
      <h2>P5 Fan-out</h2>
      <ol className="steps">
        <li>Snapshot queue dashboard/notifier/portal (broker, bukan panggilan langsung consumer).</li>
        <li>Terminal: <code>docker compose stop notifier-consumer</code> — dashboard tetap healthy, queue notifier bertambah.</li>
        <li>Terminal: <code>docker compose start notifier-consumer</code> — queue terkuras (at-least-once, consumer idempotent).</li>
        <li>Terminal: <code>docker compose --profile portal up -d portal-consumer</code> — consumer baru tanpa ubah producer.</li>
        <li>Outbox saat broker mati hanya dari artifact E2E, bukan query Canonical Store.</li>
      </ol>
      <button className="primary" disabled={busy} type="button" onClick={() => void run()}>{busy ? "Running..." : "Capture queue snapshot"}</button>
      <button type="button" onClick={() => void snapshot("queue snapshot after operator action")}>Refresh queues</button>
      <LogLines lines={log} />
    </section>
  );
}

function requireRawSession(session: Session | undefined, setLog: (lines: string[]) => void): session is Session {
  if (!session?.accessToken) {
    setLog(["Login Tim Lapangan atau BNPB Ops (raw scope) terlebih dahulu."]);
    return false;
  }
  return true;
}

function note(
  record: ReturnType<typeof useEvidence>["record"],
  setLog: (updater: string[] | ((lines: string[]) => string[])) => void,
  problem: ProblemId,
  criterion: string,
  response: RecordedResponse<unknown>,
  assertion: { ok: boolean; expected: string; actual: string },
) {
  record({
    problem,
    criterion,
    method: response.method,
    url: response.url,
    correlationId: response.correlationId,
    durationMs: response.durationMs,
    expected: assertion.expected,
    actual: assertion.actual,
    status: statusFromOk(assertion.ok),
  });
  setLog((lines) => [...lines, `${assertion.ok ? "PASS" : "FAIL"} · ${criterion} · ${assertion.actual} · cid=${response.correlationId}`]);
}

function LogLines({ lines }: { lines: string[] }) {
  if (lines.length === 0) return null;
  return <pre>{lines.join("\n")}</pre>;
}

function emptyHazard(): RawHazard {
  return { hazard_id: "", source: "", hazard_type: "", severity: "", area_name: "", occurred_at: "", ingested_at: "" };
}

function missing(method: string, url: string): RecordedResponse<never> {
  return { ok: false, status: 0, data: null, error: "timeout", correlationId: "-", durationMs: 0, url, method };
}

async function waitFor<T>(timeoutMs: number, fn: () => Promise<T | null>): Promise<T | null> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn();
    if (value) return value;
    await delay(400);
  }
  return null;
}

async function waitUntilExpiry(expiresAt: string) {
  while (!isExpired(expiresAt)) {
    await delay(500);
  }
  await delay(300);
}

function delay(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}
