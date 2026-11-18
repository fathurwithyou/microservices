import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const controlledRejections = new Counter('controlled_rejections');
const unexpectedErrors = new Rate('unexpected_errors');
const latency = new Trend('hazard_latency', true);
const served = new Counter('served_ok');

// k6 metrics must exist in the init context; anything else counts as other.
const statusCounters = Object.fromEntries([0, 200, 400, 401, 403, 429, 500, 502, 503, 504].map((code) => [code, new Counter(`status_${code}`)]));
const otherStatus = new Counter('status_other');
function statusCounter(code) {
  return statusCounters[code] || otherStatus;
}

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(95)', 'p(99)'],
  scenarios: {
    // Ramp to the target first: 10k simultaneous TCP connects from one load
    // host time out in the host-to-VM network before reaching the cluster.
    // The hold stage is the measured sustained load (>= 120 s at 10k VUs).
    sustained: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: __ENV.RAMP || '30s', target: Number(__ENV.VUS || 10000) },
        { duration: __ENV.DURATION || '120s', target: Number(__ENV.VUS || 10000) },
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    unexpected_errors: ['rate<0.01'],
    hazard_latency: ['p(95)<300'],
  },
};

export function setup() {
  const base = __ENV.BASE_URL || 'http://localhost:8080';
  const secret = __ENV.FIELD_TEAM_CLIENT_SECRET;
  if (!secret) throw new Error('FIELD_TEAM_CLIENT_SECRET is required');
  let response;
  for (let attempt = 0; attempt < 5; attempt++) {
    response = http.post(`${base}/auth/token`, JSON.stringify({ client_id: 'field-team-client', client_secret: secret }), { headers: { 'Content-Type': 'application/json' } });
    if (response.status === 200) break;
    sleep(1);
  }
  check(response, { 'field token issued': (r) => r.status === 200 });
  return { base, token: response.json('access_token') };
}

// BASE_URLS (comma separated) spreads VUs over several entry points, e.g.
// the NodePort on every node, so one node's network path is not the limit.
const entries = (__ENV.BASE_URLS || '').split(',').map((url) => url.trim()).filter(Boolean);

export default function (session) {
  const base = entries.length > 0 ? entries[__VU % entries.length] : session.base;
  const response = http.get(`${base}/hazards?source=BMKG`, { headers: { Authorization: `Bearer ${session.token}` } });
  latency.add(response.timings.duration);
  if (response.status === 200) served.add(1);
  if (response.status === 429) {
    controlledRejections.add(1);
    unexpectedErrors.add(false);
  } else {
    unexpectedErrors.add(response.status !== 200);
  }
  statusCounter(response.status).add(1);
  check(response, { 'served or controlled': (r) => r.status === 200 || r.status === 429 });
  // THINK=0 turns the VUs into a closed loop with no think time, which is
  // what the throughput comparison needs: served requests per second are then
  // limited by the system, not by the one-second pause.
  const think = Number(__ENV.THINK ?? 1);
  if (think > 0) sleep(think);
}

export function handleSummary(data) {
  const durationMs = data.state.testRunDurationMs || 1;
  const requests = data.metrics.http_reqs?.values?.count || 0;
  const values = data.metrics.hazard_latency?.values || {};
  const payload = {
    problem: 'P2',
    label: `M2 sustained ${__ENV.VUS || 10000} VU / ${__ENV.DURATION || '120s'}`,
    duration_s: durationMs / 1000,
    http_reqs: requests,
    throughput_rps: requests / (durationMs / 1000),
    p50_ms: values['p(50)'] ?? null,
    p95_ms: values['p(95)'] ?? null,
    p99_ms: values['p(99)'] ?? null,
    error_rate: data.metrics.unexpected_errors?.values?.rate ?? null,
    controlled_rejections: data.metrics.controlled_rejections?.values?.count ?? 0,
    served_ok: data.metrics.served_ok?.values?.count ?? 0,
    served_ok_rps: (data.metrics.served_ok?.values?.count ?? 0) / (durationMs / 1000),
    vus: Number(__ENV.VUS || 10000),
    label_extra: __ENV.LABEL || null,
    status_counts: Object.fromEntries(Object.entries(data.metrics).filter(([k]) => k.startsWith('status_')).map(([k, v]) => [k, v.values.count])),
  };
  const output = { stdout: JSON.stringify(payload, null, 2) + '\n' };
  if (__ENV.K6_SUMMARY_PATH) output[__ENV.K6_SUMMARY_PATH] = JSON.stringify(payload, null, 2);
  return output;
}
