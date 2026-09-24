import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const controlledRejections = new Counter('controlled_rejections');
const unexpectedErrors = new Rate('unexpected_errors');
const bmkgLatency = new Trend('bmkg_only_latency', true);
const pvmbgLatency = new Trend('pvmbg_latency', true);

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(95)', 'p(99)'],
  scenarios: {
    bmkg_only: { executor: 'constant-vus', vus: 25, duration: '30s', exec: 'bmkgOnly' },
    pvmbg: { executor: 'constant-vus', vus: 25, duration: '30s', exec: 'pvmbgTraffic' },
  },
  thresholds: {
    unexpected_errors: ['rate<0.01'],
    bmkg_only_latency: ['p(95)<300'],
  },
};

export function setup() {
  const base = __ENV.BASE_URL || 'http://localhost:8080';
  const secret = __ENV.FIELD_TEAM_CLIENT_SECRET;
  if (!secret) {
    throw new Error('FIELD_TEAM_CLIENT_SECRET is required');
  }
  const response = http.post(`${base}/auth/token`, JSON.stringify({ client_id: 'field-team-client', client_secret: secret }), { headers: { 'Content-Type': 'application/json' } });
  check(response, { 'field token issued': (r) => r.status === 200 });
  return { base, token: response.json('access_token') };
}

export function bmkgOnly(session) {
  const response = http.get(`${session.base}/hazards?source=BMKG`, { headers: { Authorization: `Bearer ${session.token}` } });
  bmkgLatency.add(response.timings.duration);
  recordStatus(response);
  check(response, { 'BMKG-only served or controlled': (r) => r.status === 200 || r.status === 429 });
  sleep(0.05);
}

export function pvmbgTraffic(session) {
  const response = http.get(`${session.base}/hazards?source=PVMBG`, { headers: { Authorization: `Bearer ${session.token}` } });
  pvmbgLatency.add(response.timings.duration);
  recordStatus(response);
  check(response, { 'PVMBG served, stale, or controlled': (r) => r.status === 200 || r.status === 429 });
  sleep(0.05);
}

function recordStatus(response) {
  if (response.status === 429) {
    controlledRejections.add(1);
    unexpectedErrors.add(false);
    return;
  }
  unexpectedErrors.add(response.status !== 200);
}

export function handleSummary(data) {
  const durationMs = data.state.testRunDurationMs || 1;
  const requests = (data.metrics.http_reqs && data.metrics.http_reqs.values && data.metrics.http_reqs.values.count) || 0;
  const bmkg = data.metrics.bmkg_only_latency && data.metrics.bmkg_only_latency.values;
  const pvmbg = data.metrics.pvmbg_latency && data.metrics.pvmbg_latency.values;
  const payload = {
    problem: 'P2',
    label: 'P2.1 concurrent BMKG-only vs PVMBG (set PVMBG_MAX_DELAY=3s)',
    duration_s: durationMs / 1000,
    http_reqs: requests,
    throughput_rps: requests / (durationMs / 1000),
    bmkg_p50_ms: bmkg ? bmkg['p(50)'] : null,
    bmkg_p95_ms: bmkg ? bmkg['p(95)'] : null,
    bmkg_p99_ms: bmkg ? bmkg['p(99)'] : null,
    pvmbg_p95_ms: pvmbg ? pvmbg['p(95)'] : null,
    error_rate: data.metrics.unexpected_errors && data.metrics.unexpected_errors.values ? data.metrics.unexpected_errors.values.rate : null,
    controlled_rejections: data.metrics.controlled_rejections && data.metrics.controlled_rejections.values ? data.metrics.controlled_rejections.values.count : 0,
  };
  const output = { stdout: JSON.stringify(payload, null, 2) + '\n' };
  if (__ENV.K6_SUMMARY_PATH) {
    output[__ENV.K6_SUMMARY_PATH] = JSON.stringify(payload, null, 2);
  }
  return output;
}
