import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const controlledRejections = new Counter('controlled_rejections');
const unexpectedErrors = new Rate('unexpected_errors');
const latency = new Trend('hazard_latency', true);

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(95)', 'p(99)'],
  scenarios: {
    sustained: { executor: 'constant-vus', vus: 10000, duration: '120s' },
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
  const response = http.post(`${base}/auth/token`, JSON.stringify({ client_id: 'field-team-client', client_secret: secret }), { headers: { 'Content-Type': 'application/json' } });
  check(response, { 'field token issued': (r) => r.status === 200 });
  return { base, token: response.json('access_token') };
}

export default function (session) {
  const response = http.get(`${session.base}/hazards?source=BMKG`, { headers: { Authorization: `Bearer ${session.token}` } });
  latency.add(response.timings.duration);
  if (response.status === 429) {
    controlledRejections.add(1);
    unexpectedErrors.add(false);
  } else {
    unexpectedErrors.add(response.status !== 200);
  }
  check(response, { 'served or controlled': (r) => r.status === 200 || r.status === 429 });
  sleep(1);
}

export function handleSummary(data) {
  const durationMs = data.state.testRunDurationMs || 1;
  const requests = data.metrics.http_reqs?.values?.count || 0;
  const values = data.metrics.hazard_latency?.values || {};
  const payload = {
    problem: 'P2',
    label: 'M2 sustained 10000 VU / 120s',
    duration_s: durationMs / 1000,
    http_reqs: requests,
    throughput_rps: requests / (durationMs / 1000),
    p50_ms: values['p(50)'] ?? null,
    p95_ms: values['p(95)'] ?? null,
    p99_ms: values['p(99)'] ?? null,
    error_rate: data.metrics.unexpected_errors?.values?.rate ?? null,
    controlled_rejections: data.metrics.controlled_rejections?.values?.count ?? 0,
  };
  const output = { stdout: JSON.stringify(payload, null, 2) + '\n' };
  if (__ENV.K6_SUMMARY_PATH) output[__ENV.K6_SUMMARY_PATH] = JSON.stringify(payload, null, 2);
  return output;
}
