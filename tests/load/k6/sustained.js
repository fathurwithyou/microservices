import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const controlledRejections = new Counter('controlled_rejections');
const unexpectedErrors = new Rate('unexpected_errors');
const bmkgLatency = new Trend('bmkg_only_latency', true);

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(95)', 'p(99)'],
  scenarios: { sustained: { executor: 'constant-vus', vus: 50, duration: '60s' } },
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

export default function (session) {
  const response = http.get(`${session.base}/hazards?source=BMKG`, { headers: { Authorization: `Bearer ${session.token}` } });
  bmkgLatency.add(response.timings.duration);
  if (response.status === 429) {
    controlledRejections.add(1);
    unexpectedErrors.add(false);
  } else {
    unexpectedErrors.add(response.status !== 200);
  }
  check(response, { 'BMKG query is served or controlled': (r) => r.status === 200 || r.status === 429 });
  sleep(0.1);
}

export function handleSummary(data) {
  const payload = summarize(data, 'P2.2 sustained 50 VU / 60s BMKG-only');
  const serialized = JSON.stringify(payload, null, 2);
  const output = { stdout: serialized + '\n' };
  if (__ENV.K6_SUMMARY_PATH) {
    output[__ENV.K6_SUMMARY_PATH] = serialized;
  }
  return output;
}

function summarize(data, label) {
  const durationMs = data.state.testRunDurationMs || 1;
  const requests = (data.metrics.http_reqs && data.metrics.http_reqs.values && data.metrics.http_reqs.values.count) || 0;
  const duration = data.metrics.http_req_duration && data.metrics.http_req_duration.values;
  const bmkg = data.metrics.bmkg_only_latency && data.metrics.bmkg_only_latency.values;
  return {
    problem: 'P2',
    label,
    duration_s: durationMs / 1000,
    http_reqs: requests,
    throughput_rps: requests / (durationMs / 1000),
    p50_ms: (bmkg && bmkg['p(50)']) || (duration && duration['p(50)']) || null,
    p95_ms: (bmkg && bmkg['p(95)']) || (duration && duration['p(95)']) || null,
    p99_ms: (bmkg && bmkg['p(99)']) || (duration && duration['p(99)']) || null,
    error_rate: data.metrics.unexpected_errors && data.metrics.unexpected_errors.values ? data.metrics.unexpected_errors.values.rate : null,
    controlled_rejections: data.metrics.controlled_rejections && data.metrics.controlled_rejections.values ? data.metrics.controlled_rejections.values.count : 0,
  };
}
