import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const controlledRejections = new Counter('controlled_rejections');
const unexpectedErrors = new Rate('unexpected_errors');
const bmkgLatency = new Trend('bmkg_only_latency', true);

export const options = {
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
