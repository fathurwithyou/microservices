// M2 P5.5: under overload, Field Team requests are served while Media
// requests are rejected in a controlled way (429 + Retry-After).
//
// Two scenarios run at once against the same entry points. The whole run is
// kept under the 60 s access-token lifetime so setup tokens stay valid.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const FIELD_VUS = Number(__ENV.FIELD_VUS || __ENV.VUS || 100);
const MEDIA_VUS = Number(__ENV.MEDIA_VUS || __ENV.VUS || 2000);
const HOLD = __ENV.DURATION || '35s';
const entries = (__ENV.BASE_URLS || __ENV.BASE_URL || 'http://localhost:8080').split(',').map((url) => url.trim()).filter(Boolean);

const codes = [0, 200, 401, 429, 500, 502, 503, 504];
const counters = {};
for (const klass of ['field', 'media']) {
  counters[klass] = Object.fromEntries(codes.map((code) => [code, new Counter(`${klass}_status_${code}`)]));
  counters[klass].other = new Counter(`${klass}_status_other`);
}
const fieldLatency = new Trend('field_ok_latency', true);
const mediaLatency = new Trend('media_ok_latency', true);
const retryAfter = new Counter('media_429_with_retry_after');

export const options = {
  summaryTrendStats: ['med', 'p(95)', 'max'],
  scenarios: {
    field: { executor: 'ramping-vus', exec: 'field', startVUs: 0, stages: [{ duration: '10s', target: FIELD_VUS }, { duration: HOLD, target: FIELD_VUS }] },
    media: { executor: 'ramping-vus', exec: 'media', startVUs: 0, stages: [{ duration: '10s', target: MEDIA_VUS }, { duration: HOLD, target: MEDIA_VUS }] },
  },
};

function token(client, secret) {
  for (let attempt = 0; attempt < 5; attempt++) {
    const response = http.post(`${entries[0]}/auth/token`, JSON.stringify({ client_id: client, client_secret: secret }), { headers: { 'Content-Type': 'application/json' } });
    if (response.status === 200) return response.json('access_token');
    sleep(1);
  }
  throw new Error(`token for ${client} failed`);
}

export function setup() {
  return {
    field: token('field-team-client', __ENV.FIELD_TEAM_CLIENT_SECRET),
    media: token('media-client', __ENV.MEDIA_CLIENT_SECRET),
  };
}

function call(klass, bearer, latency) {
  const base = entries[__VU % entries.length];
  const response = http.get(`${base}/hazards?source=BMKG&page_size=20`, { headers: { Authorization: `Bearer ${bearer}` }, timeout: '10s' });
  (counters[klass][response.status] || counters[klass].other).add(1);
  if (response.status === 200) latency.add(response.timings.duration);
  if (klass === 'media' && response.status === 429 && response.headers['Retry-After']) retryAfter.add(1);
  sleep(0.5);
}

export function field(tokens) { call('field', tokens.field, fieldLatency); }
export function media(tokens) { call('media', tokens.media, mediaLatency); }

export function handleSummary(data) {
  const summary = { problem: 'P5.5', field_vus: FIELD_VUS, media_vus: MEDIA_VUS, hold: HOLD, classes: {} };
  for (const klass of ['field', 'media']) {
    const statuses = {};
    let total = 0;
    for (const [name, metric] of Object.entries(data.metrics)) {
      if (name.startsWith(`${klass}_status_`)) {
        statuses[name.slice(klass.length + 8)] = metric.values.count;
        total += metric.values.count;
      }
    }
    const ok = statuses['200'] || 0;
    summary.classes[klass] = {
      requests: total, statuses, served_ratio: total ? ok / total : null,
      ok_latency_ms: data.metrics[`${klass}_ok_latency`]?.values || null,
    };
  }
  summary.classes.media.rejections_with_retry_after = data.metrics.media_429_with_retry_after?.values?.count || 0;
  const out = { stdout: JSON.stringify(summary, null, 2) + '\n' };
  if (__ENV.K6_SUMMARY_PATH) out[__ENV.K6_SUMMARY_PATH] = JSON.stringify(summary, null, 2);
  return out;
}
