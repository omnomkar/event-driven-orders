// Load test for the public API: about 80% catalog reads and 20% order
// creation, with a ramping number of virtual users (VUs).
//
// Each VU loops: pick a request by READ_RATIO, send it, check the status,
// then wait THINK_SECONDS. Reads are split evenly between GET /products and
// GET /products/{id}. Orders use the good test card and one unit of a
// product with deep stock, so they are expected to be accepted (202) and
// confirmed. The script does not wait for confirmation; the saga's duration
// and outcomes are measured on the server side (see scripts/loadtest.sh).
//
// Run it through scripts/loadtest.sh, which starts the stack, resets stock,
// and saves the results. Settings (all optional, passed with -e):
//   BASE_URL       gateway URL (default http://gateway:8080, the Compose name)
//   TARGET_VUS     VUs held during the steady phase (default 50)
//   RAMP_UP        time to ramp from 0 to TARGET_VUS (default 1m)
//   HOLD           time held at TARGET_VUS (default 2m)
//   RAMP_DOWN      time to ramp back to 0 (default 30s)
//   READ_RATIO     share of iterations that are reads (default 0.8)
//   THINK_SECONDS  pause after each request (default 0.1)
//   RUN_ID         customer_id for every order, so the run's orders can be
//                  found in the database (default k6-<timestamp>)
import http from 'k6/http';
import { check, sleep } from 'k6';

const baseURL = __ENV.BASE_URL || 'http://gateway:8080';
const readRatio = Number(__ENV.READ_RATIO || 0.8);
const thinkSeconds = Number(__ENV.THINK_SECONDS || 0.1);
const runID = __ENV.RUN_ID || `k6-${Date.now()}`;

// Products with deep stock (the load test resets them before each run).
const products = ['sku-keyboard', 'sku-mouse', 'sku-monitor', 'sku-headset', 'sku-cable'];

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: __ENV.RAMP_UP || '1m', target: Number(__ENV.TARGET_VUS || 50) },
        { duration: __ENV.HOLD || '2m', target: Number(__ENV.TARGET_VUS || 50) },
        { duration: __ENV.RAMP_DOWN || '30s', target: 0 },
      ],
      gracefulRampDown: '10s',
    },
  },
  // Default pass/fail bars, chosen before the first run and not tuned to
  // the results. The kind:read and kind:order thresholds also make k6
  // report those sub-metrics separately in the summary.
  thresholds: {
    http_req_failed: ['rate<0.01'],
    checks: ['rate>0.99'],
    'http_req_duration{kind:read}': ['p(95)<200'],
    'http_req_duration{kind:order}': ['p(95)<500'],
    'http_reqs{kind:read}': ['count>0'],
    'http_reqs{kind:order}': ['count>0'],
  },
};

function pick(list) {
  return list[Math.floor(Math.random() * list.length)];
}

function readList() {
  const res = http.get(`${baseURL}/products`, { tags: { kind: 'read', name: 'GET /products' } });
  check(res, { 'list products is 200': (r) => r.status === 200 });
}

function readOne() {
  const res = http.get(`${baseURL}/products/${pick(products)}`, {
    tags: { kind: 'read', name: 'GET /products/{id}' },
  });
  check(res, { 'get product is 200': (r) => r.status === 200 });
}

function createOrder() {
  // Standard test card number only.
  const body = JSON.stringify({
    customer_id: runID,
    items: [{ product_id: pick(products), quantity: 1 }],
    card: { number: '4242 4242 4242 4242', exp_month: 12, exp_year: 2030 },
  });
  const res = http.post(`${baseURL}/orders`, body, {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': `${runID}-${__VU}-${__ITER}`,
    },
    tags: { kind: 'order', name: 'POST /orders' },
  });
  check(res, { 'create order is 202': (r) => r.status === 202 });
}

export default function () {
  if (Math.random() < readRatio) {
    if (Math.random() < 0.5) {
      readList();
    } else {
      readOne();
    }
  } else {
    createOrder();
  }
  sleep(thinkSeconds);
}
