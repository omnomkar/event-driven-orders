# Load test results

Two runs of [`loadtest/order-flow.js`](../order-flow.js) through `scripts/loadtest.sh`, one with the inventory catalog cache on and one with it off. Every number below is copied from the raw files in this directory; nothing is estimated.

| Run | k6 summary (JSON) | k6 console output | Server-side snapshot (Prometheus and Postgres) |
| --- | --- | --- | --- |
| Cache on | [`20261017T003931Z-cache-on.json`](20261017T003931Z-cache-on.json) | [`.txt`](20261017T003931Z-cache-on.txt) | [`-server.json`](20261017T003931Z-cache-on-server.json) |
| Cache off | [`20261017T004503Z-cache-off.json`](20261017T004503Z-cache-off.json) | [`.txt`](20261017T004503Z-cache-off.txt) | [`-server.json`](20261017T004503Z-cache-off-server.json) |

## Workload

- Ramping VUs: 0 to 50 over 1 minute, 50 for 2 minutes, 50 to 0 over 30 seconds.
- Each VU sends one request, then pauses 0.1 s. About 80% of requests are reads (half `GET /products`, half `GET /products/{id}`), and about 20% are `POST /orders` for one unit of a deep-stock product with the `4242 4242 4242 4242` test card and a unique `Idempotency-Key`.
- Before each run the script resets the five deep-stock products to 10,000,000 units and empties Redis, so neither run sells out and no cache entry predates the run.
- Thresholds were set before the first run and not changed: error rate below 1%, checks above 99%, read p95 below 200 ms, order p95 below 500 ms. Both runs passed all of them.

## Results

Latencies are in milliseconds. Rounding never favours the result: latencies are rounded up, request rates and hit ratios down.

| Metric | Cache on | Cache off |
| --- | --- | --- |
| Read latency p50 / p95 / p99 (ms) | 1.02 / 2.03 / 3.71 | 0.96 / 1.79 / 3.09 |
| Order creation latency p50 / p95 / p99 (ms) | 5.83 / 13.72 / 39.16 | 5.75 / 12.83 / 28.28 |
| Requests per second (all / reads / orders) | 380.40 / 303.86 / 76.54 | 380.95 / 305.01 / 75.93 |
| Requests (all / reads / orders) | 79,898 / 63,822 / 16,076 | 80,021 / 64,070 / 15,951 |
| Error rate (`http_req_failed`) | 0% (0 of 79,898) | 0% (0 of 80,021) |
| Saga duration p95 / p99, exact from Postgres (ms) | 853 / 914 | 855 / 915 |
| Saga duration p95 / p99, Prometheus histogram estimate (ms) | 967 / 994 | 971 / 995 |
| Max consumer lag, summed over partitions (order-saga / inventory / payment) | 39 / 22 / 24 | 62 / 21 / 23 |
| Cache hit ratio (product / list / overall) | 75.3% / 76.9% / 76.1% | not applicable |
| Orders confirmed, payments recorded, dead-lettered | 16,076, 16,076, 0 | 15,951, 15,951, 0 |

Definitions:

- **Read and order latency** are k6's `http_req_duration` sub-metrics tagged `kind:read` and `kind:order`, measured at the client, for the whole run including ramps. Order creation is the synchronous part only: tokenize the card, insert the PENDING order and its outbox rows, answer 202.
- **Saga duration** is the time from order creation to CONFIRMED, measured after the run once every order was final. The exact value is `updated_at - created_at` over the run's orders. The Prometheus value is `histogram_quantile` over the run's `saga_duration_seconds` buckets. The exact p50, p95, and p99 all fall in the 0.5 s to 1 s bucket, and Prometheus interpolates linearly inside a bucket, so its estimate is higher than the exact value. It is shown because it is what a dashboard would display.
- **Max consumer lag** is the highest value of `kafka_consumer_lag` summed over a group's three partitions at any 5 s step of the run. The gauge itself is refreshed every 10 s, so a brief peak between refreshes is not captured.
- **Cache hit ratio** is hits / (hits + misses) from the increase in `inventory_cache_{hits,misses}_total` over the run, by key kind. With the cache on, hits plus misses (48,604 + 15,218 = 63,822) equal k6's read count exactly.

## What the comparison shows

**The cache did not make reads faster here.** Read p95 was 2.03 ms with the cache and 1.79 ms without it, and p99 was 3.71 ms against 3.09 ms. The catalog is seven rows, which Postgres serves from memory by primary key, so a database read costs about the same as a Redis round trip on the same host. The cache then adds JSON decoding on every hit, and on every miss a Redis `SET` on top of the database read. Between the runs, order p50 and p95 differed by less than 7%, saga p95 and p99 by less than 1%, and throughput by less than 1%; order p99 differed more (39.16 against 28.28 ms), and saga p50 by 8% (640 against 692 ms). Each mode was run once, so these numbers carry no variance estimate, and differences this small should not be read as a ranking.

**The list key churns, as expected.** Every committed reservation deletes `products:all`, and the run committed about 76 reservations per second against about 152 list reads per second. The list hit ratio was 76.9%. Single products churn too: each order invalidates its product's key, and with only five products each key is deleted about 15 times per second, which is why the product hit ratio (75.3%) is no better than the list's. At this write rate a key is deleted long before its 30 s TTL, so invalidation, not expiry, decides the hit ratio.

**Where a cache would pay off** is a catalog read path that is expensive (many rows, joins, or a remote database), a database under read pressure, or a read-heavy mix with few stock changes per key. None of these apply to this test, and the cache stays useful mainly as a design exercise in keeping stale data from mattering: reservations never read it.

**Saga time is dominated by polling.** A saga passes through four relayed messages (ReserveStock, StockReserved, ChargePayment, PaymentSucceeded), and each outbox relay polls every 200 ms. The exact saga p50 was 640 ms with the cache on and 692 ms with it off, which is consistent with waiting about half a poll interval on average at each of the four hops, plus the work itself. Lowering `OUTBOX_POLL_INTERVAL` or notifying the relay on commit would cut it; Kafka and the consumers are not the bottleneck (max lag of a few dozen records).

## Test conditions

Both runs were on one laptop (Intel Core Ultra 7 155H, 22 logical CPUs visible to WSL2, 15 GB of memory assigned to WSL2) in WSL2 with Docker Engine 28.2.2, and every container ran on that one host: the k6 load generator, the four services, Kafka, Postgres, Redis, and Prometheus. The load generator competes with the system under test for CPU, and all traffic stays on a local Docker bridge with no real network latency, so the latencies are lower than they would be between machines and the throughput says nothing about a multi-host deployment. Each VU waits 0.1 s between requests and k6 iterations averaged about 103 ms, so the stack was not saturated: about 380 requests per second is the load 50 VUs generate with that pause, not the system's capacity. There is one instance of every service, one Kafka broker, and one Postgres. The cache-on run went first, so the cache-off run started with 16,076 more orders in the database. Each mode was run once.

## Reproducing

```bash
make loadtest CACHE=on
make loadtest CACHE=off
```

Settings such as `LOADTEST_VUS`, `LOADTEST_HOLD`, and `LOADTEST_THINK_SECONDS` are listed at the top of `scripts/loadtest.sh`. Each run writes a new set of timestamped files here.
