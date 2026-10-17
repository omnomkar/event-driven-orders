# Design Decisions

Each entry records a decision, why I made it, and the main alternative I considered.

## 1. Card tokenization happens before the saga starts

The gateway calls the payment service to tokenize the card before it creates the order, so only an opaque payment token flows through the order service, its database, and Kafka.
This keeps raw card numbers inside a single service, which shrinks the part of the system that ever touches cardholder data. CVV is never accepted.
Alternative: send the card number with the order and let payment read it from the charge command. That is simpler, but it would put card data in the order database, the outbox, and Kafka topics.

## 2. Multi-item orders reserve stock all-or-nothing

An order can contain several line items, and the inventory service reserves all of them inside one database transaction or none of them.
A partial reservation would leave the saga needing to compensate for half-reserved orders, and customers rarely want half an order.
Alternative: reserve each item separately and release the successful ones if any item fails. That spreads one logical step over many messages and makes compensation harder to reason about.

## 3. Inventory owns prices

The inventory service stores product prices, and its `StockReserved` event carries the unit prices and the order total. The order service charges exactly that total and never calls inventory directly.
Pricing is read at the same moment stock is reserved, so the charged amount always matches what was reserved, and services stay coupled only through events.
Alternative: have the order service look up prices over gRPC when the order is created. That adds a synchronous dependency and a window where the price can change between lookup and reservation.

## 4. Kafka payloads are Protobuf

Every topic carries one Protobuf wrapper message with a message ID, the order ID, a timestamp, and a `oneof` of the variants for that topic.
The same `.proto` files define gRPC and events, so types are shared and payloads are compact. CI already runs `buf lint` and fails if the committed generated code is out of date. Adding `buf breaking` to CI, so incompatible schema changes fail the build, is a planned next step.
Alternative: JSON events. They are easier to read in a console, but they have no enforced schema, so a renamed field breaks consumers silently.

## 5. The saga is orchestrated by the order service

The order service decides every step of an order: it sends ReserveStock, waits for the reply, sends ChargePayment, and confirms or cancels. Inventory and payment only answer the command they receive and know nothing about the steps around them. The whole flow is a single transition table in the order service, `next(status, event)`, which is a pure function with table-driven tests.
I chose orchestration because the flow has a strict order and a compensation path (a failed payment must release stock). With orchestration that logic lives in one place, the current state of any order is one database row, and adding a step means changing one service.
Alternative: choreography, where each service reacts to the others' events (inventory listens for OrderCreated, payment listens for StockReserved, and so on). It removes the central coordinator and suits flows where independent consumers fan out from one event, such as notifications or analytics. For a sequential flow with compensation, though, the logic ends up spread across services and the end-to-end flow is hard to see or change. `order.events` is still published, so choreographed consumers like a fraud check or email service can be added without touching the saga.

## 6. Every state change publishes through a transactional outbox

A service never writes to its database and then publishes to Kafka as two separate steps. It writes its state change and the message it wants to send into an `outbox` table in the same Postgres transaction. A relay in the same service polls for unsent rows, publishes them with a synchronous produce, and marks them sent. The order, inventory, and payment services each have their own outbox in their own database.
Without the outbox, a crash right after the database commit would leave, for example, a PENDING order whose ReserveStock command was never sent, so the order would never move. Publishing first and committing second has the opposite failure: a command for an order that was rolled back. The outbox makes the message as durable as the state change.
The relay selects a batch with `FOR UPDATE SKIP LOCKED`, so several relay instances can run at once without sending the same row at the same time. An integration test runs two relays over 200 rows and checks that Kafka receives exactly 200 records.
Alternative: Kafka transactions, or change data capture (for example Debezium reading the Postgres write-ahead log). Kafka transactions cannot include the Postgres write, so they don't solve the dual write. CDC removes the polling delay, but it adds a connector cluster to run and monitor, which is more infrastructure than this system needs.

## 7. Delivery is at-least-once, so consumers must be idempotent

Messages can be delivered more than once, and the design says so explicitly instead of claiming exactly-once.
- **Producing:** if the relay crashes after Kafka acknowledges a record but before the `sent_at` update commits, the row is still unsent and is published again.
- **Consuming:** auto-commit is off. Each record is handled inside one database transaction, and its offset is committed only after that transaction commits. A crash between the two redelivers the record. Committing the offset first would be at-most-once instead: a crash would lose the record and could leave an order stuck.

Messages are never lost, but duplicates are possible, so every handler must make a duplicate harmless. Every message carries a unique `message_id` as the key for consumer deduplication. Database constraints act as a backstop underneath it: one reservation per order, one payment per order, and saga transitions that reject an event that is not valid for the order's current state.
A record that fails permanently, for example one that cannot be decoded or arrives in the wrong state, goes through a single failure handler shared by every consumer, which sends it to that topic's dead-letter topic (entry 11).
Alternative: Kafka's exactly-once semantics. They cover read-process-write loops that stay inside Kafka, but our side effects are Postgres writes, so idempotent consumers are needed either way.

## 8. Business failures are events, not errors

Out of stock and a declined card are normal answers, so they travel as reply events: inventory commits `StockRejected` and payment commits `PaymentFailed`, each in the same transaction as its outbox row. An unknown payment token or an unusable amount also produces `PaymentFailed`, so the saga releases the stock instead of waiting forever. Errors are kept for two cases only: infrastructure trouble, which is retried, and messages that can never succeed (undecodable, or invalid for the order's state), which go to the dead-letter topic.
A failed payment moves the order to `CANCELLING` and sends `ReleaseStock`. The order becomes `CANCELLED` only when inventory replies `StockReleased`, so `GET /orders/{id}` never reports an order as cancelled while its stock is still held. Release is idempotent: releasing an already released (or never made) reservation changes nothing and still replies, so the saga can always finish.
Alternative: return an error for out of stock and let the order time out. That leaves the order stuck in `PENDING` and needs a separate timeout process to clean up.

## 9. Idempotency-Key is scoped per customer and checked against the request

`POST /orders` accepts an `Idempotency-Key` header. The order service stores the key with the order under a unique `(customer_id, idempotency_key)` constraint, plus a SHA-256 hash of the order's items (sorted, so listing them in another order is the same request).
Every create tries the insert. If the unique constraint rejects it, the existing order is read: the same hash returns that order with 200, and a different hash returns 409 Conflict. Concurrent duplicates take the same path: Postgres makes the second insert wait for the first to commit, then fail, so exactly one order exists and every caller gets it.
The hash leaves the card out on purpose. The gateway tokenizes the card on every request, so the token is different each time, and storing a hash of the card number would put a brute-forceable derivative of it in the order database. A retry with the same key and items but a different card therefore returns the original order.
Alternative: deduplicate in the gateway with a cache. That fails when the cache restarts or when two gateway instances receive the duplicates, and it cannot be atomic with the order insert.

## 10. Consumer deduplication gives effectively-once effects

Delivery stays at-least-once (entry 7), so each consumer makes duplicates harmless. Every message carries a `message_id` that is assigned when the outbox row is written, so a relay resend carries the same ID. Each service has a `processed_messages` table, and the first statement in a handler's transaction inserts the ID with `ON CONFLICT DO NOTHING`. If nothing was inserted, the message was already handled: the handler skips the work and the offset is committed.
Because the ID and the effect commit in one transaction, the two are always recorded together. A crash before the commit leaves neither, so the redelivered message does the work. A crash after the commit but before the offset commit leaves both, so the redelivered message is skipped. The result is exactly one effect per message, even though the message may arrive more than once.
Database constraints remain as a second guard: one reservation per order and one payment per order. A second `ChargePayment` for an order under a different `message_id` hits the unique `payments.order_id` and goes to the dead-letter topic for review instead of charging again.
The chaos test (`make chaos`) kills the payment container with SIGKILL while it is charging, restarts it, and checks that every order is final, that no order has more than one payment, and that successful payments equal confirmed orders. In a 200-order run the restarted service skipped 25 redelivered charge commands by `message_id`, and every order ended with exactly one payment.
Alternative: make each operation naturally idempotent, for example "set stock to X" instead of "subtract N". That works for some updates, but a charge or a reservation is inherently an increment, so an explicit record of handled messages is simpler to reason about.

## 11. Retries happen in place; failures go to a dead-letter topic

A handler error that might be temporary, such as a database timeout, is retried in place with exponential backoff and full jitter: retry n waits a random time between 0 and min(5s, 100ms x 2^(n-1)), for up to 5 attempts (all configurable). Full jitter keeps many consumers that failed together from retrying together.
After the last attempt, or immediately for a permanent error such as an undecodable payload, the record is published to `<topic>.dlq` with its original key and bytes, plus headers for the error, the attempt count, and the source topic, partition, and offset. Its offset is committed only after the dead-letter publish is acknowledged, so a record is never dropped.
Retrying in place has a cost: while one record waits for its next attempt, every record behind it on the same partition waits too. That keeps each order's messages in order, which the saga relies on, but one slow record delays unrelated orders on its partition. At higher scale the next step would be retry topics (for example `<topic>.retry.1s`, `<topic>.retry.30s`), which free the partition immediately at the cost of giving up strict per-order ordering during retries. Five attempts at these settings also cover only a few seconds of outage, so a longer database outage sends records to the dead-letter topic, and they are replayed afterwards.
A dead-letter record is handled by a person: `make dlq` lists every dead-letter record with its order ID, error, and source position. Once the cause is fixed (a bug deployed, a dependency restored), `make dlq-replay TOPIC=<topic>.dlq` publishes the records back to their source topic. Replay is safe because consumers deduplicate by `message_id`: a record whose effects were in fact applied is skipped, and replaying the same record twice has no extra effect. Replay progress is committed under its own consumer group, so a second replay publishes only records that arrived since the first.
Alternative: skip failed records after logging them. That is simpler, but a skipped saga reply leaves its order stuck with no way to resume it.


## 12. Catalog reads are cached cache-aside, and the cache never decides stock

`GetProduct` and `ListProducts` in the inventory service read Redis first (`product:<id>`, `products:all`). On a miss they read Postgres and store the result with a TTL (30 seconds by default, `CACHE_TTL`). After a reservation or a release commits, the service deletes the changed products' keys and `products:all`. It deletes rather than writes the new value, so it can never put an older value over a newer one, and the next read simply loads the committed row.
The cache serves display reads only. A reservation never reads it: the authoritative check is the `SELECT ... FOR UPDATE` on the product rows inside the reserve transaction. A stale cached count can show a customer the wrong number for a moment, but it cannot cause an oversell.
Staleness has two bounds. Normally a count is stale only between the stock commit and the delete that follows it. When the delete is lost (Redis unreachable at that moment), or in the classic cache-aside race (a reader misses, reads the old row, the writer commits and deletes, then the reader stores the old row), the entry is stale for at most one TTL.
Redis is optional. If a cache call fails, the read goes to Postgres, the failure is logged and counted (`inventory_cache_errors_total`), and the cache is bypassed for 5 seconds so an outage costs one slow call instead of one per request. Each Redis call has a 250 ms deadline, which matters because a stopped Redis container takes about half a second to fail its DNS lookup. `/readyz` reports Redis as "degraded" but stays 200, so an orchestrator never takes inventory out of service for a cache outage. `CACHE_ENABLED=false` turns the cache off entirely, which the load test uses for its comparison.
Alternative: write-through (update the cached value inside the stock change). That keeps the cache warmer, but it couples every stock transaction to Redis, and two writers can still apply their cache writes in the opposite order from their commits.

## 13. Metrics: a few that answer specific questions

Every service exposes Prometheus metrics on its admin port, and the Compose stack runs Prometheus scraping all four every 5 seconds. Each metric answers one operational question:

| Metric | Question it answers |
| --- | --- |
| `http_server_request_duration_seconds{route,method,status}` (gateway) | How fast is the public API, per route, and how many requests fail? |
| `grpc_server_handling_seconds{method,code}` (order, payment, inventory) | Which backend call is slow or failing? |
| `saga_outcomes_total{outcome,reason}` (order) | How many orders end confirmed, cancelled for out of stock, cancelled for a failed payment, or stuck behind a dead-lettered reply? |
| `saga_duration_seconds{outcome}` (order) | How long does a customer wait from placing an order to a final answer? |
| `kafka_consumer_lag{group,topic,partition}` | Is a consumer falling behind? |
| `kafka_dlq_records_total{source_topic}` | Is anything being dead-lettered, and from where? |
| `outbox_pending_rows` | Is a relay falling behind or failing to publish? |
| `inventory_cache_{hits,misses,errors}_total` | Is the cache doing its job, and is Redis healthy? |

The HTTP route label is the ServeMux pattern (`/orders/{id}`), never the raw path. Every distinct label value is its own time series, so a path label would create one series per order. Saga outcomes are counted after the final transaction commits, so a redelivered reply, which deduplication skips, is never counted twice, and the saga duration is computed by the database clock (`clock_timestamp() - created_at`) in the same statement that sets the final status. Consumer lag is read every 10 seconds with admin requests (partition end offset minus the group's committed offset), so it measures what the group has durably finished, not what it has fetched.
Alternative: OpenTelemetry metrics, or a third-party gRPC metrics package. Both work, but the interceptor and HTTP middleware here are a few dozen lines on top of `client_golang`, and the stack has only one consumer of the metrics.
