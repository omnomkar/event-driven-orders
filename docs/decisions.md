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
A record that fails permanently, for example one that cannot be decoded or arrives in the wrong state, goes through a single failure handler shared by every consumer. Today it logs the record with its topic, partition, and offset. Routing the record to that topic's dead-letter topic only means replacing that one handler, so no failed record is dropped without a trace.
Alternative: Kafka's exactly-once semantics. They cover read-process-write loops that stay inside Kafka, but our side effects are Postgres writes, so idempotent consumers are needed either way.
