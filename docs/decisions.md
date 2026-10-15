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
The same `.proto` files define gRPC and events, so types are shared, payloads are compact, and `buf breaking` catches incompatible schema changes in CI.
Alternative: JSON events. They are easier to read in a console, but they have no enforced schema, so a renamed field breaks consumers silently.
