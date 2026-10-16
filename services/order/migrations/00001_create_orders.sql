-- +goose Up
CREATE TABLE orders (
    id              uuid PRIMARY KEY,
    customer_id     text NOT NULL,
    status          text NOT NULL CHECK (status IN ('PENDING', 'INVENTORY_RESERVED', 'CONFIRMED', 'CANCELLED')),
    -- Opaque token from the payment service. Never a card number.
    payment_token   text NOT NULL,
    -- Set from StockReserved; inventory owns prices, so both are null while PENDING.
    total_cents     bigint CHECK (total_cents >= 0),
    currency        char(3),
    cancel_reason   text,
    -- Null when the client sent no Idempotency-Key header.
    idempotency_key text UNIQUE,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_id   uuid NOT NULL REFERENCES orders (id),
    position   int NOT NULL,
    product_id text NOT NULL,
    quantity   int NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, position),
    UNIQUE (order_id, product_id)
);

-- One row per state change, so an order's path through the saga can be audited.
CREATE TABLE order_status_history (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id   uuid NOT NULL REFERENCES orders (id),
    status     text NOT NULL,
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX order_status_history_order_idx ON order_status_history (order_id, id);

-- Messages waiting to be published by the outbox relay (see pkg/outbox).
CREATE TABLE outbox (
    id         uuid PRIMARY KEY,
    topic      text NOT NULL,
    key        text NOT NULL,
    payload    bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    sent_at    timestamptz
);
CREATE INDEX outbox_unsent_idx ON outbox (created_at) WHERE sent_at IS NULL;

-- +goose Down
DROP TABLE outbox;
DROP TABLE order_status_history;
DROP TABLE order_items;
DROP TABLE orders;
