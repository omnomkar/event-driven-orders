-- +goose Up
CREATE TABLE products (
    id               text PRIMARY KEY,
    name             text NOT NULL,
    -- Money is integer cents plus an ISO 4217 code, never a float.
    unit_price_cents bigint NOT NULL CHECK (unit_price_cents >= 0),
    currency         char(3) NOT NULL,
    -- The CHECK is a last line of defense: stock can never go negative even
    -- if application code has a bug.
    available        int NOT NULL CHECK (available >= 0),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- One reservation per order. Lines record the price charged at reservation time.
CREATE TABLE reservations (
    order_id    uuid PRIMARY KEY,
    status      text NOT NULL CHECK (status IN ('RESERVED', 'RELEASED')),
    total_cents bigint NOT NULL CHECK (total_cents >= 0),
    currency    char(3) NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE reservation_lines (
    order_id         uuid NOT NULL REFERENCES reservations (order_id),
    product_id       text NOT NULL REFERENCES products (id),
    quantity         int NOT NULL CHECK (quantity > 0),
    unit_price_cents bigint NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

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
DROP TABLE reservation_lines;
DROP TABLE reservations;
DROP TABLE products;
