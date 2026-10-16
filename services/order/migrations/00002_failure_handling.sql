-- +goose Up
-- CANCELLING: payment failed and the reserved stock is being released.
ALTER TABLE orders DROP CONSTRAINT orders_status_check;
ALTER TABLE orders ADD CONSTRAINT orders_status_check
    CHECK (status IN ('PENDING', 'INVENTORY_RESERVED', 'CANCELLING', 'CONFIRMED', 'CANCELLED'));

-- Idempotency keys are scoped per customer, and each key remembers a hash of
-- the request it was first used with, so a reuse with different items can be
-- refused instead of silently returning the wrong order.
ALTER TABLE orders DROP CONSTRAINT orders_idempotency_key_key;
ALTER TABLE orders ADD COLUMN request_hash text;
ALTER TABLE orders ADD CONSTRAINT orders_customer_idempotency_key UNIQUE (customer_id, idempotency_key);

-- IDs of saga replies already handled (see pkg/dedup).
CREATE TABLE processed_messages (
    message_id   text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE processed_messages;
ALTER TABLE orders DROP CONSTRAINT orders_customer_idempotency_key;
ALTER TABLE orders DROP COLUMN request_hash;
ALTER TABLE orders ADD CONSTRAINT orders_idempotency_key_key UNIQUE (idempotency_key);
ALTER TABLE orders DROP CONSTRAINT orders_status_check;
ALTER TABLE orders ADD CONSTRAINT orders_status_check
    CHECK (status IN ('PENDING', 'INVENTORY_RESERVED', 'CONFIRMED', 'CANCELLED'));
