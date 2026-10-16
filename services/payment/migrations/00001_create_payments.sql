-- +goose Up
-- Tokenized cards. The card number itself is never stored: only an opaque
-- token, the last four digits, and the brand. CVV is never accepted at all.
CREATE TABLE cards (
    token          text PRIMARY KEY,
    last4          char(4) NOT NULL,
    brand          text NOT NULL,
    -- Set for the standard decline test card, so failure paths can be tested.
    always_decline boolean NOT NULL DEFAULT false,
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- One payment per order: the unique order_id is a backstop against double charges.
CREATE TABLE payments (
    id           uuid PRIMARY KEY,
    order_id     uuid NOT NULL UNIQUE,
    token        text NOT NULL REFERENCES cards (token),
    amount_cents bigint NOT NULL CHECK (amount_cents > 0),
    currency     char(3) NOT NULL,
    status       text NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED')),
    created_at   timestamptz NOT NULL DEFAULT now()
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
DROP TABLE payments;
DROP TABLE cards;
