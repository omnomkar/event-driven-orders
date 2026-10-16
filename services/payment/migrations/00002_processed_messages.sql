-- +goose Up
-- IDs of commands already handled (see pkg/dedup).
CREATE TABLE processed_messages (
    message_id   text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE processed_messages;
