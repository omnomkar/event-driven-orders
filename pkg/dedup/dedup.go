// Package dedup makes Kafka consumers safe to run twice. Kafka delivers each
// message at least once, so a handler can see the same message again after a
// crash, a rebalance, or a relay resend. Every message carries a unique
// message_id, and each service records the IDs it has handled in its own
// database:
//
//	CREATE TABLE processed_messages (
//	    message_id   text PRIMARY KEY,
//	    processed_at timestamptz NOT NULL DEFAULT now()
//	);
//
// A handler calls Claim as the first statement of the transaction that does
// its work. The ID and the work commit together or not at all, so a message
// is recorded as processed exactly when its effects are stored.
package dedup

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ErrDuplicate means the message was already processed. The caller should
// roll back, skip the work, and treat the message as handled.
var ErrDuplicate = errors.New("message already processed")

// Claim records messageID as processed inside tx. It returns ErrDuplicate if
// a committed transaction already recorded it.
//
// If another transaction has inserted the same ID but not committed yet, the
// insert waits for it: a commit makes this call return ErrDuplicate, and a
// rollback lets this call claim the ID. Two deliveries of one message can
// therefore never both do the work.
func Claim(ctx context.Context, tx pgx.Tx, messageID string) error {
	if messageID == "" {
		return errors.New("message has no message_id")
	}
	tag, err := tx.Exec(ctx,
		`INSERT INTO processed_messages (message_id) VALUES ($1) ON CONFLICT DO NOTHING`, messageID)
	if err != nil {
		return fmt.Errorf("record processed message: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrDuplicate
	}
	return nil
}
