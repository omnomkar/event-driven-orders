package orders

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/omnomkar/event-driven-orders/pkg/dedup"
	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/outbox"
	eventsv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/events/v1"
)

// EventData carries the parts of a saga reply the order needs to keep.
type EventData struct {
	// TotalCents and Currency come from StockReserved: inventory owns prices.
	TotalCents int64
	Currency   string
	// Reason comes from StockRejected or PaymentFailed.
	Reason string
}

// Apply moves the order through the saga in response to e, the reply with
// ID messageID. In one transaction it records messageID as processed, locks
// the order row, asks next() for the new status and commands, updates the
// order, appends a history row, and writes every command to the outbox. It
// returns the new status, or an error wrapping dedup.ErrDuplicate if the
// reply was already applied.
//
// The row lock (SELECT ... FOR UPDATE) serializes concurrent events for the
// same order, so two replies can never both act on the same old status.
func (s *Store) Apply(ctx context.Context, messageID, orderID string, e Event, data EventData) (Status, error) {
	var to Status
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := dedup.Claim(ctx, tx, messageID); err != nil {
			return err
		}
		var (
			from         Status
			token        string
			total        *int64
			currency     *string
			cancelReason *string
		)
		err := tx.QueryRow(ctx, `
			SELECT status, payment_token, total_cents, currency, cancel_reason
			FROM orders WHERE id = $1 FOR UPDATE`, orderID,
		).Scan(&from, &token, &total, &currency, &cancelReason)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("lock order: %w", err)
		}

		var cmds []Command
		to, cmds, err = next(from, e)
		if err != nil {
			return err
		}

		if e == EventStockReserved {
			total, currency = &data.TotalCents, &data.Currency
		}
		// The reason arrives with the failure (StockRejected or
		// PaymentFailed). StockReleased carries none, so the reason stored
		// when the order entered CANCELLING is kept.
		if data.Reason != "" && (to == StatusCancelling || to == StatusCancelled) {
			cancelReason = &data.Reason
		}
		if _, err := tx.Exec(ctx, `
			UPDATE orders
			SET status = $2, total_cents = $3, currency = $4, cancel_reason = $5, updated_at = now()
			WHERE id = $1`, orderID, to, total, currency, cancelReason); err != nil {
			return fmt.Errorf("update order: %w", err)
		}
		if err := recordStatus(ctx, tx, orderID, to); err != nil {
			return err
		}
		for _, c := range cmds {
			topic, msg, err := buildCommand(c, orderID, token, deref(total), deref(currency), deref(cancelReason))
			if err != nil {
				return err
			}
			if err := outbox.Add(ctx, tx, topic, orderID, msg); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("apply %s to order %s: %w", e, orderID, err)
	}
	return to, nil
}

// buildCommand turns a saga command into the Kafka message that carries it.
func buildCommand(c Command, orderID, token string, totalCents int64, currency, reason string) (string, proto.Message, error) {
	id, now := uuid.NewString(), timestamppb.Now()
	switch c {
	case CommandChargePayment:
		return kafka.TopicPaymentCommands, &eventsv1.PaymentCommand{
			MessageId: id, OrderId: orderID, CreatedAt: now,
			Command: &eventsv1.PaymentCommand_ChargePayment{ChargePayment: &eventsv1.ChargePayment{
				PaymentToken: token, AmountCents: totalCents, Currency: currency,
			}},
		}, nil
	case CommandReleaseStock:
		return kafka.TopicInventoryCommands, &eventsv1.InventoryCommand{
			MessageId: id, OrderId: orderID, CreatedAt: now,
			Command: &eventsv1.InventoryCommand_ReleaseStock{ReleaseStock: &eventsv1.ReleaseStock{}},
		}, nil
	case CommandPublishConfirmed:
		return kafka.TopicOrderEvents, &eventsv1.OrderEvent{
			MessageId: id, OrderId: orderID, CreatedAt: now,
			Event: &eventsv1.OrderEvent_OrderConfirmed{OrderConfirmed: &eventsv1.OrderConfirmed{}},
		}, nil
	case CommandPublishCancelled:
		return kafka.TopicOrderEvents, &eventsv1.OrderEvent{
			MessageId: id, OrderId: orderID, CreatedAt: now,
			Event: &eventsv1.OrderEvent_OrderCancelled{OrderCancelled: &eventsv1.OrderCancelled{Reason: reason}},
		}, nil
	}
	return "", nil, fmt.Errorf("unknown saga command %d", c)
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}
