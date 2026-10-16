package payments

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/omnomkar/event-driven-orders/pkg/dedup"
	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/logging"
	eventsv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/events/v1"
)

// CommandHandler returns a kafka.Handler for the payment.commands topic.
func (s *Store) CommandHandler(logger *slog.Logger) kafka.Handler {
	return func(ctx context.Context, rec *kgo.Record) error {
		var cmd eventsv1.PaymentCommand
		if err := proto.Unmarshal(rec.Value, &cmd); err != nil {
			return kafka.Permanent(fmt.Errorf("decode payment command: %w", err))
		}
		if _, err := uuid.Parse(cmd.GetOrderId()); err != nil {
			return kafka.Permanent(fmt.Errorf("payment command has invalid order_id %q", cmd.GetOrderId()))
		}
		if cmd.GetMessageId() == "" {
			// Without an ID the command cannot be deduplicated, and a
			// duplicate here would be a double charge.
			return kafka.Permanent(fmt.Errorf("payment command for order %s has no message_id", cmd.GetOrderId()))
		}
		log := logging.WithOrder(logger, cmd.GetOrderId()).With("message_id", cmd.GetMessageId())

		switch c := cmd.GetCommand().(type) {
		case *eventsv1.PaymentCommand_ChargePayment:
			ch := c.ChargePayment
			p, err := s.Charge(ctx, cmd.GetMessageId(), cmd.GetOrderId(), ch.GetPaymentToken(), ch.GetAmountCents(), ch.GetCurrency())
			switch {
			case errors.Is(err, dedup.ErrDuplicate):
				log.Info("duplicate command skipped")
				return nil
			case errors.Is(err, ErrAlreadyCharged):
				// Never charge an order twice; leave it for manual review.
				return kafka.Permanent(err)
			case err != nil:
				return err
			case p.Status == StatusFailed:
				log.Info("payment failed", "reason", p.FailureReason, "amount_cents", p.AmountCents, "currency", p.Currency)
			default:
				log.Info("payment succeeded", "payment_id", p.ID, "amount_cents", p.AmountCents, "currency", p.Currency)
			}
			return nil

		default:
			return kafka.Permanent(fmt.Errorf("unknown payment command %T", c))
		}
	}
}
