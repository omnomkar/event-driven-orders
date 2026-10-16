package payments

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

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
		log := logging.WithOrder(logger, cmd.GetOrderId()).With("message_id", cmd.GetMessageId())

		switch c := cmd.GetCommand().(type) {
		case *eventsv1.PaymentCommand_ChargePayment:
			ch := c.ChargePayment
			p, err := s.Charge(ctx, cmd.GetOrderId(), ch.GetPaymentToken(), ch.GetAmountCents(), ch.GetCurrency())
			if errors.Is(err, ErrUnknownToken) || errors.Is(err, ErrInvalidCharge) {
				// TODO(stage 3): emit PaymentFailed so the saga compensates.
				return kafka.Permanent(err)
			}
			if err != nil {
				return err
			}
			log.Info("payment succeeded", "payment_id", p.ID, "amount_cents", p.AmountCents, "currency", p.Currency)
			return nil

		default:
			return kafka.Permanent(fmt.Errorf("unknown payment command %T", c))
		}
	}
}
