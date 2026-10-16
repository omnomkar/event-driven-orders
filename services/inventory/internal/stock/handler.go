package stock

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

// CommandHandler returns a kafka.Handler for the inventory.commands topic.
func (s *Store) CommandHandler(logger *slog.Logger) kafka.Handler {
	return func(ctx context.Context, rec *kgo.Record) error {
		var cmd eventsv1.InventoryCommand
		if err := proto.Unmarshal(rec.Value, &cmd); err != nil {
			return kafka.Permanent(fmt.Errorf("decode inventory command: %w", err))
		}
		if _, err := uuid.Parse(cmd.GetOrderId()); err != nil {
			return kafka.Permanent(fmt.Errorf("inventory command has invalid order_id %q", cmd.GetOrderId()))
		}
		if cmd.GetMessageId() == "" {
			// Without an ID the command cannot be deduplicated.
			return kafka.Permanent(fmt.Errorf("inventory command for order %s has no message_id", cmd.GetOrderId()))
		}
		log := logging.WithOrder(logger, cmd.GetOrderId()).With("message_id", cmd.GetMessageId())

		var err error
		switch c := cmd.GetCommand().(type) {
		case *eventsv1.InventoryCommand_ReserveStock:
			lines := make([]Line, len(c.ReserveStock.GetItems()))
			for i, it := range c.ReserveStock.GetItems() {
				lines[i] = Line{ProductID: it.GetProductId(), Quantity: it.GetQuantity()}
			}
			var (
				res *Reservation
				rej *Rejection
			)
			res, rej, err = s.Reserve(ctx, cmd.GetMessageId(), cmd.GetOrderId(), lines)
			switch {
			case err != nil:
				// handled below
			case rej != nil:
				log.Info("stock rejected", "reason", rej.Reason)
			default:
				log.Info("stock reserved", "total_cents", res.TotalCents, "currency", res.Currency)
			}

		case *eventsv1.InventoryCommand_ReleaseStock:
			var result ReleaseResult
			result, err = s.Release(ctx, cmd.GetMessageId(), cmd.GetOrderId())
			if err == nil {
				log.Info("stock released", "result", string(result))
			}

		default:
			return kafka.Permanent(fmt.Errorf("unknown inventory command %T", c))
		}

		switch {
		case errors.Is(err, dedup.ErrDuplicate):
			log.Info("duplicate command skipped")
			return nil
		case errors.Is(err, ErrAlreadyReserved):
			return kafka.Permanent(err)
		}
		return err
	}
}
