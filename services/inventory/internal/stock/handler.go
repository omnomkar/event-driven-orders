package stock

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
		log := logging.WithOrder(logger, cmd.GetOrderId()).With("message_id", cmd.GetMessageId())

		switch c := cmd.GetCommand().(type) {
		case *eventsv1.InventoryCommand_ReserveStock:
			lines := make([]Line, len(c.ReserveStock.GetItems()))
			for i, it := range c.ReserveStock.GetItems() {
				lines[i] = Line{ProductID: it.GetProductId(), Quantity: it.GetQuantity()}
			}
			res, err := s.Reserve(ctx, cmd.GetOrderId(), lines)
			if isRejection(err) {
				// No retry can make stock appear, so do not retry.
				return kafka.Permanent(err)
			}
			if err != nil {
				return err
			}
			log.Info("stock reserved", "total_cents", res.TotalCents, "currency", res.Currency)
			return nil

		case *eventsv1.InventoryCommand_ReleaseStock:
			// TODO(stage 3): release the reservation and emit StockReleased.
			return kafka.Permanent(errors.New("ReleaseStock is not handled yet"))

		default:
			return kafka.Permanent(fmt.Errorf("unknown inventory command %T", c))
		}
	}
}

func isRejection(err error) bool {
	return errors.Is(err, ErrUnknownProduct) || errors.Is(err, ErrInsufficientStock) ||
		errors.Is(err, ErrMixedCurrency) || errors.Is(err, ErrInvalidLine)
}
