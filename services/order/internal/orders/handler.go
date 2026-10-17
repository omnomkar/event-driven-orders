package orders

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

// SagaTopics are the reply topics the saga consumer reads.
var SagaTopics = []string{kafka.TopicInventoryEvents, kafka.TopicPaymentEvents}

// reply is a decoded saga reply: which order, what happened, and its data.
type reply struct {
	orderID   string
	messageID string
	event     Event
	data      EventData
}

// SagaHandler returns a kafka.Handler for the inventory.events and
// payment.events topics. It is the saga orchestrator's entry point. Each
// saga that reaches a final state is recorded in m once, after its
// transaction commits; a redelivered reply is skipped by dedup and never
// counted twice.
func (s *Store) SagaHandler(logger *slog.Logger, m *SagaMetrics) kafka.Handler {
	return func(ctx context.Context, rec *kgo.Record) error {
		r, err := decodeReply(rec)
		if err != nil {
			return kafka.Permanent(err)
		}
		log := logging.WithOrder(logger, r.orderID).With("message_id", r.messageID, "event", r.event.String())

		applied, err := s.Apply(ctx, r.messageID, r.orderID, r.event, r.data)
		if errors.Is(err, dedup.ErrDuplicate) {
			log.Info("duplicate reply skipped")
			return nil
		}
		if errors.Is(err, ErrInvalidTransition) || errors.Is(err, ErrNotFound) {
			// Retrying cannot make the event valid for this order.
			return kafka.Permanent(err)
		}
		if err != nil {
			return err
		}
		m.finished(r.event, applied.To, applied.Elapsed)
		log.Info("order status changed", "status", applied.To)
		return nil
	}
}

// decodeReply decodes rec into a saga reply.
func decodeReply(rec *kgo.Record) (r reply, err error) {
	switch rec.Topic {
	case kafka.TopicInventoryEvents:
		var ev eventsv1.InventoryEvent
		if err := proto.Unmarshal(rec.Value, &ev); err != nil {
			return r, fmt.Errorf("decode inventory event: %w", err)
		}
		r.orderID, r.messageID = ev.GetOrderId(), ev.GetMessageId()
		switch e := ev.GetEvent().(type) {
		case *eventsv1.InventoryEvent_StockReserved:
			total, currency := e.StockReserved.GetTotalCents(), e.StockReserved.GetCurrency()
			if total <= 0 || len(currency) != 3 {
				return r, fmt.Errorf("StockReserved for order %s has invalid total %d %q", r.orderID, total, currency)
			}
			r.event, r.data = EventStockReserved, EventData{TotalCents: total, Currency: currency}
		case *eventsv1.InventoryEvent_StockRejected:
			r.event, r.data = EventStockRejected, EventData{Reason: e.StockRejected.GetReason()}
		case *eventsv1.InventoryEvent_StockReleased:
			r.event = EventStockReleased
		default:
			return r, fmt.Errorf("unknown inventory event %T", e)
		}

	case kafka.TopicPaymentEvents:
		var ev eventsv1.PaymentEvent
		if err := proto.Unmarshal(rec.Value, &ev); err != nil {
			return r, fmt.Errorf("decode payment event: %w", err)
		}
		r.orderID, r.messageID = ev.GetOrderId(), ev.GetMessageId()
		switch e := ev.GetEvent().(type) {
		case *eventsv1.PaymentEvent_PaymentSucceeded:
			r.event = EventPaymentSucceeded
		case *eventsv1.PaymentEvent_PaymentFailed:
			r.event, r.data = EventPaymentFailed, EventData{Reason: e.PaymentFailed.GetReason()}
		default:
			return r, fmt.Errorf("unknown payment event %T", e)
		}

	default:
		return r, fmt.Errorf("saga does not consume topic %q", rec.Topic)
	}
	if _, err := uuid.Parse(r.orderID); err != nil {
		return r, fmt.Errorf("saga reply has invalid order_id %q", r.orderID)
	}
	if r.messageID == "" {
		// Without an ID the reply cannot be deduplicated.
		return r, fmt.Errorf("saga reply for order %s has no message_id", r.orderID)
	}
	return r, nil
}
