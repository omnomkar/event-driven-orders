//go:build integration

package orders

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/testinfra"
	eventsv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/events/v1"
	"github.com/omnomkar/event-driven-orders/services/order/migrations"
)

func record(t *testing.T, topic, orderID string, msg proto.Message) *kgo.Record {
	t.Helper()
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: topic, Key: []byte(orderID), Value: b}
}

func stockReserved(t *testing.T, orderID string) *kgo.Record {
	return record(t, kafka.TopicInventoryEvents, orderID, &eventsv1.InventoryEvent{
		MessageId: uuid.NewString(), OrderId: orderID,
		Event: &eventsv1.InventoryEvent_StockReserved{StockReserved: &eventsv1.StockReserved{
			TotalCents: 14997, Currency: "USD",
		}},
	})
}

func paymentSucceeded(t *testing.T, orderID string) *kgo.Record {
	return record(t, kafka.TopicPaymentEvents, orderID, &eventsv1.PaymentEvent{
		MessageId: uuid.NewString(), OrderId: orderID,
		Event: &eventsv1.PaymentEvent_PaymentSucceeded{PaymentSucceeded: &eventsv1.PaymentSucceeded{PaymentId: uuid.NewString()}},
	})
}

// The saga handler drives an order PENDING -> INVENTORY_RESERVED ->
// CONFIRMED, records each step in the history table, stores the total from
// inventory, and sends ChargePayment with that total and the order's token.
func TestSagaHappyPath(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()
	h := store.SagaHandler(testinfra.Logger())

	o, err := store.Create(ctx, validParams())
	if err != nil {
		t.Fatal(err)
	}

	if err := h(ctx, stockReserved(t, o.ID)); err != nil {
		t.Fatalf("StockReserved: %v", err)
	}
	got, _ := store.Get(ctx, o.ID)
	if got.Status != StatusInventoryReserved || got.TotalCents != 14997 || got.Currency != "USD" {
		t.Fatalf("after StockReserved: %+v; want INVENTORY_RESERVED with 14997 USD", got)
	}
	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM outbox WHERE topic = $1 AND key = $2`,
		kafka.TopicPaymentCommands, o.ID).Scan(&payload); err != nil {
		t.Fatalf("find ChargePayment outbox row: %v", err)
	}
	var cmd eventsv1.PaymentCommand
	if err := proto.Unmarshal(payload, &cmd); err != nil {
		t.Fatal(err)
	}
	charge := cmd.GetChargePayment()
	if charge.GetAmountCents() != 14997 || charge.GetCurrency() != "USD" || charge.GetPaymentToken() != "tok_test" {
		t.Errorf("ChargePayment = %v; want 14997 USD on tok_test", charge)
	}

	if err := h(ctx, paymentSucceeded(t, o.ID)); err != nil {
		t.Fatalf("PaymentSucceeded: %v", err)
	}
	got, _ = store.Get(ctx, o.ID)
	if got.Status != StatusConfirmed || got.TotalCents != 14997 {
		t.Fatalf("after PaymentSucceeded: %+v; want CONFIRMED keeping the total", got)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE topic = $1 AND key = $2`, kafka.TopicOrderEvents, o.ID).Scan(&n)
	if n != 2 {
		t.Errorf("order.events outbox rows = %d, want 2 (OrderCreated, OrderConfirmed)", n)
	}

	rows, err := pool.Query(ctx, `SELECT status FROM order_status_history WHERE order_id = $1 ORDER BY id`, o.ID)
	if err != nil {
		t.Fatal(err)
	}
	var history []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		history = append(history, s)
	}
	if want := []string{"PENDING", "INVENTORY_RESERVED", "CONFIRMED"}; !slices.Equal(history, want) {
		t.Errorf("history %v, want %v", history, want)
	}
}

// Events that are not valid for the order's state are permanent failures
// and change nothing.
func TestSagaRejectsOutOfOrderEvents(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()
	h := store.SagaHandler(testinfra.Logger())

	o, err := store.Create(ctx, validParams())
	if err != nil {
		t.Fatal(err)
	}
	err = h(ctx, paymentSucceeded(t, o.ID))
	if !kafka.IsPermanent(err) || !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("PaymentSucceeded while PENDING: error %v, want permanent ErrInvalidTransition", err)
	}
	if got, _ := store.Get(ctx, o.ID); got.Status != StatusPending {
		t.Errorf("status %s after a rejected event, want PENDING", got.Status)
	}

	if err := h(ctx, stockReserved(t, uuid.NewString())); !kafka.IsPermanent(err) || !errors.Is(err, ErrNotFound) {
		t.Errorf("event for unknown order: error %v, want permanent ErrNotFound", err)
	}
	if err := h(ctx, &kgo.Record{Topic: kafka.TopicPaymentEvents, Value: []byte("not protobuf")}); !kafka.IsPermanent(err) {
		t.Errorf("garbage payload: error %v, want permanent", err)
	}
}
