//go:build integration

package orders

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
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
	m := NewSagaMetrics(prometheus.NewRegistry())
	h := store.SagaHandler(testinfra.Logger(), m)

	o, _, err := store.Create(ctx, validParams())
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
	if got := outcomes(m, OutcomeConfirmed, ReasonNone); got != 1 {
		t.Errorf("confirmed outcomes = %v, want 1", got)
	}
	if got := durations(t, m, OutcomeConfirmed); got != 1 {
		t.Errorf("confirmed durations = %d, want 1", got)
	}
}

// Events that are not valid for the order's state are permanent failures
// and change nothing.
func TestSagaRejectsOutOfOrderEvents(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()
	m := NewSagaMetrics(prometheus.NewRegistry())
	h := store.SagaHandler(testinfra.Logger(), m)

	o, _, err := store.Create(ctx, validParams())
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

func inventoryEvent(t *testing.T, orderID string, ev *eventsv1.InventoryEvent) *kgo.Record {
	ev.MessageId, ev.OrderId = uuid.NewString(), orderID
	return record(t, kafka.TopicInventoryEvents, orderID, ev)
}

func paymentFailed(t *testing.T, orderID, reason string) *kgo.Record {
	return record(t, kafka.TopicPaymentEvents, orderID, &eventsv1.PaymentEvent{
		MessageId: uuid.NewString(), OrderId: orderID,
		Event: &eventsv1.PaymentEvent_PaymentFailed{PaymentFailed: &eventsv1.PaymentFailed{Reason: reason}},
	})
}

func history(t *testing.T, pool *pgxpool.Pool, orderID string) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT status FROM order_status_history WHERE order_id = $1 ORDER BY id`, orderID)
	if err != nil {
		t.Fatal(err)
	}
	h, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// outboxRows returns the payloads of the order's outbox rows on topic, oldest first.
func outboxRows(t *testing.T, pool *pgxpool.Pool, topic, orderID string) [][]byte {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT payload FROM outbox WHERE topic = $1 AND key = $2 ORDER BY created_at`, topic, orderID)
	if err != nil {
		t.Fatal(err)
	}
	payloads, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		t.Fatal(err)
	}
	return payloads
}

// A failed payment compensates: the order moves to CANCELLING and sends
// ReleaseStock, and only once inventory replies StockReleased does it become
// CANCELLED and announce OrderCancelled with the payment's reason.
func TestSagaPaymentFailureReleasesStockThenCancels(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()
	m := NewSagaMetrics(prometheus.NewRegistry())
	h := store.SagaHandler(testinfra.Logger(), m)

	o, _, err := store.Create(ctx, validParams())
	if err != nil {
		t.Fatal(err)
	}
	if err := h(ctx, stockReserved(t, o.ID)); err != nil {
		t.Fatalf("StockReserved: %v", err)
	}
	if err := h(ctx, paymentFailed(t, o.ID, "card declined")); err != nil {
		t.Fatalf("PaymentFailed: %v", err)
	}
	got, _ := store.Get(ctx, o.ID)
	if got.Status != StatusCancelling || got.CancelReason != "card declined" {
		t.Fatalf("after PaymentFailed: %+v; want CANCELLING with the reason", got)
	}
	if got := outcomes(m, OutcomeCancelled, ReasonPaymentFailed); got != 0 {
		t.Errorf("payment_failed outcomes while CANCELLING = %v, want 0 (stock not released yet)", got)
	}
	release := outboxRows(t, pool, kafka.TopicInventoryCommands, o.ID)
	var cmd eventsv1.InventoryCommand
	if len(release) != 2 || proto.Unmarshal(release[1], &cmd) != nil || cmd.GetReleaseStock() == nil {
		t.Fatalf("inventory.commands outbox rows = %d; want ReserveStock then ReleaseStock", len(release))
	}

	if err := h(ctx, inventoryEvent(t, o.ID, &eventsv1.InventoryEvent{
		Event: &eventsv1.InventoryEvent_StockReleased{StockReleased: &eventsv1.StockReleased{}},
	})); err != nil {
		t.Fatalf("StockReleased: %v", err)
	}
	got, _ = store.Get(ctx, o.ID)
	if got.Status != StatusCancelled || got.CancelReason != "card declined" {
		t.Fatalf("after StockReleased: %+v; want CANCELLED keeping the reason", got)
	}
	events := outboxRows(t, pool, kafka.TopicOrderEvents, o.ID)
	var ev eventsv1.OrderEvent
	if len(events) != 2 || proto.Unmarshal(events[1], &ev) != nil || ev.GetOrderCancelled().GetReason() != "card declined" {
		t.Errorf("order.events = %d rows, last %v; want OrderCreated then OrderCancelled(card declined)", len(events), &ev)
	}
	want := []string{"PENDING", "INVENTORY_RESERVED", "CANCELLING", "CANCELLED"}
	if got := history(t, pool, o.ID); !slices.Equal(got, want) {
		t.Errorf("history %v, want %v", got, want)
	}
	if got := outcomes(m, OutcomeCancelled, ReasonPaymentFailed); got != 1 {
		t.Errorf("payment_failed outcomes = %v, want 1", got)
	}
	if got := durations(t, m, OutcomeCancelled); got != 1 {
		t.Errorf("cancelled durations = %d, want 1", got)
	}
}

// Out of stock cancels the order directly: nothing was reserved, so there is
// nothing to release and no charge is ever sent.
func TestSagaStockRejectedCancelsWithoutPayment(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()
	m := NewSagaMetrics(prometheus.NewRegistry())
	h := store.SagaHandler(testinfra.Logger(), m)

	o, _, err := store.Create(ctx, validParams())
	if err != nil {
		t.Fatal(err)
	}
	if err := h(ctx, inventoryEvent(t, o.ID, &eventsv1.InventoryEvent{
		Event: &eventsv1.InventoryEvent_StockRejected{StockRejected: &eventsv1.StockRejected{
			Reason: "insufficient stock: sku-mouse", ProductId: "sku-mouse",
		}},
	})); err != nil {
		t.Fatalf("StockRejected: %v", err)
	}
	got, _ := store.Get(ctx, o.ID)
	if got.Status != StatusCancelled || got.CancelReason != "insufficient stock: sku-mouse" {
		t.Errorf("after StockRejected: %+v; want CANCELLED with the reason", got)
	}
	if n := len(outboxRows(t, pool, kafka.TopicPaymentCommands, o.ID)); n != 0 {
		t.Errorf("payment.commands outbox rows = %d, want 0", n)
	}
	if want := []string{"PENDING", "CANCELLED"}; !slices.Equal(history(t, pool, o.ID), want) {
		t.Errorf("history %v, want %v", history(t, pool, o.ID), want)
	}
	if oos, pf := outcomes(m, OutcomeCancelled, ReasonOutOfStock), outcomes(m, OutcomeCancelled, ReasonPaymentFailed); oos != 1 || pf != 0 {
		t.Errorf("out_of_stock, payment_failed outcomes = %v, %v; want 1, 0", oos, pf)
	}
}

// A redelivered reply (same message_id) is skipped: it returns nil so the
// offset is committed, and it changes nothing.
func TestSagaSkipsRedeliveredReply(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()
	m := NewSagaMetrics(prometheus.NewRegistry())
	h := store.SagaHandler(testinfra.Logger(), m)

	o, _, err := store.Create(ctx, validParams())
	if err != nil {
		t.Fatal(err)
	}
	rec := stockReserved(t, o.ID)
	for i := range 2 {
		if err := h(ctx, rec); err != nil {
			t.Fatalf("delivery %d: %v", i+1, err)
		}
	}
	if n := len(outboxRows(t, pool, kafka.TopicPaymentCommands, o.ID)); n != 1 {
		t.Errorf("ChargePayment outbox rows = %d, want 1", n)
	}
	if want := []string{"PENDING", "INVENTORY_RESERVED"}; !slices.Equal(history(t, pool, o.ID), want) {
		t.Errorf("history %v, want %v", history(t, pool, o.ID), want)
	}

	// The final reply delivered twice confirms, and is counted, once.
	confirm := paymentSucceeded(t, o.ID)
	for i := range 2 {
		if err := h(ctx, confirm); err != nil {
			t.Fatalf("PaymentSucceeded delivery %d: %v", i+1, err)
		}
	}
	if got := outcomes(m, OutcomeConfirmed, ReasonNone); got != 1 {
		t.Errorf("confirmed outcomes after a redelivery = %v, want 1", got)
	}

	// A reply with no message_id cannot be deduplicated, so it is refused.
	noID := record(t, kafka.TopicPaymentEvents, o.ID, &eventsv1.PaymentEvent{
		OrderId: o.ID,
		Event:   &eventsv1.PaymentEvent_PaymentSucceeded{PaymentSucceeded: &eventsv1.PaymentSucceeded{}},
	})
	if err := h(ctx, noID); !kafka.IsPermanent(err) {
		t.Errorf("reply without message_id: error %v, want permanent", err)
	}
}
