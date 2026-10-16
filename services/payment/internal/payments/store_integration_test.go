//go:build integration

package payments

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/testinfra"
	eventsv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/events/v1"
	"github.com/omnomkar/event-driven-orders/services/payment/migrations"
)

// Standard test card numbers only.
const (
	testVisa    = "4242424242424242"
	testDecline = "4000000000000002"
)

func count(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func TestTokenizeStoresOnlyTokenLast4AndBrand(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()

	c, err := store.Tokenize(ctx, "4242 4242 4242 4242", 12, 2030)
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	if c.Last4 != "4242" || c.Brand != "visa" {
		t.Errorf("Tokenize = %+v, want last4 4242 brand visa", c)
	}
	declined, err := store.Tokenize(ctx, testDecline, 12, 2030)
	if err != nil {
		t.Fatalf("Tokenize decline card: %v", err)
	}
	if n := count(t, pool, `SELECT count(*) FROM cards WHERE token = $1 AND always_decline`, declined.Token); n != 1 {
		t.Error("decline test card is not marked always_decline")
	}

	// No column of any row in the payment database holds a full card number.
	for _, number := range []string{testVisa, testDecline} {
		if n := count(t, pool, `SELECT count(*) FROM cards c WHERE c::text LIKE '%' || $1 || '%'`, number); n != 0 {
			t.Errorf("cards table contains card number ending %s", number[len(number)-4:])
		}
	}

	// Validation failures store nothing.
	if _, err := store.Tokenize(ctx, "4242424242424241", 12, 2030); err == nil {
		t.Error("Tokenize accepted a number that fails the Luhn check")
	}
	if n := count(t, pool, `SELECT count(*) FROM cards`); n != 2 {
		t.Errorf("cards rows = %d, want 2", n)
	}
}

func TestChargeWritesPaymentAndOutboxTogether(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()
	c, err := store.Tokenize(ctx, testVisa, 12, 2030)
	if err != nil {
		t.Fatal(err)
	}
	orderID := uuid.NewString()

	p, err := store.Charge(ctx, orderID, c.Token, 11997, "USD")
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if n := count(t, pool, `SELECT count(*) FROM payments WHERE order_id = $1 AND status = 'SUCCEEDED' AND amount_cents = 11997`, orderID); n != 1 {
		t.Errorf("payments rows = %d, want 1", n)
	}
	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM outbox WHERE topic = $1 AND key = $2`,
		kafka.TopicPaymentEvents, orderID).Scan(&payload); err != nil {
		t.Fatalf("find PaymentSucceeded outbox row: %v", err)
	}
	var ev eventsv1.PaymentEvent
	if err := proto.Unmarshal(payload, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.GetPaymentSucceeded().GetPaymentId() != p.ID || ev.GetOrderId() != orderID {
		t.Errorf("PaymentSucceeded = %v; want payment %s for order %s", &ev, p.ID, orderID)
	}

	// An unknown token is rejected and writes neither a payment nor an event.
	if _, err := store.Charge(ctx, uuid.NewString(), "tok_unknown", 100, "USD"); !errors.Is(err, ErrUnknownToken) {
		t.Errorf("Charge(unknown token) error = %v, want ErrUnknownToken", err)
	}
	if n := count(t, pool, `SELECT count(*) FROM payments`); n != 1 {
		t.Errorf("payments rows = %d, want 1", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM outbox`); n != 1 {
		t.Errorf("outbox rows = %d, want 1", n)
	}
}

func TestCommandHandlerChargesFromKafkaRecord(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()
	c, err := store.Tokenize(ctx, testVisa, 12, 2030)
	if err != nil {
		t.Fatal(err)
	}
	orderID := uuid.NewString()
	payload, err := proto.Marshal(&eventsv1.PaymentCommand{
		MessageId: uuid.NewString(),
		OrderId:   orderID,
		Command: &eventsv1.PaymentCommand_ChargePayment{ChargePayment: &eventsv1.ChargePayment{
			PaymentToken: c.Token, AmountCents: 999, Currency: "USD",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	h := store.CommandHandler(testinfra.Logger())
	if err := h(ctx, &kgo.Record{Key: []byte(orderID), Value: payload}); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if n := count(t, pool, `SELECT count(*) FROM payments WHERE order_id = $1`, orderID); n != 1 {
		t.Errorf("payments for order = %d, want 1", n)
	}
	if err := h(ctx, &kgo.Record{Value: []byte("not protobuf")}); !kafka.IsPermanent(err) {
		t.Errorf("garbage payload error = %v, want permanent", err)
	}
}
