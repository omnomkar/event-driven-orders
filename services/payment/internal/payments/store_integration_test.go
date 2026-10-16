//go:build integration

package payments

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

// paymentEvents decodes the outbox replies for an order, oldest first.
func paymentEvents(t *testing.T, pool *pgxpool.Pool, orderID string) []*eventsv1.PaymentEvent {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT payload FROM outbox WHERE topic = $1 AND key = $2 ORDER BY created_at`, kafka.TopicPaymentEvents, orderID)
	if err != nil {
		t.Fatal(err)
	}
	payloads, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		t.Fatal(err)
	}
	events := make([]*eventsv1.PaymentEvent, len(payloads))
	for i, b := range payloads {
		events[i] = &eventsv1.PaymentEvent{}
		if err := proto.Unmarshal(b, events[i]); err != nil {
			t.Fatal(err)
		}
	}
	return events
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

	p, err := store.Charge(ctx, uuid.NewString(), orderID, c.Token, 11997, "USD")
	if err != nil || p.Status != StatusSucceeded {
		t.Fatalf("Charge = %+v, %v; want SUCCEEDED", p, err)
	}
	if n := count(t, pool, `SELECT count(*) FROM payments WHERE order_id = $1 AND status = 'SUCCEEDED' AND amount_cents = 11997`, orderID); n != 1 {
		t.Errorf("payments rows = %d, want 1", n)
	}
	events := paymentEvents(t, pool, orderID)
	if len(events) != 1 || events[0].GetPaymentSucceeded().GetPaymentId() != p.ID || events[0].GetOrderId() != orderID {
		t.Errorf("outbox events = %v; want one PaymentSucceeded for payment %s", events, p.ID)
	}
}

// The decline test card records a FAILED payment and replies PaymentFailed in
// the same transaction. An unknown token or an unusable amount records no
// payment but still replies PaymentFailed, so the order is never left
// waiting.
func TestChargeFailuresReplyPaymentFailed(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()
	declined, err := store.Tokenize(ctx, testDecline, 12, 2030)
	if err != nil {
		t.Fatal(err)
	}
	visa, err := store.Tokenize(ctx, testVisa, 12, 2030)
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name       string
		token      string
		amount     int64
		wantReason string
		wantRows   int
	}{
		{"declined card", declined.Token, 11997, ReasonDeclined, 1},
		{"unknown token", "tok_unknown", 100, ReasonUnknownToken, 0},
		{"zero amount", visa.Token, 0, ReasonInvalidAmount, 0},
	} {
		orderID := uuid.NewString()
		p, err := store.Charge(ctx, uuid.NewString(), orderID, tt.token, tt.amount, "USD")
		if err != nil || p.Status != StatusFailed || p.FailureReason != tt.wantReason {
			t.Errorf("%s: Charge = %+v, %v; want FAILED with %q", tt.name, p, err, tt.wantReason)
			continue
		}
		if n := count(t, pool, `SELECT count(*) FROM payments WHERE order_id = $1 AND status = 'FAILED'`, orderID); n != tt.wantRows {
			t.Errorf("%s: FAILED payment rows = %d, want %d", tt.name, n, tt.wantRows)
		}
		events := paymentEvents(t, pool, orderID)
		if len(events) != 1 || events[0].GetPaymentFailed().GetReason() != tt.wantReason {
			t.Errorf("%s: outbox events = %v; want one PaymentFailed(%s)", tt.name, events, tt.wantReason)
		}
	}
	if n := count(t, pool, `SELECT count(*) FROM payments WHERE status = 'SUCCEEDED'`); n != 0 {
		t.Errorf("SUCCEEDED payments = %d, want 0", n)
	}
}

func chargeCommand(t *testing.T, messageID, orderID, token string) *kgo.Record {
	t.Helper()
	payload, err := proto.Marshal(&eventsv1.PaymentCommand{
		MessageId: messageID,
		OrderId:   orderID,
		Command: &eventsv1.PaymentCommand_ChargePayment{ChargePayment: &eventsv1.ChargePayment{
			PaymentToken: token, AmountCents: 999, Currency: "USD",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: kafka.TopicPaymentCommands, Key: []byte(orderID), Value: payload}
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

	h := store.CommandHandler(testinfra.Logger())
	if err := h(ctx, chargeCommand(t, uuid.NewString(), orderID, c.Token)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if n := count(t, pool, `SELECT count(*) FROM payments WHERE order_id = $1`, orderID); n != 1 {
		t.Errorf("payments for order = %d, want 1", n)
	}
	if err := h(ctx, &kgo.Record{Value: []byte("not protobuf")}); !kafka.IsPermanent(err) {
		t.Errorf("garbage payload error = %v, want permanent", err)
	}
	if err := h(ctx, chargeCommand(t, "", uuid.NewString(), c.Token)); !kafka.IsPermanent(err) {
		t.Errorf("missing message_id error = %v, want permanent", err)
	}
}

// A redelivered ChargePayment (same message_id) charges once and replies
// once. A second ChargePayment for the same order under a new message_id is
// stopped by the unique order_id and goes to manual review, not retried.
func TestCommandHandlerChargesOnceOnRedelivery(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()
	c, err := store.Tokenize(ctx, testVisa, 12, 2030)
	if err != nil {
		t.Fatal(err)
	}
	orderID := uuid.NewString()
	h := store.CommandHandler(testinfra.Logger())

	rec := chargeCommand(t, uuid.NewString(), orderID, c.Token)
	for i := range 3 {
		if err := h(ctx, rec); err != nil {
			t.Fatalf("delivery %d: %v", i+1, err)
		}
	}
	if n := count(t, pool, `SELECT count(*) FROM payments WHERE order_id = $1`, orderID); n != 1 {
		t.Errorf("payments after 3 deliveries = %d, want 1", n)
	}
	if n := len(paymentEvents(t, pool, orderID)); n != 1 {
		t.Errorf("replies after 3 deliveries = %d, want 1", n)
	}

	err = h(ctx, chargeCommand(t, uuid.NewString(), orderID, c.Token))
	if !kafka.IsPermanent(err) || !errors.Is(err, ErrAlreadyCharged) {
		t.Errorf("second ChargePayment: error %v, want permanent ErrAlreadyCharged", err)
	}
	if n := count(t, pool, `SELECT count(*) FROM payments WHERE order_id = $1`, orderID); n != 1 {
		t.Errorf("payments after a second command = %d, want 1", n)
	}
}
