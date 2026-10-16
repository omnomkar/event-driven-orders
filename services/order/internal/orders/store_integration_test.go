//go:build integration

package orders

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/testinfra"
	eventsv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/events/v1"
	"github.com/omnomkar/event-driven-orders/services/order/migrations"
)

func validParams() CreateParams {
	return CreateParams{
		CustomerID:   "cust-1",
		PaymentToken: "tok_test",
		Items: []Item{
			{ProductID: "sku-keyboard", Quantity: 1},
			{ProductID: "sku-mouse", Quantity: 2},
		},
	}
}

func count(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func TestCreateWritesOrderAndOutboxTogether(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()

	o, _, err := store.Create(ctx, validParams())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if o.Status != StatusPending {
		t.Errorf("new order status %s, want PENDING", o.Status)
	}
	if n := count(t, pool, `SELECT count(*) FROM orders WHERE id = $1 AND status = 'PENDING'`, o.ID); n != 1 {
		t.Errorf("orders rows = %d, want 1", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM order_items WHERE order_id = $1`, o.ID); n != 2 {
		t.Errorf("order_items rows = %d, want 2", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM order_status_history WHERE order_id = $1 AND status = 'PENDING'`, o.ID); n != 1 {
		t.Errorf("history rows = %d, want 1", n)
	}

	// The ReserveStock command is in the outbox, keyed by the order ID and
	// carrying the order's items.
	var payload []byte
	err = pool.QueryRow(ctx, `
		SELECT payload FROM outbox WHERE topic = $1 AND key = $2 AND sent_at IS NULL`,
		kafka.TopicInventoryCommands, o.ID).Scan(&payload)
	if err != nil {
		t.Fatalf("find ReserveStock outbox row: %v", err)
	}
	var cmd eventsv1.InventoryCommand
	if err := proto.Unmarshal(payload, &cmd); err != nil {
		t.Fatalf("unmarshal command: %v", err)
	}
	items := cmd.GetReserveStock().GetItems()
	if cmd.GetOrderId() != o.ID || cmd.GetMessageId() == "" || len(items) != 2 || items[1].GetQuantity() != 2 {
		t.Errorf("ReserveStock = %v; want order %s with 2 items", &cmd, o.ID)
	}
	if n := count(t, pool, `SELECT count(*) FROM outbox WHERE topic = $1 AND key = $2`, kafka.TopicOrderEvents, o.ID); n != 1 {
		t.Errorf("OrderCreated outbox rows = %d, want 1", n)
	}

	got, err := store.Get(ctx, o.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != StatusPending || len(got.Items) != 2 || got.Items[0].ProductID != "sku-keyboard" {
		t.Errorf("Get = %+v; want the PENDING order with its 2 items in order", got)
	}
}

// If anything fails after the order row is inserted, the transaction rolls
// back and neither the order nor its outbox rows exist.
func TestCreateFailureWritesNeitherOrderNorOutbox(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	ctx := context.Background()

	// Make every outbox insert fail. Create inserts the order (and its items
	// and history) first, so this failure happens after those rows are
	// already written inside the transaction.
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION fail_outbox() RETURNS trigger LANGUAGE plpgsql AS
		$$ BEGIN RAISE EXCEPTION 'forced outbox failure'; END $$;
		CREATE TRIGGER fail_outbox BEFORE INSERT ON outbox
		FOR EACH ROW EXECUTE FUNCTION fail_outbox();`); err != nil {
		t.Fatalf("install failing trigger: %v", err)
	}

	if _, _, err := NewStore(pool).Create(ctx, validParams()); err == nil {
		t.Fatal("Create succeeded; want the forced outbox failure")
	}
	for _, table := range []string{"orders", "order_items", "order_status_history", "outbox"} {
		if n := count(t, pool, `SELECT count(*) FROM `+table); n != 0 {
			t.Errorf("%s has %d rows after a failed Create, want 0", table, n)
		}
	}
}

func TestGetUnknownOrder(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	_, err := NewStore(pool).Get(context.Background(), "0192f5d1-0000-7000-8000-000000000000")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(unknown) error = %v, want ErrNotFound", err)
	}
}

// The same idempotency key and items return the original order without
// writing anything new. Different items with that key are refused, and the
// key is scoped per customer.
func TestCreateIdempotencyKey(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()

	p := validParams()
	p.IdempotencyKey = "key-1"
	first, replayed, err := store.Create(ctx, p)
	if err != nil || replayed {
		t.Fatalf("first Create: replayed %v, error %v", replayed, err)
	}

	// Same items listed in another order, with a fresh token: same request.
	again := p
	again.PaymentToken = "tok_other"
	again.Items = []Item{p.Items[1], p.Items[0]}
	second, replayed, err := store.Create(ctx, again)
	if err != nil || !replayed || second.ID != first.ID {
		t.Fatalf("repeat Create = %v, replayed %v, error %v; want order %s replayed", second, replayed, err, first.ID)
	}
	if n := count(t, pool, `SELECT count(*) FROM orders`); n != 1 {
		t.Errorf("orders rows = %d, want 1", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM outbox WHERE topic = $1`, kafka.TopicInventoryCommands); n != 1 {
		t.Errorf("ReserveStock outbox rows = %d, want 1", n)
	}

	changed := p
	changed.Items = []Item{{ProductID: "sku-keyboard", Quantity: 5}}
	if _, _, err := store.Create(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Errorf("same key, different items: error %v, want ErrIdempotencyConflict", err)
	}

	other := p
	other.CustomerID = "cust-2"
	o, replayed, err := store.Create(ctx, other)
	if err != nil || replayed || o.ID == first.ID {
		t.Errorf("same key, other customer: replayed %v, error %v; want a new order", replayed, err)
	}
}

// Concurrent requests with one key: the unique constraint lets one insert
// win, and every other request returns the winner's order.
func TestCreateIdempotencyKeyConcurrent(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	p := validParams()
	p.IdempotencyKey = "key-race"

	const requests = 10
	ids := make([]string, requests)
	var wg sync.WaitGroup
	for i := range requests {
		wg.Go(func() {
			o, _, err := store.Create(context.Background(), p)
			if err != nil {
				t.Errorf("request %d: %v", i, err)
				return
			}
			ids[i] = o.ID
		})
	}
	wg.Wait()
	for i, id := range ids {
		if id != ids[0] {
			t.Fatalf("request %d got order %s, request 0 got %s; want one order", i, id, ids[0])
		}
	}
	if n := count(t, pool, `SELECT count(*) FROM orders`); n != 1 {
		t.Errorf("orders rows = %d, want 1", n)
	}
}
