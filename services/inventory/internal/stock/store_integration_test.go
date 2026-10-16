//go:build integration

package stock

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/testinfra"
	commonv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/common/v1"
	eventsv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/events/v1"
	"github.com/omnomkar/event-driven-orders/services/inventory/migrations"
)

func available(t *testing.T, pool *pgxpool.Pool, productID string) int32 {
	t.Helper()
	var n int32
	if err := pool.QueryRow(context.Background(), `SELECT available FROM products WHERE id = $1`, productID).Scan(&n); err != nil {
		t.Fatalf("read stock for %s: %v", productID, err)
	}
	return n
}

func count(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func TestReserveDecrementsStockAndWritesOutbox(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()
	orderID := uuid.NewString()
	keyboards, mice := available(t, pool, "sku-keyboard"), available(t, pool, "sku-mouse")

	res, err := store.Reserve(ctx, orderID, []Line{
		{ProductID: "sku-mouse", Quantity: 2},
		{ProductID: "sku-keyboard", Quantity: 1},
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	const wantTotal = 2*2999 + 8999
	if res.TotalCents != wantTotal || res.Currency != "USD" {
		t.Errorf("reservation total %d %s, want %d USD", res.TotalCents, res.Currency, wantTotal)
	}
	if got := available(t, pool, "sku-keyboard"); got != keyboards-1 {
		t.Errorf("keyboards available %d, want %d", got, keyboards-1)
	}
	if got := available(t, pool, "sku-mouse"); got != mice-2 {
		t.Errorf("mice available %d, want %d", got, mice-2)
	}
	if n := count(t, pool, `SELECT count(*) FROM reservation_lines WHERE order_id = $1`, orderID); n != 2 {
		t.Errorf("reservation lines %d, want 2", n)
	}

	// StockReserved is in the outbox with the prices and total.
	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM outbox WHERE topic = $1 AND key = $2`,
		kafka.TopicInventoryEvents, orderID).Scan(&payload); err != nil {
		t.Fatalf("find StockReserved outbox row: %v", err)
	}
	var ev eventsv1.InventoryEvent
	if err := proto.Unmarshal(payload, &ev); err != nil {
		t.Fatal(err)
	}
	sr := ev.GetStockReserved()
	if sr.GetTotalCents() != wantTotal || sr.GetCurrency() != "USD" || len(sr.GetLines()) != 2 || sr.GetLines()[0].GetUnitPriceCents() != 2999 {
		t.Errorf("StockReserved = %v; want total %d USD with unit prices", sr, wantTotal)
	}
}

// All or nothing: one short line means no line is reserved and nothing is
// written.
func TestReserveShortLineChangesNothing(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	keyboards := available(t, pool, "sku-keyboard")

	for name, lines := range map[string][]Line{
		"insufficient": {{ProductID: "sku-keyboard", Quantity: 1}, {ProductID: "sku-limited", Quantity: 6}},
		"unknown":      {{ProductID: "sku-keyboard", Quantity: 1}, {ProductID: "sku-missing", Quantity: 1}},
	} {
		_, err := store.Reserve(context.Background(), uuid.NewString(), lines)
		if !errors.Is(err, ErrInsufficientStock) && !errors.Is(err, ErrUnknownProduct) {
			t.Errorf("%s: Reserve error = %v, want a rejection", name, err)
		}
	}
	if got := available(t, pool, "sku-keyboard"); got != keyboards {
		t.Errorf("keyboards available %d, want unchanged %d", got, keyboards)
	}
	if got := available(t, pool, "sku-limited"); got != 5 {
		t.Errorf("sku-limited available %d, want unchanged 5", got)
	}
	for _, table := range []string{"reservations", "reservation_lines", "outbox"} {
		if n := count(t, pool, `SELECT count(*) FROM `+table); n != 0 {
			t.Errorf("%s has %d rows, want 0", table, n)
		}
	}
}

// Many orders race for the last 5 units, half of them listing their products
// in the opposite order. Row locks taken in product ID order mean there is no
// oversell and no deadlock: exactly 5 orders succeed and the rest are rejected.
func TestConcurrentReservationsNeverOversellOrDeadlock(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	const orders = 20

	var (
		wg              sync.WaitGroup
		mu              sync.Mutex
		reserved, short int
	)
	for i := range orders {
		lines := []Line{{ProductID: "sku-limited", Quantity: 1}, {ProductID: "sku-keyboard", Quantity: 1}}
		if i%2 == 1 {
			lines[0], lines[1] = lines[1], lines[0]
		}
		wg.Go(func() {
			_, err := store.Reserve(context.Background(), uuid.NewString(), lines)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				reserved++
			case errors.Is(err, ErrInsufficientStock):
				short++
			default:
				t.Errorf("unexpected error (deadlock?): %v", err)
			}
		})
	}
	wg.Wait()

	if reserved != 5 || short != orders-5 {
		t.Errorf("reserved %d, rejected %d; want 5 and %d", reserved, short, orders-5)
	}
	if got := available(t, pool, "sku-limited"); got != 0 {
		t.Errorf("sku-limited available %d, want 0", got)
	}
	if n := count(t, pool, `SELECT count(*) FROM outbox`); n != 5 {
		t.Errorf("outbox rows %d, want 5", n)
	}
}

func TestCommandHandlerReservesFromKafkaRecord(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	h := store.CommandHandler(testinfra.Logger())
	orderID := uuid.NewString()

	payload, err := proto.Marshal(&eventsv1.InventoryCommand{
		MessageId: uuid.NewString(),
		OrderId:   orderID,
		Command: &eventsv1.InventoryCommand_ReserveStock{ReserveStock: &eventsv1.ReserveStock{
			Items: []*commonv1.LineItem{{ProductId: "sku-cable", Quantity: 3}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := h(context.Background(), &kgo.Record{Key: []byte(orderID), Value: payload}); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if n := count(t, pool, `SELECT count(*) FROM reservations WHERE order_id = $1`, orderID); n != 1 {
		t.Errorf("reservations for order = %d, want 1", n)
	}

	// Undecodable and out-of-stock records are permanent failures, not retried.
	if err := h(context.Background(), &kgo.Record{Value: []byte("not protobuf")}); !kafka.IsPermanent(err) {
		t.Errorf("garbage payload error = %v, want permanent", err)
	}
	payload, _ = proto.Marshal(&eventsv1.InventoryCommand{
		OrderId: uuid.NewString(),
		Command: &eventsv1.InventoryCommand_ReserveStock{ReserveStock: &eventsv1.ReserveStock{
			Items: []*commonv1.LineItem{{ProductId: "sku-soldout", Quantity: 1}},
		}},
	})
	if err := h(context.Background(), &kgo.Record{Value: payload}); !kafka.IsPermanent(err) {
		t.Errorf("sold-out error = %v, want permanent", err)
	}
}

func TestListAndGetProducts(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)

	products, err := store.ListProducts(context.Background())
	if err != nil || len(products) != 7 {
		t.Fatalf("ListProducts = %d products, %v; want the 7 seeded", len(products), err)
	}
	p, err := store.GetProduct(context.Background(), "sku-monitor")
	if err != nil || p.UnitPriceCents != 24999 {
		t.Errorf("GetProduct(sku-monitor) = %+v, %v", p, err)
	}
	if _, err := store.GetProduct(context.Background(), "sku-missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetProduct(missing) error = %v, want ErrNotFound", err)
	}
}
