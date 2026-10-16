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

	res, rej, err := store.Reserve(ctx, uuid.NewString(), orderID, []Line{
		{ProductID: "sku-mouse", Quantity: 2},
		{ProductID: "sku-keyboard", Quantity: 1},
	})
	if err != nil || rej != nil {
		t.Fatalf("Reserve: rejection %v, error %v", rej, err)
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

// All or nothing: one short line means no line is reserved. The only thing
// written is the StockRejected reply, which names the product.
func TestReserveShortLineRejectsAndChangesNothing(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()
	keyboards := available(t, pool, "sku-keyboard")

	for _, tt := range []struct {
		name    string
		lines   []Line
		want    error
		product string
	}{
		{"insufficient", []Line{{ProductID: "sku-keyboard", Quantity: 1}, {ProductID: "sku-limited", Quantity: 6}}, ErrInsufficientStock, "sku-limited"},
		{"unknown", []Line{{ProductID: "sku-keyboard", Quantity: 1}, {ProductID: "sku-missing", Quantity: 1}}, ErrUnknownProduct, "sku-missing"},
		{"bad quantity", []Line{{ProductID: "sku-keyboard", Quantity: 0}}, ErrInvalidLine, "sku-keyboard"},
	} {
		orderID := uuid.NewString()
		res, rej, err := store.Reserve(ctx, uuid.NewString(), orderID, tt.lines)
		if err != nil || res != nil || rej == nil || !errors.Is(rej.Err, tt.want) || rej.ProductID != tt.product {
			t.Errorf("%s: Reserve = %v, %+v, %v; want a %v rejection for %s", tt.name, res, rej, err, tt.want, tt.product)
			continue
		}
		var payload []byte
		if err := pool.QueryRow(ctx, `SELECT payload FROM outbox WHERE key = $1`, orderID).Scan(&payload); err != nil {
			t.Fatalf("%s: find StockRejected outbox row: %v", tt.name, err)
		}
		var ev eventsv1.InventoryEvent
		if err := proto.Unmarshal(payload, &ev); err != nil {
			t.Fatal(err)
		}
		if got := ev.GetStockRejected(); got.GetProductId() != tt.product || got.GetReason() != rej.Reason {
			t.Errorf("%s: StockRejected = %v, want product %s and reason %q", tt.name, got, tt.product, rej.Reason)
		}
	}
	if got := available(t, pool, "sku-keyboard"); got != keyboards {
		t.Errorf("keyboards available %d, want unchanged %d", got, keyboards)
	}
	if got := available(t, pool, "sku-limited"); got != 5 {
		t.Errorf("sku-limited available %d, want unchanged 5", got)
	}
	for _, table := range []string{"reservations", "reservation_lines"} {
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
			_, rej, err := store.Reserve(context.Background(), uuid.NewString(), uuid.NewString(), lines)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				t.Errorf("unexpected error (deadlock?): %v", err)
			case rej == nil:
				reserved++
			case errors.Is(rej.Err, ErrInsufficientStock):
				short++
			default:
				t.Errorf("unexpected rejection: %+v", rej)
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
	// One reply per order: StockReserved or StockRejected.
	if n := count(t, pool, `SELECT count(*) FROM outbox`); n != orders {
		t.Errorf("outbox rows %d, want %d", n, orders)
	}
}

// Release gives the stock back, marks the reservation RELEASED, and replies
// StockReleased. Releasing again, or releasing an order that was never
// reserved, changes nothing but still replies, so the saga can finish.
func TestReleaseRestoresStockAndIsIdempotent(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	ctx := context.Background()
	orderID := uuid.NewString()
	limited, keyboards := available(t, pool, "sku-limited"), available(t, pool, "sku-keyboard")

	if _, rej, err := store.Reserve(ctx, uuid.NewString(), orderID, []Line{
		{ProductID: "sku-limited", Quantity: 2}, {ProductID: "sku-keyboard", Quantity: 1},
	}); err != nil || rej != nil {
		t.Fatalf("Reserve: %v, %v", rej, err)
	}
	if got := available(t, pool, "sku-limited"); got != limited-2 {
		t.Fatalf("sku-limited after reserve %d, want %d", got, limited-2)
	}

	for i, want := range []ReleaseResult{Released, AlreadyReleased} {
		got, err := store.Release(ctx, uuid.NewString(), orderID)
		if err != nil || got != want {
			t.Errorf("release %d = %q, %v; want %q", i+1, got, err, want)
		}
	}
	if got := available(t, pool, "sku-limited"); got != limited {
		t.Errorf("sku-limited after release %d, want restored %d", got, limited)
	}
	if got := available(t, pool, "sku-keyboard"); got != keyboards {
		t.Errorf("sku-keyboard after release %d, want restored %d", got, keyboards)
	}
	if n := count(t, pool, `SELECT count(*) FROM reservations WHERE order_id = $1 AND status = 'RELEASED'`, orderID); n != 1 {
		t.Errorf("RELEASED reservations = %d, want 1", n)
	}
	if got, err := store.Release(ctx, uuid.NewString(), uuid.NewString()); err != nil || got != NoReservation {
		t.Errorf("release of unknown order = %q, %v; want %q", got, err, NoReservation)
	}
	// StockReserved plus two StockReleased for the order, and one for the
	// unknown order.
	if n := count(t, pool, `SELECT count(*) FROM outbox WHERE key = $1`, orderID); n != 3 {
		t.Errorf("outbox rows for the order = %d, want 3", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM outbox`); n != 4 {
		t.Errorf("outbox rows = %d, want 4", n)
	}
}

func command(t *testing.T, cmd *eventsv1.InventoryCommand) *kgo.Record {
	t.Helper()
	payload, err := proto.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: kafka.TopicInventoryCommands, Key: []byte(cmd.GetOrderId()), Value: payload}
}

func TestCommandHandlerReservesFromKafkaRecord(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	h := store.CommandHandler(testinfra.Logger())
	ctx := context.Background()
	orderID := uuid.NewString()

	if err := h(ctx, command(t, &eventsv1.InventoryCommand{
		MessageId: uuid.NewString(),
		OrderId:   orderID,
		Command: &eventsv1.InventoryCommand_ReserveStock{ReserveStock: &eventsv1.ReserveStock{
			Items: []*commonv1.LineItem{{ProductId: "sku-cable", Quantity: 3}},
		}},
	})); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if n := count(t, pool, `SELECT count(*) FROM reservations WHERE order_id = $1`, orderID); n != 1 {
		t.Errorf("reservations for order = %d, want 1", n)
	}

	// Out of stock is a normal outcome: the handler succeeds and the reply
	// is StockRejected.
	soldOut := uuid.NewString()
	if err := h(ctx, command(t, &eventsv1.InventoryCommand{
		MessageId: uuid.NewString(),
		OrderId:   soldOut,
		Command: &eventsv1.InventoryCommand_ReserveStock{ReserveStock: &eventsv1.ReserveStock{
			Items: []*commonv1.LineItem{{ProductId: "sku-soldout", Quantity: 1}},
		}},
	})); err != nil {
		t.Errorf("sold-out: handler error %v, want nil", err)
	}
	if n := count(t, pool, `SELECT count(*) FROM outbox WHERE key = $1`, soldOut); n != 1 {
		t.Errorf("sold-out: outbox rows = %d, want 1 StockRejected", n)
	}

	// Undecodable records and commands without a message_id are permanent
	// failures, not retried.
	if err := h(ctx, &kgo.Record{Value: []byte("not protobuf")}); !kafka.IsPermanent(err) {
		t.Errorf("garbage payload error = %v, want permanent", err)
	}
	noID := command(t, &eventsv1.InventoryCommand{
		OrderId: uuid.NewString(),
		Command: &eventsv1.InventoryCommand_ReleaseStock{ReleaseStock: &eventsv1.ReleaseStock{}},
	})
	if err := h(ctx, noID); !kafka.IsPermanent(err) {
		t.Errorf("missing message_id error = %v, want permanent", err)
	}
}

// A redelivered ReserveStock or ReleaseStock (same message_id) is skipped:
// stock moves once, and only one reply is written per command.
func TestCommandHandlerSkipsRedeliveredCommands(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	store := NewStore(pool)
	h := store.CommandHandler(testinfra.Logger())
	ctx := context.Background()
	orderID := uuid.NewString()
	cables := available(t, pool, "sku-cable")

	reserve := command(t, &eventsv1.InventoryCommand{
		MessageId: uuid.NewString(),
		OrderId:   orderID,
		Command: &eventsv1.InventoryCommand_ReserveStock{ReserveStock: &eventsv1.ReserveStock{
			Items: []*commonv1.LineItem{{ProductId: "sku-cable", Quantity: 3}},
		}},
	})
	for i := range 2 {
		if err := h(ctx, reserve); err != nil {
			t.Fatalf("ReserveStock delivery %d: %v", i+1, err)
		}
	}
	if got := available(t, pool, "sku-cable"); got != cables-3 {
		t.Errorf("cables after two deliveries %d, want %d (reserved once)", got, cables-3)
	}
	if n := count(t, pool, `SELECT count(*) FROM outbox WHERE key = $1`, orderID); n != 1 {
		t.Errorf("outbox rows after ReserveStock twice = %d, want 1", n)
	}

	release := command(t, &eventsv1.InventoryCommand{
		MessageId: uuid.NewString(),
		OrderId:   orderID,
		Command:   &eventsv1.InventoryCommand_ReleaseStock{ReleaseStock: &eventsv1.ReleaseStock{}},
	})
	for i := range 2 {
		if err := h(ctx, release); err != nil {
			t.Fatalf("ReleaseStock delivery %d: %v", i+1, err)
		}
	}
	if got := available(t, pool, "sku-cable"); got != cables {
		t.Errorf("cables after release %d, want %d", got, cables)
	}
	if n := count(t, pool, `SELECT count(*) FROM outbox WHERE key = $1`, orderID); n != 2 {
		t.Errorf("outbox rows after ReleaseStock twice = %d, want 2", n)
	}

	// A second ReserveStock under a new message_id cannot be a redelivery,
	// so it is a permanent failure rather than a second reservation.
	reserve2 := command(t, &eventsv1.InventoryCommand{
		MessageId: uuid.NewString(),
		OrderId:   orderID,
		Command: &eventsv1.InventoryCommand_ReserveStock{ReserveStock: &eventsv1.ReserveStock{
			Items: []*commonv1.LineItem{{ProductId: "sku-cable", Quantity: 1}},
		}},
	})
	if err := h(ctx, reserve2); !kafka.IsPermanent(err) || !errors.Is(err, ErrAlreadyReserved) {
		t.Errorf("second ReserveStock: error %v, want permanent ErrAlreadyReserved", err)
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
