// Package stock holds the inventory service's state: products, prices, and
// per-order reservations.
package stock

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/outbox"
	eventsv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/events/v1"
)

var (
	// ErrNotFound means no product has the requested ID.
	ErrNotFound = errors.New("product not found")
	// ErrUnknownProduct means a reservation named a product that does not exist.
	ErrUnknownProduct = errors.New("unknown product")
	// ErrInsufficientStock means a reservation asked for more than is available.
	ErrInsufficientStock = errors.New("insufficient stock")
	// ErrMixedCurrency means one order's products are priced in different currencies.
	ErrMixedCurrency = errors.New("products in one order must share a currency")
	// ErrInvalidLine means a reservation line has no product or a non-positive quantity.
	ErrInvalidLine = errors.New("invalid reservation line")
)

// Product is one catalog entry with its current stock.
type Product struct {
	ID             string
	Name           string
	UnitPriceCents int64
	Currency       string
	Available      int32
}

// Line is one product and quantity to reserve.
type Line struct {
	ProductID string
	Quantity  int32
}

// Reservation is the result of a successful Reserve.
type Reservation struct {
	OrderID    string
	Lines      []PricedLine
	TotalCents int64
	Currency   string
}

// PricedLine is a reserved line with the unit price charged for it.
type PricedLine struct {
	Line
	UnitPriceCents int64
}

// Store reads and writes the inventory service's own database.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore returns a Store backed by pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Reserve reserves every line for orderID, or none of them, in one
// transaction. On success it also writes StockReserved (with unit prices and
// the total) to the outbox in that same transaction.
//
// The product rows are locked with SELECT ... FOR UPDATE in product ID order.
// Two orders that share products therefore always take their locks in the
// same order, so neither can hold a lock the other is waiting for, and they
// can never deadlock. The lock also means two orders cannot both read the
// same "available" value and oversell it.
func (s *Store) Reserve(ctx context.Context, orderID string, lines []Line) (*Reservation, error) {
	if len(lines) == 0 {
		return nil, fmt.Errorf("%w: no lines", ErrInvalidLine)
	}
	ids := make([]string, len(lines))
	for i, l := range lines {
		if strings.TrimSpace(l.ProductID) == "" || l.Quantity <= 0 {
			return nil, fmt.Errorf("%w: line %d", ErrInvalidLine, i)
		}
		ids[i] = l.ProductID
	}
	slices.Sort(ids)

	var res *Reservation
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, name, unit_price_cents, currency, available
			FROM products
			WHERE id = ANY($1)
			ORDER BY id
			FOR UPDATE`, ids)
		if err != nil {
			return fmt.Errorf("lock products: %w", err)
		}
		products, err := pgx.CollectRows(rows, scanProduct)
		if err != nil {
			return fmt.Errorf("read products: %w", err)
		}
		byID := make(map[string]Product, len(products))
		for _, p := range products {
			byID[p.ID] = p
		}

		// Check every line before changing anything: all or nothing.
		// TODO(stage 3): instead of returning these errors, write a
		// StockRejected event to the outbox so the order is cancelled.
		r := &Reservation{OrderID: orderID}
		for _, l := range lines {
			p, ok := byID[l.ProductID]
			switch {
			case !ok:
				return fmt.Errorf("%w: %s", ErrUnknownProduct, l.ProductID)
			case p.Available < l.Quantity:
				return fmt.Errorf("%w: %s has %d, order wants %d", ErrInsufficientStock, p.ID, p.Available, l.Quantity)
			case r.Currency != "" && p.Currency != r.Currency:
				return fmt.Errorf("%w: %s is in %s", ErrMixedCurrency, p.ID, p.Currency)
			}
			r.Currency = p.Currency
			r.Lines = append(r.Lines, PricedLine{Line: l, UnitPriceCents: p.UnitPriceCents})
			r.TotalCents += p.UnitPriceCents * int64(l.Quantity)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO reservations (order_id, status, total_cents, currency)
			VALUES ($1, 'RESERVED', $2, $3)`, orderID, r.TotalCents, r.Currency); err != nil {
			// TODO(stage 3): a redelivered ReserveStock hits this primary key;
			// consumer dedup by message_id will skip it before it gets here.
			return fmt.Errorf("insert reservation: %w", err)
		}
		for _, l := range r.Lines {
			if _, err := tx.Exec(ctx, `
				UPDATE products SET available = available - $2, updated_at = now() WHERE id = $1`,
				l.ProductID, l.Quantity); err != nil {
				return fmt.Errorf("decrement %s: %w", l.ProductID, err)
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO reservation_lines (order_id, product_id, quantity, unit_price_cents)
				VALUES ($1, $2, $3, $4)`, orderID, l.ProductID, l.Quantity, l.UnitPriceCents); err != nil {
				return fmt.Errorf("insert reservation line: %w", err)
			}
		}

		priced := make([]*eventsv1.PricedLine, len(r.Lines))
		for i, l := range r.Lines {
			priced[i] = &eventsv1.PricedLine{ProductId: l.ProductID, Quantity: l.Quantity, UnitPriceCents: l.UnitPriceCents}
		}
		if err := outbox.Add(ctx, tx, kafka.TopicInventoryEvents, orderID, &eventsv1.InventoryEvent{
			MessageId: uuid.NewString(),
			OrderId:   orderID,
			CreatedAt: timestamppb.Now(),
			Event: &eventsv1.InventoryEvent_StockReserved{
				StockReserved: &eventsv1.StockReserved{Lines: priced, TotalCents: r.TotalCents, Currency: r.Currency},
			},
		}); err != nil {
			return err
		}
		res = r
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reserve stock for order %s: %w", orderID, err)
	}
	return res, nil
}

// GetProduct returns one product, or ErrNotFound.
func (s *Store) GetProduct(ctx context.Context, id string) (*Product, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, unit_price_cents, currency, available FROM products WHERE id = $1`, id)
	if err != nil {
		return nil, fmt.Errorf("select product: %w", err)
	}
	p, err := pgx.CollectExactlyOneRow(rows, scanProduct)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read product: %w", err)
	}
	return &p, nil
}

// ListProducts returns every product, ordered by ID.
func (s *Store) ListProducts(ctx context.Context) ([]Product, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, unit_price_cents, currency, available FROM products ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("select products: %w", err)
	}
	products, err := pgx.CollectRows(rows, scanProduct)
	if err != nil {
		return nil, fmt.Errorf("read products: %w", err)
	}
	return products, nil
}

func scanProduct(row pgx.CollectableRow) (Product, error) {
	var p Product
	err := row.Scan(&p.ID, &p.Name, &p.UnitPriceCents, &p.Currency, &p.Available)
	return p, err
}
