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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/omnomkar/event-driven-orders/pkg/dedup"
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
	// ErrAlreadyReserved means the order already has a reservation.
	ErrAlreadyReserved = errors.New("order already has a reservation")
)

// pgUniqueViolation is the Postgres error code for a unique constraint
// violation.
const pgUniqueViolation = "23505"

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

// Rejection explains why an order could not be reserved. Its reason ends up
// as the order's cancel reason, so it names the product but never the stock
// level.
type Rejection struct {
	ProductID string
	Reason    string
	Err       error // one of the rejection sentinels above
}

// Reserve handles the ReserveStock command with ID messageID. In one
// transaction it records the message as processed and then either reserves
// every line for orderID or none of them:
//
//   - reserved: stock is decremented, the reservation is stored, and
//     StockReserved (with unit prices and the total) goes to the outbox;
//   - rejected: nothing changes except that StockRejected goes to the
//     outbox, and the returned Rejection says why.
//
// Exactly one of the two results is non-nil when err is nil. A redelivered
// command returns an error wrapping dedup.ErrDuplicate.
//
// The product rows are locked with SELECT ... FOR UPDATE in product ID order.
// Two orders that share products therefore always take their locks in the
// same order, so neither can hold a lock the other is waiting for, and they
// can never deadlock. The lock also means two orders cannot both read the
// same "available" value and oversell it.
func (s *Store) Reserve(ctx context.Context, messageID, orderID string, lines []Line) (*Reservation, *Rejection, error) {
	var (
		res *Reservation
		rej *Rejection
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := dedup.Claim(ctx, tx, messageID); err != nil {
			return err
		}
		ids := make([]string, len(lines))
		for i, l := range lines {
			ids[i] = l.ProductID
		}
		slices.Sort(ids)
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
		var r *Reservation
		if r, rej = price(orderID, lines, byID); rej != nil {
			return outbox.Add(ctx, tx, kafka.TopicInventoryEvents, orderID, &eventsv1.InventoryEvent{
				MessageId: uuid.NewString(),
				OrderId:   orderID,
				CreatedAt: timestamppb.Now(),
				Event: &eventsv1.InventoryEvent_StockRejected{
					StockRejected: &eventsv1.StockRejected{Reason: rej.Reason, ProductId: rej.ProductID},
				},
			})
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO reservations (order_id, status, total_cents, currency)
			VALUES ($1, 'RESERVED', $2, $3)`, orderID, r.TotalCents, r.Currency); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
				// A second ReserveStock for the same order under a different
				// message_id. Redeliveries never get here (dedup skips them),
				// so this is a bug upstream, not something to retry.
				return fmt.Errorf("%w: order %s", ErrAlreadyReserved, orderID)
			}
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
		return nil, nil, fmt.Errorf("reserve stock for order %s: %w", orderID, err)
	}
	return res, rej, nil
}

// price checks every line against the locked products and prices the order.
// It returns a Rejection for the first line that cannot be reserved.
func price(orderID string, lines []Line, byID map[string]Product) (*Reservation, *Rejection) {
	reject := func(productID string, sentinel error) *Rejection {
		reason := sentinel.Error()
		if productID != "" {
			reason += ": " + productID
		}
		return &Rejection{ProductID: productID, Reason: reason, Err: sentinel}
	}
	if len(lines) == 0 {
		return nil, reject("", ErrInvalidLine)
	}
	r := &Reservation{OrderID: orderID}
	for _, l := range lines {
		if strings.TrimSpace(l.ProductID) == "" || l.Quantity <= 0 {
			return nil, reject(l.ProductID, ErrInvalidLine)
		}
		p, ok := byID[l.ProductID]
		switch {
		case !ok:
			return nil, reject(l.ProductID, ErrUnknownProduct)
		case p.Available < l.Quantity:
			return nil, reject(l.ProductID, ErrInsufficientStock)
		case r.Currency != "" && p.Currency != r.Currency:
			return nil, reject(l.ProductID, ErrMixedCurrency)
		}
		r.Currency = p.Currency
		r.Lines = append(r.Lines, PricedLine{Line: l, UnitPriceCents: p.UnitPriceCents})
		r.TotalCents += p.UnitPriceCents * int64(l.Quantity)
	}
	return r, nil
}

// ReleaseResult says what Release found.
type ReleaseResult string

const (
	Released        ReleaseResult = "released"
	AlreadyReleased ReleaseResult = "already_released"
	NoReservation   ReleaseResult = "no_reservation"
)

// Release handles the ReleaseStock command with ID messageID: it gives the
// order's reserved stock back and marks the reservation RELEASED, in one
// transaction with the dedup record and a StockReleased reply.
//
// Release is idempotent. If the reservation is already released, or was
// never made, nothing changes but StockReleased is still sent, so the saga
// can always finish cancelling the order.
func (s *Store) Release(ctx context.Context, messageID, orderID string) (ReleaseResult, error) {
	var result ReleaseResult
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := dedup.Claim(ctx, tx, messageID); err != nil {
			return err
		}
		var status string
		err := tx.QueryRow(ctx, `SELECT status FROM reservations WHERE order_id = $1 FOR UPDATE`, orderID).Scan(&status)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			result = NoReservation
		case err != nil:
			return fmt.Errorf("lock reservation: %w", err)
		case status == "RELEASED":
			result = AlreadyReleased
		default:
			result = Released
			// Lock the products in ID order, the same order Reserve uses, so
			// a release and a reservation cannot deadlock.
			if _, err := tx.Exec(ctx, `
				SELECT 1 FROM products
				WHERE id IN (SELECT product_id FROM reservation_lines WHERE order_id = $1)
				ORDER BY id
				FOR UPDATE`, orderID); err != nil {
				return fmt.Errorf("lock products: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE products p
				SET available = p.available + rl.quantity, updated_at = now()
				FROM reservation_lines rl
				WHERE rl.order_id = $1 AND p.id = rl.product_id`, orderID); err != nil {
				return fmt.Errorf("restore stock: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE reservations SET status = 'RELEASED', updated_at = now() WHERE order_id = $1`, orderID); err != nil {
				return fmt.Errorf("mark reservation released: %w", err)
			}
		}
		return outbox.Add(ctx, tx, kafka.TopicInventoryEvents, orderID, &eventsv1.InventoryEvent{
			MessageId: uuid.NewString(),
			OrderId:   orderID,
			CreatedAt: timestamppb.Now(),
			Event:     &eventsv1.InventoryEvent_StockReleased{StockReleased: &eventsv1.StockReleased{}},
		})
	})
	if err != nil {
		return "", fmt.Errorf("release stock for order %s: %w", orderID, err)
	}
	return result, nil
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
