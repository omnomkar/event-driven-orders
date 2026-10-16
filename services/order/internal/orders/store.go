// Package orders holds the order service's state: the saga state machine,
// the Postgres store, and the gRPC API.
package orders

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/outbox"
	commonv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/common/v1"
	eventsv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/events/v1"
)

const (
	maxItems    = 50
	maxQuantity = 1000
)

var (
	// ErrNotFound means no order has the requested ID.
	ErrNotFound = errors.New("order not found")
	// ErrInvalid means the request failed validation. The wrapped message
	// says why and is safe to show to the client.
	ErrInvalid = errors.New("invalid order")
	// ErrIdempotencyConflict means the customer already used the idempotency
	// key for a request with different items.
	ErrIdempotencyConflict = errors.New("idempotency key was already used with a different request")
)

// idempotencyConstraint is the unique constraint on (customer_id,
// idempotency_key), created by migration 00002.
const idempotencyConstraint = "orders_customer_idempotency_key"

// Item is one product and quantity in an order.
type Item struct {
	ProductID string
	Quantity  int32
}

// Order is an order as stored in the database.
type Order struct {
	ID           string
	CustomerID   string
	Status       Status
	Items        []Item
	PaymentToken string
	TotalCents   int64
	Currency     string
	CancelReason string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// CreateParams is a validated-on-create request for a new order.
type CreateParams struct {
	CustomerID     string
	Items          []Item
	PaymentToken   string
	IdempotencyKey string
}

// Store reads and writes orders in the order service's own database.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore returns a Store backed by pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (p CreateParams) validate() error {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
	}
	if strings.TrimSpace(p.CustomerID) == "" {
		return invalid("customer_id is required")
	}
	if !strings.HasPrefix(p.PaymentToken, "tok_") {
		return invalid("payment_token must be a token from TokenizeCard")
	}
	if len(p.Items) == 0 || len(p.Items) > maxItems {
		return invalid("an order needs between 1 and %d items", maxItems)
	}
	seen := make(map[string]bool, len(p.Items))
	for i, it := range p.Items {
		if strings.TrimSpace(it.ProductID) == "" {
			return invalid("items[%d].product_id is required", i)
		}
		if it.Quantity < 1 || it.Quantity > maxQuantity {
			return invalid("items[%d].quantity must be between 1 and %d", i, maxQuantity)
		}
		if seen[it.ProductID] {
			return invalid("product %q appears more than once; combine the quantities", it.ProductID)
		}
		seen[it.ProductID] = true
	}
	return nil
}

// Create stores a new PENDING order and starts the saga. The order, its
// items, its first history row, the ReserveStock command, and the
// OrderCreated event are all written in one transaction: either all of them
// are stored or none are.
//
// If p has an idempotency key that the customer already used, nothing new is
// created. With the same items, Create returns the original order and
// replayed is true; with different items it returns ErrIdempotencyConflict.
// Concurrent requests with the same key are decided by the unique
// constraint: one insert wins, and the others wait for it, fail, and read
// the winner's order.
func (s *Store) Create(ctx context.Context, p CreateParams) (o *Order, replayed bool, err error) {
	if err := p.validate(); err != nil {
		return nil, false, err
	}
	var hash *string
	if p.IdempotencyKey != "" {
		h := requestHash(p.Items)
		hash = &h
	}
	o, err = s.insert(ctx, p, hash)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation && pgErr.ConstraintName == idempotencyConstraint {
		return s.replay(ctx, p.CustomerID, p.IdempotencyKey, *hash)
	}
	if err != nil {
		return nil, false, fmt.Errorf("create order: %w", err)
	}
	return o, false, nil
}

// pgUniqueViolation is the Postgres error code for a unique constraint
// violation.
const pgUniqueViolation = "23505"

// replay returns the order the customer created earlier with key, if it was
// created from the same items.
func (s *Store) replay(ctx context.Context, customerID, key, hash string) (*Order, bool, error) {
	var (
		id         string
		storedHash *string
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id, request_hash FROM orders WHERE customer_id = $1 AND idempotency_key = $2`,
		customerID, key).Scan(&id, &storedHash)
	if err != nil {
		return nil, false, fmt.Errorf("look up order for idempotency key: %w", err)
	}
	if storedHash == nil || *storedHash != hash {
		return nil, false, ErrIdempotencyConflict
	}
	o, err := s.Get(ctx, id)
	if err != nil {
		return nil, false, err
	}
	return o, true, nil
}

func (s *Store) insert(ctx context.Context, p CreateParams, hash *string) (*Order, error) {
	// UUIDv7 IDs start with a timestamp, so new rows land at the end of the
	// primary key index instead of at random places in it.
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate order id: %w", err)
	}
	o := &Order{
		ID:           id.String(),
		CustomerID:   p.CustomerID,
		Status:       StatusPending,
		Items:        p.Items,
		PaymentToken: p.PaymentToken,
	}
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			INSERT INTO orders (id, customer_id, status, payment_token, idempotency_key, request_hash)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING created_at, updated_at`,
			o.ID, o.CustomerID, o.Status, o.PaymentToken, nullIfEmpty(p.IdempotencyKey), hash,
		).Scan(&o.CreatedAt, &o.UpdatedAt)
		if err != nil {
			return fmt.Errorf("insert order: %w", err)
		}
		productIDs := make([]string, len(o.Items))
		quantities := make([]int32, len(o.Items))
		for i, it := range o.Items {
			productIDs[i], quantities[i] = it.ProductID, it.Quantity
		}
		// One statement for every item: unnest turns the two arrays into
		// rows, and WITH ORDINALITY numbers them to keep the client's order.
		if _, err := tx.Exec(ctx, `
			INSERT INTO order_items (order_id, position, product_id, quantity)
			SELECT $1, t.position, t.product_id, t.quantity
			FROM unnest($2::text[], $3::int[]) WITH ORDINALITY AS t(product_id, quantity, position)`,
			o.ID, productIDs, quantities); err != nil {
			return fmt.Errorf("insert order items: %w", err)
		}

		if err := recordStatus(ctx, tx, o.ID, o.Status); err != nil {
			return err
		}

		lines := make([]*commonv1.LineItem, len(o.Items))
		for i, it := range o.Items {
			lines[i] = &commonv1.LineItem{ProductId: it.ProductID, Quantity: it.Quantity}
		}
		if err := outbox.Add(ctx, tx, kafka.TopicInventoryCommands, o.ID, &eventsv1.InventoryCommand{
			MessageId: uuid.NewString(),
			OrderId:   o.ID,
			CreatedAt: timestamppb.Now(),
			Command: &eventsv1.InventoryCommand_ReserveStock{
				ReserveStock: &eventsv1.ReserveStock{Items: lines},
			},
		}); err != nil {
			return err
		}
		return outbox.Add(ctx, tx, kafka.TopicOrderEvents, o.ID, &eventsv1.OrderEvent{
			MessageId: uuid.NewString(),
			OrderId:   o.ID,
			CreatedAt: timestamppb.Now(),
			Event: &eventsv1.OrderEvent_OrderCreated{
				OrderCreated: &eventsv1.OrderCreated{CustomerId: o.CustomerID, PaymentToken: o.PaymentToken},
			},
		})
	})
	if err != nil {
		return nil, err
	}
	return o, nil
}

// requestHash fingerprints an order's items for idempotency checks. Items are
// sorted first, so listing the same items in another order is the same
// request. Card details are deliberately left out: the payment token changes
// on every request, and no value derived from a card number is stored
// outside the payment service.
func requestHash(items []Item) string {
	sorted := slices.Clone(items)
	slices.SortFunc(sorted, func(a, b Item) int { return strings.Compare(a.ProductID, b.ProductID) })
	h := sha256.New()
	for _, it := range sorted {
		// Length-prefixing the ID keeps the encoding unambiguous. Writes to
		// a hash never fail.
		_, _ = fmt.Fprintf(h, "%d:%s:%d;", len(it.ProductID), it.ProductID, it.Quantity)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Get returns the order with its items, or ErrNotFound.
func (s *Store) Get(ctx context.Context, id string) (*Order, error) {
	if _, err := uuid.Parse(id); err != nil {
		return nil, fmt.Errorf("%w: order_id must be a UUID", ErrInvalid)
	}
	o := &Order{ID: id}
	var (
		total                  *int64
		currency, cancelReason *string
	)
	err := s.pool.QueryRow(ctx, `
		SELECT customer_id, status, payment_token, total_cents, currency, cancel_reason, created_at, updated_at
		FROM orders WHERE id = $1`, id,
	).Scan(&o.CustomerID, &o.Status, &o.PaymentToken, &total, &currency, &cancelReason, &o.CreatedAt, &o.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select order: %w", err)
	}
	if total != nil {
		o.TotalCents = *total
	}
	if currency != nil {
		o.Currency = *currency
	}
	if cancelReason != nil {
		o.CancelReason = *cancelReason
	}

	rows, err := s.pool.Query(ctx, `
		SELECT product_id, quantity FROM order_items WHERE order_id = $1 ORDER BY position`, id)
	if err != nil {
		return nil, fmt.Errorf("select order items: %w", err)
	}
	o.Items, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (Item, error) {
		var it Item
		err := row.Scan(&it.ProductID, &it.Quantity)
		return it, err
	})
	if err != nil {
		return nil, fmt.Errorf("read order items: %w", err)
	}
	return o, nil
}

// recordStatus appends a row to the order's status history.
func recordStatus(ctx context.Context, tx pgx.Tx, orderID string, s Status) error {
	if _, err := tx.Exec(ctx,
		`INSERT INTO order_status_history (order_id, status) VALUES ($1, $2)`, orderID, s); err != nil {
		return fmt.Errorf("insert status history: %w", err)
	}
	return nil
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
