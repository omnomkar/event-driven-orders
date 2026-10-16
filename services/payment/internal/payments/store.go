// Package payments holds the payment service's state: tokenized cards and
// simulated charges.
package payments

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/outbox"
	eventsv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/events/v1"
	"github.com/omnomkar/event-driven-orders/services/payment/internal/card"
)

var (
	// ErrUnknownToken means a charge named a token that was never issued.
	ErrUnknownToken = errors.New("unknown payment token")
	// ErrInvalidCharge means the amount or currency is not usable.
	ErrInvalidCharge = errors.New("invalid charge")
)

// Card is a tokenized card: everything the service keeps about it.
type Card struct {
	Token string
	Last4 string
	Brand string
}

// Payment is a recorded charge.
type Payment struct {
	ID          string
	OrderID     string
	AmountCents int64
	Currency    string
	Status      string
}

// Store reads and writes the payment service's own database.
type Store struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

// NewStore returns a Store backed by pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, now: time.Now}
}

// Tokenize validates the card and stores a new token for it. The card
// number is used only in memory for validation and is never stored or
// logged; the database keeps only the token, last four digits, and brand.
//
// This is a stub for envelope encryption with a key management service,
// which would also keep the encrypted number for later charges.
func (s *Store) Tokenize(ctx context.Context, number string, expMonth, expYear int32) (*Card, error) {
	d, err := card.Validate(number, expMonth, expYear, s.now())
	if err != nil {
		return nil, err
	}
	token, err := card.NewToken()
	if err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO cards (token, last4, brand, always_decline) VALUES ($1, $2, $3, $4)`,
		token, d.Last4, d.Brand, d.AlwaysDecline); err != nil {
		return nil, fmt.Errorf("store card token: %w", err)
	}
	return &Card{Token: token, Last4: d.Last4, Brand: d.Brand}, nil
}

// Charge simulates charging token for an order. The payment row and the
// PaymentSucceeded event are written in one transaction.
func (s *Store) Charge(ctx context.Context, orderID, token string, amountCents int64, currency string) (*Payment, error) {
	if amountCents <= 0 || len(currency) != 3 {
		return nil, fmt.Errorf("%w: amount %d %q", ErrInvalidCharge, amountCents, currency)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate payment id: %w", err)
	}
	p := &Payment{ID: id.String(), OrderID: orderID, AmountCents: amountCents, Currency: currency, Status: "SUCCEEDED"}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var alwaysDecline bool
		err := tx.QueryRow(ctx, `SELECT always_decline FROM cards WHERE token = $1`, token).Scan(&alwaysDecline)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrUnknownToken
		}
		if err != nil {
			return fmt.Errorf("look up card: %w", err)
		}
		// TODO(stage 3): when alwaysDecline is set, record a FAILED payment
		// and emit PaymentFailed instead. Every charge succeeds for now.
		_ = alwaysDecline

		if _, err := tx.Exec(ctx, `
			INSERT INTO payments (id, order_id, token, amount_cents, currency, status)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			p.ID, orderID, token, amountCents, currency, p.Status); err != nil {
			// TODO(stage 3): a redelivered ChargePayment hits the unique
			// order_id; consumer dedup by message_id will skip it first.
			return fmt.Errorf("insert payment: %w", err)
		}
		return outbox.Add(ctx, tx, kafka.TopicPaymentEvents, orderID, &eventsv1.PaymentEvent{
			MessageId: uuid.NewString(),
			OrderId:   orderID,
			CreatedAt: timestamppb.Now(),
			Event: &eventsv1.PaymentEvent_PaymentSucceeded{
				PaymentSucceeded: &eventsv1.PaymentSucceeded{PaymentId: p.ID},
			},
		})
	})
	if err != nil {
		return nil, fmt.Errorf("charge order %s: %w", orderID, err)
	}
	return p, nil
}
