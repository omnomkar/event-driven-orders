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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/omnomkar/event-driven-orders/pkg/dedup"
	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/outbox"
	eventsv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/events/v1"
	"github.com/omnomkar/event-driven-orders/services/payment/internal/card"
)

var (
	// ErrAlreadyCharged means the order already has a payment.
	ErrAlreadyCharged = errors.New("order already has a payment")
)

// pgUniqueViolation is the Postgres error code for a unique constraint
// violation.
const pgUniqueViolation = "23505"

// Card is a tokenized card: everything the service keeps about it.
type Card struct {
	Token string
	Last4 string
	Brand string
}

// Payment statuses, as stored in payments.status.
const (
	StatusSucceeded = "SUCCEEDED"
	StatusFailed    = "FAILED"
)

// Payment is the outcome of a charge.
type Payment struct {
	// ID is empty when the charge failed before a payment could be recorded.
	ID            string
	OrderID       string
	AmountCents   int64
	Currency      string
	Status        string
	FailureReason string
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

// Reasons a charge fails. They become the order's cancel reason.
const (
	ReasonDeclined      = "card declined"
	ReasonUnknownToken  = "unknown payment token"
	ReasonInvalidAmount = "invalid charge amount"
)

// Charge handles the ChargePayment command with ID messageID: it simulates
// charging token for an order. In one transaction it records the message as
// processed, stores the payment, and writes the reply to the outbox:
//
//   - a normal card: a SUCCEEDED payment and PaymentSucceeded;
//   - the decline test card: a FAILED payment and PaymentFailed;
//   - an unknown token or an unusable amount: no payment row (there is
//     nothing valid to record) but still PaymentFailed, so the saga releases
//     the stock instead of waiting forever.
//
// A redelivered command returns an error wrapping dedup.ErrDuplicate. The
// unique payments.order_id is a second guard: a different ChargePayment for
// an order that already has a payment returns ErrAlreadyCharged.
func (s *Store) Charge(ctx context.Context, messageID, orderID, token string, amountCents int64, currency string) (*Payment, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate payment id: %w", err)
	}
	p := &Payment{OrderID: orderID, AmountCents: amountCents, Currency: currency}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := dedup.Claim(ctx, tx, messageID); err != nil {
			return err
		}
		p.Status, p.FailureReason = StatusFailed, ""
		switch alwaysDecline, err := s.lookupCard(ctx, tx, token); {
		case amountCents <= 0 || len(currency) != 3:
			p.FailureReason = ReasonInvalidAmount
		case errors.Is(err, pgx.ErrNoRows):
			p.FailureReason = ReasonUnknownToken
		case err != nil:
			return err
		case alwaysDecline:
			p.ID, p.FailureReason = id.String(), ReasonDeclined
		default:
			p.ID, p.Status = id.String(), StatusSucceeded
		}

		if p.ID != "" {
			if _, err := tx.Exec(ctx, `
				INSERT INTO payments (id, order_id, token, amount_cents, currency, status)
				VALUES ($1, $2, $3, $4, $5, $6)`,
				p.ID, orderID, token, amountCents, currency, p.Status); err != nil {
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
					return fmt.Errorf("%w: order %s", ErrAlreadyCharged, orderID)
				}
				return fmt.Errorf("insert payment: %w", err)
			}
		}

		ev := &eventsv1.PaymentEvent{MessageId: uuid.NewString(), OrderId: orderID, CreatedAt: timestamppb.Now()}
		if p.Status == StatusSucceeded {
			ev.Event = &eventsv1.PaymentEvent_PaymentSucceeded{PaymentSucceeded: &eventsv1.PaymentSucceeded{PaymentId: p.ID}}
		} else {
			ev.Event = &eventsv1.PaymentEvent_PaymentFailed{PaymentFailed: &eventsv1.PaymentFailed{Reason: p.FailureReason}}
		}
		return outbox.Add(ctx, tx, kafka.TopicPaymentEvents, orderID, ev)
	})
	if err != nil {
		return nil, fmt.Errorf("charge order %s: %w", orderID, err)
	}
	return p, nil
}

// lookupCard reports whether token's card always declines. It returns
// pgx.ErrNoRows for a token that was never issued.
func (s *Store) lookupCard(ctx context.Context, tx pgx.Tx, token string) (bool, error) {
	var alwaysDecline bool
	err := tx.QueryRow(ctx, `SELECT always_decline FROM cards WHERE token = $1`, token).Scan(&alwaysDecline)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("look up card: %w", err)
	}
	return alwaysDecline, err
}
