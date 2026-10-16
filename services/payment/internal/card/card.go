// Package card validates card details for the tokenization stub. Errors from
// this package never contain the card number, so they are safe to log and
// return to clients.
package card

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	// ErrInvalidNumber means the number is malformed or fails the Luhn check.
	ErrInvalidNumber = errors.New("card number is not valid")
	// ErrInvalidExpiry means the expiry month or year is out of range.
	ErrInvalidExpiry = errors.New("card expiry is not valid")
	// ErrExpired means the card's expiry month has passed.
	ErrExpired = errors.New("card has expired")
)

// declineTestNumber is the standard "generic decline" test card. It
// tokenizes normally but is marked so that charges against it fail.
const declineTestNumber = "4000000000000002"

// maxYearsAhead bounds how far in the future an expiry year can be.
const maxYearsAhead = 20

// Details is what may be kept about a card after validation.
type Details struct {
	Last4         string
	Brand         string
	AlwaysDecline bool
}

// Validate checks number (spaces and dashes allowed) with the Luhn
// algorithm and checks that the card has not expired as of now. A card is
// valid through the last day of its expiry month.
func Validate(number string, expMonth, expYear int32, now time.Time) (Details, error) {
	digits := normalize(number)
	if len(digits) < 12 || len(digits) > 19 || !onlyDigits(digits) || !luhn(digits) {
		return Details{}, ErrInvalidNumber
	}
	if expMonth < 1 || expMonth > 12 || int(expYear) < now.Year()-maxYearsAhead || int(expYear) > now.Year()+maxYearsAhead {
		return Details{}, ErrInvalidExpiry
	}
	if int(expYear) < now.Year() || (int(expYear) == now.Year() && time.Month(expMonth) < now.Month()) {
		return Details{}, ErrExpired
	}
	return Details{
		Last4:         digits[len(digits)-4:],
		Brand:         brand(digits),
		AlwaysDecline: digits == declineTestNumber,
	}, nil
}

// NewToken returns an opaque random token. It is derived from nothing about
// the card, so it reveals nothing about the card.
func NewToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	return "tok_" + hex.EncodeToString(b), nil
}

func normalize(number string) string {
	return strings.NewReplacer(" ", "", "-", "").Replace(number)
}

func onlyDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// luhn reports whether digits passes the Luhn checksum: starting from the
// rightmost digit, double every second digit (subtracting 9 if the result is
// over 9), and the sum of all digits must be divisible by 10.
func luhn(digits string) bool {
	sum := 0
	double := false
	for i := len(digits) - 1; i >= 0; i-- {
		d := int(digits[i] - '0')
		if double {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return sum%10 == 0
}

// brand identifies the card network from the number's leading digits.
func brand(digits string) string {
	prefix := func(n int) int {
		v := 0
		for i := 0; i < n && i < len(digits); i++ {
			v = v*10 + int(digits[i]-'0')
		}
		return v
	}
	switch p2, p4 := prefix(2), prefix(4); {
	case digits[0] == '4':
		return "visa"
	case (p2 >= 51 && p2 <= 55) || (p4 >= 2221 && p4 <= 2720):
		return "mastercard"
	case p2 == 34 || p2 == 37:
		return "amex"
	case p4 == 6011 || p2 == 65:
		return "discover"
	}
	return "unknown"
}
