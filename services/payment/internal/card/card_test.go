package card

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// Standard test card numbers only.
const (
	visa       = "4242424242424242"
	mastercard = "5555555555554444"
	amex       = "378282246310005"
	discover   = "6011111111111117"
)

var now = time.Date(2026, time.October, 16, 12, 0, 0, 0, time.UTC)

func TestValidate(t *testing.T) {
	tests := []struct {
		name      string
		number    string
		month     int32
		year      int32
		wantErr   error
		wantBrand string
		wantLast4 string
	}{
		{"visa", visa, 12, 2030, nil, "visa", "4242"},
		{"visa with spaces", "4242 4242 4242 4242", 12, 2030, nil, "visa", "4242"},
		{"visa with dashes", "4242-4242-4242-4242", 12, 2030, nil, "visa", "4242"},
		{"mastercard", mastercard, 1, 2027, nil, "mastercard", "4444"},
		{"amex", amex, 6, 2028, nil, "amex", "0005"},
		{"discover", discover, 6, 2028, nil, "discover", "1117"},
		{"decline test card is still valid", declineTestNumber, 12, 2030, nil, "visa", "0002"},
		{"valid through end of expiry month", visa, 10, 2026, nil, "visa", "4242"},

		{"luhn failure", "4242424242424241", 12, 2030, ErrInvalidNumber, "", ""},
		{"letters", "4242abcd42424242", 12, 2030, ErrInvalidNumber, "", ""},
		{"too short", "42424242", 12, 2030, ErrInvalidNumber, "", ""},
		{"empty", "", 12, 2030, ErrInvalidNumber, "", ""},
		{"month zero", visa, 0, 2030, ErrInvalidExpiry, "", ""},
		{"month thirteen", visa, 13, 2030, ErrInvalidExpiry, "", ""},
		{"two-digit year", visa, 12, 30, ErrInvalidExpiry, "", ""},
		{"expired last month", visa, 9, 2026, ErrExpired, "", ""},
		{"expired last year", visa, 12, 2025, ErrExpired, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := Validate(tt.number, tt.month, tt.year, now)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate error = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				// An error must never echo the card number.
				if tt.number != "" && strings.Contains(err.Error(), normalize(tt.number)) {
					t.Errorf("error %q contains the card number", err)
				}
				return
			}
			if d.Brand != tt.wantBrand || d.Last4 != tt.wantLast4 {
				t.Errorf("Validate = %+v, want brand %s last4 %s", d, tt.wantBrand, tt.wantLast4)
			}
			if d.AlwaysDecline != (normalize(tt.number) == declineTestNumber) {
				t.Errorf("AlwaysDecline = %v for %s", d.AlwaysDecline, tt.name)
			}
		})
	}
}

func TestNewToken(t *testing.T) {
	a, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewToken()
	if !strings.HasPrefix(a, "tok_") || len(a) != len("tok_")+32 {
		t.Errorf("token %q, want tok_ plus 32 hex characters", a)
	}
	if a == b {
		t.Error("two tokens are equal")
	}
}
