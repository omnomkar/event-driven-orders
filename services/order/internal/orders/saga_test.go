package orders

import (
	"errors"
	"slices"
	"testing"
)

func TestNext(t *testing.T) {
	tests := []struct {
		name         string
		from         Status
		event        Event
		want         Status
		wantCommands []Command
		wantErr      bool
	}{
		{"reserved charges payment", StatusPending, EventStockReserved, StatusInventoryReserved, []Command{CommandChargePayment}, false},
		{"rejected cancels without compensation", StatusPending, EventStockRejected, StatusCancelled, []Command{CommandPublishCancelled}, false},
		{"payment success confirms", StatusInventoryReserved, EventPaymentSucceeded, StatusConfirmed, []Command{CommandPublishConfirmed}, false},
		{"payment failure releases stock", StatusInventoryReserved, EventPaymentFailed, StatusCancelling, []Command{CommandReleaseStock}, false},
		{"release confirmed cancels", StatusCancelling, EventStockReleased, StatusCancelled, []Command{CommandPublishCancelled}, false},

		// Events that arrive in the wrong state are rejected and change nothing.
		{"payment before reservation", StatusPending, EventPaymentSucceeded, StatusPending, nil, true},
		{"duplicate StockReserved", StatusInventoryReserved, EventStockReserved, StatusInventoryReserved, nil, true},
		{"confirmed is final", StatusConfirmed, EventPaymentFailed, StatusConfirmed, nil, true},
		{"cancelled is final", StatusCancelled, EventStockReserved, StatusCancelled, nil, true},
		{"duplicate StockReleased after cancel", StatusCancelled, EventStockReleased, StatusCancelled, nil, true},
		{"payment success while cancelling", StatusCancelling, EventPaymentSucceeded, StatusCancelling, nil, true},
		{"release without a failed payment", StatusInventoryReserved, EventStockReleased, StatusInventoryReserved, nil, true},
		{"unknown status", Status("SHIPPED"), EventStockReserved, Status("SHIPPED"), nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, cmds, err := next(tt.from, tt.event)
			if tt.wantErr != (err != nil) {
				t.Fatalf("next(%s, %s) error = %v, wantErr %v", tt.from, tt.event, err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalidTransition) {
				t.Errorf("error %v does not wrap ErrInvalidTransition", err)
			}
			if got != tt.want {
				t.Errorf("next(%s, %s) status = %s, want %s", tt.from, tt.event, got, tt.want)
			}
			if !slices.Equal(cmds, tt.wantCommands) {
				t.Errorf("next(%s, %s) commands = %v, want %v", tt.from, tt.event, cmds, tt.wantCommands)
			}
		})
	}
}

// The happy path, end to end through the state machine.
func TestNextHappyPath(t *testing.T) {
	s := StatusPending
	for _, e := range []Event{EventStockReserved, EventPaymentSucceeded} {
		var err error
		if s, _, err = next(s, e); err != nil {
			t.Fatalf("%s: %v", e, err)
		}
	}
	if s != StatusConfirmed {
		t.Errorf("final status %s, want %s", s, StatusConfirmed)
	}
}

// The compensation path: a failed payment releases stock before cancelling.
func TestNextCompensationPath(t *testing.T) {
	s := StatusPending
	var sent []Command
	for _, e := range []Event{EventStockReserved, EventPaymentFailed, EventStockReleased} {
		var (
			cmds []Command
			err  error
		)
		if s, cmds, err = next(s, e); err != nil {
			t.Fatalf("%s: %v", e, err)
		}
		sent = append(sent, cmds...)
	}
	want := []Command{CommandChargePayment, CommandReleaseStock, CommandPublishCancelled}
	if s != StatusCancelled || !slices.Equal(sent, want) {
		t.Errorf("final status %s, commands %v; want CANCELLED after %v", s, sent, want)
	}
}
