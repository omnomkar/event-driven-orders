package orders

import (
	"errors"
	"fmt"
)

// Status is an order's position in the saga. The values are stored in the
// orders.status column.
type Status string

const (
	StatusPending           Status = "PENDING"
	StatusInventoryReserved Status = "INVENTORY_RESERVED"
	StatusConfirmed         Status = "CONFIRMED"
	StatusCancelled         Status = "CANCELLED"
)

// Event is a saga reply the order service reacts to.
type Event int

const (
	EventStockReserved Event = iota + 1
	EventStockRejected
	EventPaymentSucceeded
	EventPaymentFailed
)

func (e Event) String() string {
	switch e {
	case EventStockReserved:
		return "StockReserved"
	case EventStockRejected:
		return "StockRejected"
	case EventPaymentSucceeded:
		return "PaymentSucceeded"
	case EventPaymentFailed:
		return "PaymentFailed"
	}
	return fmt.Sprintf("Event(%d)", int(e))
}

// Command is a message the saga sends in response to an event. The store
// turns each one into an outbox row in the same transaction as the status
// change.
type Command int

const (
	CommandChargePayment    Command = iota + 1 // payment.commands
	CommandReleaseStock                        // inventory.commands
	CommandPublishConfirmed                    // order.events
	CommandPublishCancelled                    // order.events
)

// ErrInvalidTransition means the event is not valid in the order's current
// status, for example a redelivered StockReserved for an order that is
// already confirmed. Retrying cannot fix it.
var ErrInvalidTransition = errors.New("invalid saga transition")

type transition struct {
	to       Status
	commands []Command
}

// transitions is the whole saga in one table: from each status, which
// events are allowed, the status they lead to, and what to send next.
var transitions = map[Status]map[Event]transition{
	StatusPending: {
		EventStockReserved: {to: StatusInventoryReserved, commands: []Command{CommandChargePayment}},
		// Nothing was reserved, so there is nothing to compensate.
		EventStockRejected: {to: StatusCancelled, commands: []Command{CommandPublishCancelled}},
	},
	StatusInventoryReserved: {
		EventPaymentSucceeded: {to: StatusConfirmed, commands: []Command{CommandPublishConfirmed}},
		// Compensate the completed reservation step before cancelling.
		EventPaymentFailed: {to: StatusCancelled, commands: []Command{CommandReleaseStock, CommandPublishCancelled}},
	},
	// CONFIRMED and CANCELLED are final: no event moves an order out of them.
}

// next is the saga's state machine. It is a pure function: given the
// current status and an event, it returns the new status and the commands
// to send, and touches no database or network.
func next(s Status, e Event) (Status, []Command, error) {
	t, ok := transitions[s][e]
	if !ok {
		return s, nil, fmt.Errorf("%w: %s while %s", ErrInvalidTransition, e, s)
	}
	return t.to, t.commands, nil
}
