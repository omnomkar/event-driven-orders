// Package logging builds structured JSON loggers shared by every service.
package logging

import (
	"io"
	"log/slog"
)

// New returns a JSON logger that writes to w and tags every line with the
// service name.
func New(w io.Writer, service string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, nil)).With("service", service)
}

// WithOrder returns a logger that tags every line with orderID, so one order
// can be traced across services by searching for its ID.
func WithOrder(l *slog.Logger, orderID string) *slog.Logger {
	return l.With("order_id", orderID)
}
