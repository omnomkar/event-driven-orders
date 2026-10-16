package payments

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	paymentv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/payment/v1"
	"github.com/omnomkar/event-driven-orders/services/payment/internal/card"
)

// Server implements paymentv1.PaymentServiceServer on top of a Store.
type Server struct {
	paymentv1.UnimplementedPaymentServiceServer
	store  *Store
	logger *slog.Logger
}

// NewServer returns a gRPC server for store.
func NewServer(store *Store, logger *slog.Logger) *Server {
	return &Server{store: store, logger: logger}
}

// TokenizeCard exchanges card details for an opaque token. The request is
// never logged, and validation errors never contain the card number.
func (s *Server) TokenizeCard(ctx context.Context, req *paymentv1.TokenizeCardRequest) (*paymentv1.TokenizeCardResponse, error) {
	c, err := s.store.Tokenize(ctx, req.GetCardNumber(), req.GetExpMonth(), req.GetExpYear())
	switch {
	case errors.Is(err, card.ErrInvalidNumber), errors.Is(err, card.ErrInvalidExpiry), errors.Is(err, card.ErrExpired):
		return nil, status.Error(codes.InvalidArgument, err.Error())
	case err != nil:
		s.logger.Error("tokenize failed", "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	s.logger.Info("card tokenized", "brand", c.Brand, "last4", c.Last4)
	return &paymentv1.TokenizeCardResponse{PaymentToken: c.Token, Last4: c.Last4, Brand: c.Brand}, nil
}
