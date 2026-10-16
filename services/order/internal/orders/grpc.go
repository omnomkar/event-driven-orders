package orders

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/omnomkar/event-driven-orders/pkg/logging"
	commonv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/common/v1"
	orderv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/order/v1"
)

// Server implements orderv1.OrderServiceServer on top of a Store.
type Server struct {
	orderv1.UnimplementedOrderServiceServer
	store  *Store
	logger *slog.Logger
}

// NewServer returns a gRPC server for store.
func NewServer(store *Store, logger *slog.Logger) *Server {
	return &Server{store: store, logger: logger}
}

// CreateOrder stores a PENDING order and starts its saga. It returns as soon
// as the order is stored; the saga finishes asynchronously.
func (s *Server) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	items := make([]Item, len(req.GetItems()))
	for i, it := range req.GetItems() {
		items[i] = Item{ProductID: it.GetProductId(), Quantity: it.GetQuantity()}
	}
	o, err := s.store.Create(ctx, CreateParams{
		CustomerID:     req.GetCustomerId(),
		Items:          items,
		PaymentToken:   req.GetPaymentToken(),
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, s.toStatus(err)
	}
	logging.WithOrder(s.logger, o.ID).Info("order created", "status", o.Status, "items", len(o.Items))
	return &orderv1.CreateOrderResponse{Order: toProto(o)}, nil
}

// GetOrder returns the order's current state.
func (s *Server) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	o, err := s.store.Get(ctx, req.GetOrderId())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &orderv1.GetOrderResponse{Order: toProto(o)}, nil
}

// toStatus maps store errors to gRPC codes. Internal errors are logged here
// and returned without detail, so database messages never reach clients.
func (s *Server) toStatus(err error) error {
	switch {
	case errors.Is(err, ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	}
	s.logger.Error("request failed", "error", err)
	return status.Error(codes.Internal, "internal error")
}

var statusToProto = map[Status]orderv1.OrderStatus{
	StatusPending:           orderv1.OrderStatus_ORDER_STATUS_PENDING,
	StatusInventoryReserved: orderv1.OrderStatus_ORDER_STATUS_INVENTORY_RESERVED,
	StatusConfirmed:         orderv1.OrderStatus_ORDER_STATUS_CONFIRMED,
	StatusCancelled:         orderv1.OrderStatus_ORDER_STATUS_CANCELLED,
}

func toProto(o *Order) *orderv1.Order {
	items := make([]*commonv1.LineItem, len(o.Items))
	for i, it := range o.Items {
		items[i] = &commonv1.LineItem{ProductId: it.ProductID, Quantity: it.Quantity}
	}
	return &orderv1.Order{
		OrderId:      o.ID,
		CustomerId:   o.CustomerID,
		Status:       statusToProto[o.Status],
		Items:        items,
		TotalCents:   o.TotalCents,
		Currency:     o.Currency,
		CancelReason: o.CancelReason,
		CreatedAt:    timestamppb.New(o.CreatedAt),
		UpdatedAt:    timestamppb.New(o.UpdatedAt),
	}
}
