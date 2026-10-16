package stock

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inventoryv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/inventory/v1"
)

// Server implements inventoryv1.InventoryServiceServer on top of a Store.
type Server struct {
	inventoryv1.UnimplementedInventoryServiceServer
	store  *Store
	logger *slog.Logger
}

// NewServer returns a gRPC server for store.
func NewServer(store *Store, logger *slog.Logger) *Server {
	return &Server{store: store, logger: logger}
}

// GetProduct returns one product with its current stock.
func (s *Server) GetProduct(ctx context.Context, req *inventoryv1.GetProductRequest) (*inventoryv1.GetProductResponse, error) {
	if req.GetProductId() == "" {
		return nil, status.Error(codes.InvalidArgument, "product_id is required")
	}
	p, err := s.store.GetProduct(ctx, req.GetProductId())
	if errors.Is(err, ErrNotFound) {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if err != nil {
		s.logger.Error("get product failed", "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	return &inventoryv1.GetProductResponse{Product: toProto(*p)}, nil
}

// ListProducts returns the whole catalog.
func (s *Server) ListProducts(ctx context.Context, _ *inventoryv1.ListProductsRequest) (*inventoryv1.ListProductsResponse, error) {
	products, err := s.store.ListProducts(ctx)
	if err != nil {
		s.logger.Error("list products failed", "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	out := make([]*inventoryv1.Product, len(products))
	for i, p := range products {
		out[i] = toProto(p)
	}
	return &inventoryv1.ListProductsResponse{Products: out}, nil
}

func toProto(p Product) *inventoryv1.Product {
	return &inventoryv1.Product{
		ProductId:         p.ID,
		Name:              p.Name,
		UnitPriceCents:    p.UnitPriceCents,
		Currency:          p.Currency,
		AvailableQuantity: p.Available,
	}
}
