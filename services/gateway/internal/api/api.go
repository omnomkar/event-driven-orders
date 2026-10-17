// Package api is the gateway's public REST API. It translates JSON requests
// into gRPC calls to the backend services.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/omnomkar/event-driven-orders/pkg/logging"
	commonv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/common/v1"
	inventoryv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/inventory/v1"
	orderv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/order/v1"
	paymentv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/payment/v1"
)

const (
	maxBodyBytes         = 64 << 10 // 64 KiB is far more than any valid order
	maxIdempotencyKeyLen = 200
	requestTimeout       = 5 * time.Second
)

// API holds the backend clients. The fields are the generated gRPC client
// interfaces, so tests can pass fakes.
type API struct {
	Orders    orderv1.OrderServiceClient
	Payments  paymentv1.PaymentServiceClient
	Inventory inventoryv1.InventoryServiceClient
	Logger    *slog.Logger
}

// Register adds the public routes to mux.
func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /orders", a.createOrder)
	mux.HandleFunc("GET /orders/{id}", a.getOrder)
	mux.HandleFunc("GET /products", a.listProducts)
	mux.HandleFunc("GET /products/{id}", a.getProduct)
}

type itemJSON struct {
	ProductID string `json:"product_id"`
	Quantity  int32  `json:"quantity"`
}

// cardJSON has no CVV field, and unknown fields are rejected, so a CVV is
// never accepted.
type cardJSON struct {
	Number   string `json:"number"`
	ExpMonth int32  `json:"exp_month"`
	ExpYear  int32  `json:"exp_year"`
}

type createOrderRequest struct {
	CustomerID string     `json:"customer_id"`
	Items      []itemJSON `json:"items"`
	Card       *cardJSON  `json:"card"`
}

type orderJSON struct {
	OrderID      string     `json:"order_id"`
	CustomerID   string     `json:"customer_id"`
	Status       string     `json:"status"`
	Items        []itemJSON `json:"items"`
	TotalCents   int64      `json:"total_cents"`
	Currency     string     `json:"currency,omitempty"`
	CancelReason string     `json:"cancel_reason,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

type productJSON struct {
	ProductID         string `json:"product_id"`
	Name              string `json:"name"`
	UnitPriceCents    int64  `json:"unit_price_cents"`
	Currency          string `json:"currency"`
	AvailableQuantity int32  `json:"available_quantity"`
}

// createOrder tokenizes the card, then creates the order with the token. It
// answers 202 Accepted with the PENDING order: the saga finishes later, and
// the client polls GET /orders/{id} for the outcome.
//
// With an Idempotency-Key header, repeating the request returns the order
// the key first created, in its current state, with 200 OK. Reusing the key
// with different items is 409 Conflict.
func (a *API) createOrder(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if msg := req.validate(); msg != "" {
		writeError(w, http.StatusBadRequest, msg)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > maxIdempotencyKeyLen {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Idempotency-Key must be at most %d characters", maxIdempotencyKeyLen))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()

	// The card number goes only to the payment service. Everything after
	// this line sees the token.
	tok, err := a.Payments.TokenizeCard(ctx, &paymentv1.TokenizeCardRequest{
		CardNumber: req.Card.Number,
		ExpMonth:   req.Card.ExpMonth,
		ExpYear:    req.Card.ExpYear,
	})
	if err != nil {
		a.writeGRPCError(w, "tokenize card", err)
		return
	}

	items := make([]*commonv1.LineItem, len(req.Items))
	for i, it := range req.Items {
		items[i] = &commonv1.LineItem{ProductId: it.ProductID, Quantity: it.Quantity}
	}
	resp, err := a.Orders.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		CustomerId:     req.CustomerID,
		Items:          items,
		PaymentToken:   tok.GetPaymentToken(),
		IdempotencyKey: key,
	})
	if err != nil {
		a.writeGRPCError(w, "create order", err)
		return
	}
	o := resp.GetOrder()
	w.Header().Set("Location", "/orders/"+o.GetOrderId())
	if resp.GetReplayed() {
		logging.WithOrder(a.Logger, o.GetOrderId()).Info("idempotent replay", "status", o.GetStatus().String())
		writeJSON(w, http.StatusOK, toOrderJSON(o))
		return
	}
	logging.WithOrder(a.Logger, o.GetOrderId()).Info("order accepted", "card_brand", tok.GetBrand())
	writeJSON(w, http.StatusAccepted, toOrderJSON(o))
}

func (a *API) getOrder(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	resp, err := a.Orders.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: r.PathValue("id")})
	if err != nil {
		a.writeGRPCError(w, "get order", err)
		return
	}
	writeJSON(w, http.StatusOK, toOrderJSON(resp.GetOrder()))
}

func (a *API) listProducts(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	resp, err := a.Inventory.ListProducts(ctx, &inventoryv1.ListProductsRequest{})
	if err != nil {
		a.writeGRPCError(w, "list products", err)
		return
	}
	products := make([]productJSON, len(resp.GetProducts()))
	for i, p := range resp.GetProducts() {
		products[i] = toProductJSON(p)
	}
	writeJSON(w, http.StatusOK, map[string]any{"products": products})
}

// getProduct returns one product with its current stock. The count is for
// display and may lag a just-placed order by up to the cache TTL; the
// reservation itself always checks the database.
func (a *API) getProduct(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	resp, err := a.Inventory.GetProduct(ctx, &inventoryv1.GetProductRequest{ProductId: r.PathValue("id")})
	if err != nil {
		a.writeGRPCError(w, "get product", err)
		return
	}
	writeJSON(w, http.StatusOK, toProductJSON(resp.GetProduct()))
}

func toProductJSON(p *inventoryv1.Product) productJSON {
	return productJSON{
		ProductID:         p.GetProductId(),
		Name:              p.GetName(),
		UnitPriceCents:    p.GetUnitPriceCents(),
		Currency:          p.GetCurrency(),
		AvailableQuantity: p.GetAvailableQuantity(),
	}
}

// validate checks the request's shape. The services do the detailed checks
// (Luhn, expiry, quantity limits); this only avoids tokenizing a card for a
// request that cannot become an order.
func (req *createOrderRequest) validate() string {
	switch {
	case strings.TrimSpace(req.CustomerID) == "":
		return "customer_id is required"
	case len(req.Items) == 0:
		return "items must contain at least one item"
	case req.Card == nil || req.Card.Number == "":
		return "card.number is required"
	case req.Card.ExpMonth == 0 || req.Card.ExpYear == 0:
		return "card.exp_month and card.exp_year are required"
	}
	for i, it := range req.Items {
		if it.ProductID == "" || it.Quantity <= 0 {
			return fmt.Sprintf("items[%d] needs a product_id and a positive quantity", i)
		}
	}
	return ""
}

// decodeJSON reads one JSON object, rejecting unknown fields and oversized
// bodies. Its errors name the problem but never echo request values, so a
// card number cannot leak through an error message.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	err := dec.Decode(v)
	var (
		typeErr *json.UnmarshalTypeError
		sizeErr *http.MaxBytesError
	)
	switch {
	case err == nil:
		if dec.More() {
			return errors.New("request body must be a single JSON object")
		}
		return nil
	case errors.As(err, &typeErr):
		return fmt.Errorf("field %q has the wrong type", typeErr.Field)
	case errors.As(err, &sizeErr):
		return fmt.Errorf("request body is larger than %d bytes", maxBodyBytes)
	case strings.HasPrefix(err.Error(), "json: unknown field "):
		// The message names only the field, for example "cvv".
		return fmt.Errorf("unknown field %s", strings.TrimPrefix(err.Error(), "json: unknown field "))
	case errors.Is(err, io.EOF):
		return errors.New("request body is empty")
	}
	return errors.New("request body is not valid JSON")
}

// writeGRPCError maps a backend error to an HTTP status. InvalidArgument,
// NotFound, and AlreadyExists messages come from code written to be
// client-safe; anything else is logged and hidden.
func (a *API) writeGRPCError(w http.ResponseWriter, op string, err error) {
	st := status.Convert(err)
	switch st.Code() {
	case codes.InvalidArgument:
		writeError(w, http.StatusBadRequest, st.Message())
	case codes.NotFound:
		writeError(w, http.StatusNotFound, st.Message())
	case codes.AlreadyExists:
		writeError(w, http.StatusConflict, st.Message())
	case codes.Unavailable, codes.DeadlineExceeded:
		a.Logger.Warn("backend unavailable", "op", op, "code", st.Code().String())
		writeError(w, http.StatusServiceUnavailable, "service temporarily unavailable")
	default:
		a.Logger.Error("backend call failed", "op", op, "code", st.Code().String(), "error", st.Message())
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func toOrderJSON(o *orderv1.Order) orderJSON {
	items := make([]itemJSON, len(o.GetItems()))
	for i, it := range o.GetItems() {
		items[i] = itemJSON{ProductID: it.GetProductId(), Quantity: it.GetQuantity()}
	}
	return orderJSON{
		OrderID:      o.GetOrderId(),
		CustomerID:   o.GetCustomerId(),
		Status:       strings.TrimPrefix(o.GetStatus().String(), "ORDER_STATUS_"),
		Items:        items,
		TotalCents:   o.GetTotalCents(),
		Currency:     o.GetCurrency(),
		CancelReason: o.GetCancelReason(),
		CreatedAt:    o.GetCreatedAt().AsTime(),
		UpdatedAt:    o.GetUpdatedAt().AsTime(),
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
