package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inventoryv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/inventory/v1"
	orderv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/order/v1"
	paymentv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/payment/v1"
)

// Standard test card number only.
const testCard = "4242424242424242"

// The fakes embed the generated client interfaces, so they satisfy them
// while overriding only the methods a test needs.
type fakePayments struct {
	paymentv1.PaymentServiceClient
	calls int
	err   error
}

func (f *fakePayments) TokenizeCard(_ context.Context, _ *paymentv1.TokenizeCardRequest, _ ...grpc.CallOption) (*paymentv1.TokenizeCardResponse, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &paymentv1.TokenizeCardResponse{PaymentToken: "tok_fake", Last4: "4242", Brand: "visa"}, nil
}

type fakeOrders struct {
	orderv1.OrderServiceClient
	created  *orderv1.CreateOrderRequest
	order    *orderv1.Order
	replayed bool
	err      error
}

func (f *fakeOrders) CreateOrder(_ context.Context, in *orderv1.CreateOrderRequest, _ ...grpc.CallOption) (*orderv1.CreateOrderResponse, error) {
	f.created = in
	return &orderv1.CreateOrderResponse{Order: &orderv1.Order{
		OrderId: "0192f5d1-0000-7000-8000-000000000001", CustomerId: in.GetCustomerId(),
		Status: orderv1.OrderStatus_ORDER_STATUS_PENDING, Items: in.GetItems(),
	}, Replayed: f.replayed}, f.err
}

func (f *fakeOrders) GetOrder(_ context.Context, _ *orderv1.GetOrderRequest, _ ...grpc.CallOption) (*orderv1.GetOrderResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &orderv1.GetOrderResponse{Order: f.order}, nil
}

type fakeInventory struct {
	inventoryv1.InventoryServiceClient
}

func (fakeInventory) GetProduct(_ context.Context, in *inventoryv1.GetProductRequest, _ ...grpc.CallOption) (*inventoryv1.GetProductResponse, error) {
	if in.GetProductId() != "sku-cable" {
		return nil, status.Error(codes.NotFound, "product not found")
	}
	return &inventoryv1.GetProductResponse{Product: &inventoryv1.Product{
		ProductId: "sku-cable", Name: "USB-C cable", UnitPriceCents: 999, Currency: "USD", AvailableQuantity: 10,
	}}, nil
}

func (fakeInventory) ListProducts(context.Context, *inventoryv1.ListProductsRequest, ...grpc.CallOption) (*inventoryv1.ListProductsResponse, error) {
	return &inventoryv1.ListProductsResponse{Products: []*inventoryv1.Product{
		{ProductId: "sku-cable", Name: "USB-C cable", UnitPriceCents: 999, Currency: "USD", AvailableQuantity: 10},
	}}, nil
}

type harness struct {
	payments *fakePayments
	orders   *fakeOrders
	logs     *bytes.Buffer
	mux      *http.ServeMux
}

func newHarness() *harness {
	h := &harness{payments: &fakePayments{}, orders: &fakeOrders{}, logs: &bytes.Buffer{}, mux: http.NewServeMux()}
	a := &API{Orders: h.orders, Payments: h.payments, Inventory: fakeInventory{}, Logger: slog.New(slog.NewJSONHandler(h.logs, nil))}
	a.Register(h.mux)
	return h
}

func (h *harness) do(method, path, body string, header map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)
	return rec
}

const validOrder = `{"customer_id":"cust-1","items":[{"product_id":"sku-cable","quantity":2}],
	"card":{"number":"4242 4242 4242 4242","exp_month":12,"exp_year":2030}}`

func TestCreateOrderTokenizesThenCreates(t *testing.T) {
	h := newHarness()
	rec := h.do("POST", "/orders", validOrder, map[string]string{"Idempotency-Key": "key-1"})

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d, want 202; body %s", rec.Code, rec.Body)
	}
	var got orderJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "PENDING" || got.OrderID == "" || len(got.Items) != 1 {
		t.Errorf("response %+v, want the PENDING order", got)
	}
	if loc := rec.Header().Get("Location"); loc != "/orders/"+got.OrderID {
		t.Errorf("Location %q", loc)
	}
	c := h.orders.created
	if c.GetPaymentToken() != "tok_fake" || c.GetIdempotencyKey() != "key-1" || c.GetCustomerId() != "cust-1" {
		t.Errorf("CreateOrder request %v; want token, key, and customer passed through", c)
	}
	if strings.Contains(c.String(), "4242") {
		t.Error("CreateOrder request contains card digits")
	}
	if strings.Contains(h.logs.String(), testCard) || strings.Contains(h.logs.String(), "4242 4242") {
		t.Error("gateway logs contain the card number")
	}
}

func TestCreateOrderBadRequests(t *testing.T) {
	tests := []struct {
		name, body, wantMsg string
	}{
		{"cvv is rejected", `{"customer_id":"c","items":[{"product_id":"p","quantity":1}],"card":{"number":"` + testCard + `","exp_month":12,"exp_year":2030,"cvv":"123"}}`, `unknown field \"cvv\"`},
		{"malformed json", `{"card":{"number":"` + testCard + `"`, "not valid JSON"},
		{"wrong type", `{"customer_id":"c","items":[{"product_id":"p","quantity":"two"}],"card":{"number":"` + testCard + `","exp_month":12,"exp_year":2030}}`, "wrong type"},
		{"empty body", ``, "empty"},
		{"no items", `{"customer_id":"c","items":[],"card":{"number":"` + testCard + `","exp_month":12,"exp_year":2030}}`, "items"},
		{"no card", `{"customer_id":"c","items":[{"product_id":"p","quantity":1}]}`, "card.number"},
		{"two objects", validOrder + validOrder, "single JSON object"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness()
			rec := h.do("POST", "/orders", tt.body, nil)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d, want 400; body %s", rec.Code, rec.Body)
			}
			if !strings.Contains(rec.Body.String(), tt.wantMsg) {
				t.Errorf("body %s, want it to mention %q", rec.Body, tt.wantMsg)
			}
			if strings.Contains(rec.Body.String(), testCard) || strings.Contains(h.logs.String(), testCard) {
				t.Error("error response or logs contain the card number")
			}
			if h.payments.calls != 0 {
				t.Error("card was tokenized for a request that cannot become an order")
			}
		})
	}
}

func TestBackendErrorsMapToHTTP(t *testing.T) {
	tests := []struct {
		name     string
		tokenErr error
		orderErr error
		want     int
	}{
		{"invalid card", status.Error(codes.InvalidArgument, "card number is not valid"), nil, http.StatusBadRequest},
		{"invalid order", nil, status.Error(codes.InvalidArgument, "invalid order: too many items"), http.StatusBadRequest},
		{"idempotency key reused", nil, status.Error(codes.AlreadyExists, "idempotency key was already used with a different request"), http.StatusConflict},
		{"order service down", nil, status.Error(codes.Unavailable, "connection refused"), http.StatusServiceUnavailable},
		{"internal", nil, status.Error(codes.Internal, "boom"), http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness()
			h.payments.err, h.orders.err = tt.tokenErr, tt.orderErr
			rec := h.do("POST", "/orders", validOrder, nil)
			if rec.Code != tt.want {
				t.Errorf("status %d, want %d; body %s", rec.Code, tt.want, rec.Body)
			}
			if tt.want == http.StatusInternalServerError && strings.Contains(rec.Body.String(), "boom") {
				t.Error("internal error detail leaked to the client")
			}
		})
	}
}

// A repeated request with the same Idempotency-Key gets the original order
// with 200 instead of 202, since nothing new was accepted.
func TestCreateOrderReplayReturns200(t *testing.T) {
	h := newHarness()
	h.orders.replayed = true
	rec := h.do("POST", "/orders", validOrder, map[string]string{"Idempotency-Key": "key-1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200; body %s", rec.Code, rec.Body)
	}
	if loc := rec.Header().Get("Location"); loc != "/orders/0192f5d1-0000-7000-8000-000000000001" {
		t.Errorf("Location = %q", loc)
	}
}

func TestGetOrder(t *testing.T) {
	h := newHarness()
	h.orders.order = &orderv1.Order{
		OrderId: "0192f5d1-0000-7000-8000-000000000001", Status: orderv1.OrderStatus_ORDER_STATUS_CONFIRMED,
		TotalCents: 1998, Currency: "USD",
	}
	rec := h.do("GET", "/orders/0192f5d1-0000-7000-8000-000000000001", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	var got orderJSON
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if got.Status != "CONFIRMED" || got.TotalCents != 1998 || got.Currency != "USD" {
		t.Errorf("response %+v", got)
	}

	h.orders.err = status.Error(codes.NotFound, "order not found")
	if rec := h.do("GET", "/orders/0192f5d1-0000-7000-8000-000000000002", "", nil); rec.Code != http.StatusNotFound {
		t.Errorf("unknown order: status %d, want 404", rec.Code)
	}
}

func TestListProducts(t *testing.T) {
	rec := newHarness().do("GET", "/products", "", nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"product_id":"sku-cable"`) {
		t.Errorf("status %d body %s", rec.Code, rec.Body)
	}
}

func TestGetProduct(t *testing.T) {
	h := newHarness()
	rec := h.do("GET", "/products/sku-cable", "", nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"available_quantity":10`) {
		t.Errorf("status %d body %s", rec.Code, rec.Body)
	}
	if rec := h.do("GET", "/products/sku-missing", "", nil); rec.Code != http.StatusNotFound {
		t.Errorf("unknown product: status %d, want 404", rec.Code)
	}
}
