//go:build integration

package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/modules/compose"
)

const (
	composeFile = "../../deploy/compose/docker-compose.yml"
	envFile     = "../../deploy/compose/.env.example"
	sagaTimeout = 60 * time.Second
)

// Standard test card numbers only: 4242... always succeeds, and
// 4000 0000 0000 0002 always declines.
const (
	goodCard    = `{"number": "4242 4242 4242 4242", "exp_month": 12, "exp_year": 2030}`
	declineCard = `{"number": "4000 0000 0000 0002", "exp_month": 12, "exp_year": 2030}`
)

func orderBody(customer, items, card string) string {
	return fmt.Sprintf(`{"customer_id": %q, "items": %s, "card": %s}`, customer, items, card)
}

// Seeded prices: one keyboard at 8999 plus three cables at 999.
const (
	happyItems     = `[{"product_id": "sku-keyboard", "quantity": 1}, {"product_id": "sku-cable", "quantity": 3}]`
	wantTotalCents = 8999 + 3*999
)

type order struct {
	OrderID      string `json:"order_id"`
	Status       string `json:"status"`
	TotalCents   int64  `json:"total_cents"`
	Currency     string `json:"currency"`
	CancelReason string `json:"cancel_reason"`
}

// TestOrderFlows starts the whole Compose stack once and places orders
// through the gateway, following each saga to its final state.
func TestOrderFlows(t *testing.T) {
	stack := startStack(t)
	ctx := context.Background()
	gateway, err := stack.ServiceContainer(ctx, "gateway")
	if err != nil {
		t.Fatalf("find gateway: %v", err)
	}
	baseURL, err := gateway.PortEndpoint(ctx, "8080/tcp", "http")
	if err != nil {
		t.Fatalf("gateway endpoint: %v", err)
	}
	c := &client{t: t, baseURL: baseURL, stack: stack}

	// Inventory reserves stock, payment charges the total, and the order
	// records PENDING, INVENTORY_RESERVED, and CONFIRMED in that order.
	t.Run("happy path confirms", func(t *testing.T) {
		c.t = t
		created := c.create(orderBody("e2e-happy", happyItems, goodCard), "", http.StatusAccepted)
		got := c.waitFinal(created.OrderID)
		if got.Status != "CONFIRMED" || got.TotalCents != wantTotalCents || got.Currency != "USD" {
			t.Fatalf("final order %+v, want CONFIRMED for %d USD", got, wantTotalCents)
		}
		c.wantHistory(created.OrderID, "PENDING", "INVENTORY_RESERVED", "CONFIRMED")
		c.wantPayments(created.OrderID, fmt.Sprintf("SUCCEEDED %d", wantTotalCents))
	})

	// The decline card fails payment, so the saga releases the reserved
	// stock and only then cancels the order.
	t.Run("payment declined releases stock and cancels", func(t *testing.T) {
		c.t = t
		before := c.available("sku-monitor")
		created := c.create(orderBody("e2e-decline", `[{"product_id": "sku-monitor", "quantity": 2}]`, declineCard), "", http.StatusAccepted)
		got := c.waitFinal(created.OrderID)
		if got.Status != "CANCELLED" || got.CancelReason != "card declined" {
			t.Fatalf("final order %+v, want CANCELLED with reason %q", got, "card declined")
		}
		c.wantHistory(created.OrderID, "PENDING", "INVENTORY_RESERVED", "CANCELLING", "CANCELLED")
		c.wantPayments(created.OrderID, "FAILED 49998")
		reservation := psql(t, stack, "inventory", fmt.Sprintf(
			`SELECT status FROM reservations WHERE order_id = '%s'`, created.OrderID))
		if !slices.Equal(reservation, []string{"RELEASED"}) {
			t.Errorf("reservation = %v, want RELEASED", reservation)
		}
		if after := c.available("sku-monitor"); after != before {
			t.Errorf("sku-monitor available %d after the cancel, want restored to %d", after, before)
		}
	})

	// Nothing can be reserved, so the order is cancelled with no payment
	// attempt at all.
	t.Run("out of stock cancels without payment", func(t *testing.T) {
		c.t = t
		created := c.create(orderBody("e2e-soldout", `[{"product_id": "sku-soldout", "quantity": 1}]`, goodCard), "", http.StatusAccepted)
		got := c.waitFinal(created.OrderID)
		if got.Status != "CANCELLED" || !strings.Contains(got.CancelReason, "sku-soldout") {
			t.Fatalf("final order %+v, want CANCELLED naming sku-soldout", got)
		}
		c.wantHistory(created.OrderID, "PENDING", "CANCELLED")
		c.wantPayments(created.OrderID)
	})

	// The same Idempotency-Key twice returns the same order and charges
	// once. The same key with different items is refused.
	t.Run("idempotency key", func(t *testing.T) {
		c.t = t
		body := orderBody("e2e-idem", `[{"product_id": "sku-headset", "quantity": 1}]`, goodCard)
		first := c.create(body, "idem-key-1", http.StatusAccepted)
		second := c.create(body, "idem-key-1", http.StatusOK)
		if second.OrderID != first.OrderID {
			t.Fatalf("second request got order %s, want %s", second.OrderID, first.OrderID)
		}
		if got := c.waitFinal(first.OrderID); got.Status != "CONFIRMED" {
			t.Fatalf("final order %+v, want CONFIRMED", got)
		}
		// A retry after the saga finished still returns the order, now final.
		if again := c.create(body, "idem-key-1", http.StatusOK); again.OrderID != first.OrderID || again.Status != "CONFIRMED" {
			t.Errorf("retry after completion = %+v, want order %s CONFIRMED", again, first.OrderID)
		}
		c.wantPayments(first.OrderID, "SUCCEEDED 7999")

		other := orderBody("e2e-idem", `[{"product_id": "sku-headset", "quantity": 2}]`, goodCard)
		c.create(other, "idem-key-1", http.StatusConflict)
		orders := psql(t, stack, "orders", `SELECT count(*) FROM orders WHERE customer_id = 'e2e-idem'`)
		if !slices.Equal(orders, []string{"1"}) {
			t.Errorf("orders for the customer = %v, want 1", orders)
		}
	})
}

// client places and inspects orders through the gateway. t is swapped for
// each subtest so failures are reported against the right one.
type client struct {
	t       *testing.T
	baseURL string
	stack   *compose.DockerCompose
}

// create POSTs body (with key as the Idempotency-Key header, if set),
// checks the status code, and returns the order in the response.
func (c *client) create(body, key string, wantCode int) order {
	c.t.Helper()
	req, err := http.NewRequest(http.MethodPost, c.baseURL+"/orders", strings.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("POST /orders: %v", err)
	}
	var o order
	decode(c.t, resp, wantCode, &o)
	if wantCode == http.StatusAccepted && (o.Status != "PENDING" || o.OrderID == "") {
		c.t.Fatalf("POST /orders returned %+v, want a PENDING order", o)
	}
	return o
}

// waitFinal polls GET /orders/{id} until the saga reaches a final state.
// Polling with a deadline returns as soon as the order is final and fails
// clearly if it never is.
func (c *client) waitFinal(id string) order {
	c.t.Helper()
	deadline := time.Now().Add(sagaTimeout)
	for {
		resp, err := http.Get(c.baseURL + "/orders/" + id)
		if err != nil {
			c.t.Fatalf("GET /orders/{id}: %v", err)
		}
		var got order
		decode(c.t, resp, http.StatusOK, &got)
		if got.Status == "CONFIRMED" || got.Status == "CANCELLED" {
			return got
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("order still %s after %s", got.Status, sagaTimeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (c *client) wantHistory(orderID string, want ...string) {
	c.t.Helper()
	history := psql(c.t, c.stack, "orders", fmt.Sprintf(
		`SELECT status FROM order_status_history WHERE order_id = '%s' ORDER BY id`, orderID))
	if !slices.Equal(history, want) {
		c.t.Errorf("order_status_history = %v, want %v", history, want)
	}
}

// wantPayments checks the order's payment rows as "STATUS amount" strings.
// No want means no payment may exist.
func (c *client) wantPayments(orderID string, want ...string) {
	c.t.Helper()
	payments := psql(c.t, c.stack, "payments", fmt.Sprintf(
		`SELECT status || ' ' || amount_cents FROM payments WHERE order_id = '%s'`, orderID))
	if !slices.Equal(payments, want) {
		c.t.Errorf("payments for the order = %v, want exactly %v", payments, want)
	}
}

// available reads a product's stock through GET /products.
func (c *client) available(productID string) int {
	c.t.Helper()
	resp, err := http.Get(c.baseURL + "/products")
	if err != nil {
		c.t.Fatalf("GET /products: %v", err)
	}
	var body struct {
		Products []struct {
			ProductID         string `json:"product_id"`
			AvailableQuantity int    `json:"available_quantity"`
		} `json:"products"`
	}
	decode(c.t, resp, http.StatusOK, &body)
	for _, p := range body.Products {
		if p.ProductID == productID {
			return p.AvailableQuantity
		}
	}
	c.t.Fatalf("product %s not in the catalog", productID)
	return 0
}

// startStack builds and starts the Compose stack, waits until every
// container is healthy, and tears it down (with its volumes) when the test
// ends.
func startStack(t *testing.T) *compose.DockerCompose {
	t.Helper()
	stack, err := compose.NewDockerComposeWith(
		compose.WithStackFiles(composeFile),
		compose.StackIdentifier("eventorders-e2e"),
	)
	if err != nil {
		t.Fatalf("load compose file: %v", err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t, stack)
		}
		if err := stack.Down(context.Background(), compose.RemoveOrphans(true), compose.RemoveVolumes(true)); err != nil {
			t.Errorf("compose down: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := stack.WithEnv(readEnv(t)).Up(ctx, compose.Wait(true)); err != nil {
		t.Fatalf("compose up: %v", err)
	}
	return stack
}

// dumpLogs prints each application container's logs, so a failed run in CI
// can be diagnosed after the stack is gone.
func dumpLogs(t *testing.T, stack *compose.DockerCompose) {
	ctx := context.Background()
	for _, svc := range []string{"gateway", "order", "inventory", "payment"} {
		ctr, err := stack.ServiceContainer(ctx, svc)
		if err != nil {
			t.Logf("%s: no container: %v", svc, err)
			continue
		}
		logs, err := ctr.Logs(ctx)
		if err != nil {
			t.Logf("%s: read logs: %v", svc, err)
			continue
		}
		b, _ := io.ReadAll(logs)
		_ = logs.Close()
		t.Logf("--- %s logs ---\n%s", svc, b)
	}
}

// readEnv parses the KEY=VALUE lines of envFile, skipping comments and
// blanks. The committed example holds local-only development passwords.
func readEnv(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open(envFile)
	if err != nil {
		t.Fatalf("open %s: %v", envFile, err)
	}
	defer func() { _ = f.Close() }()
	env := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			env[k] = v
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read %s: %v", envFile, err)
	}
	return env
}

// psql runs query inside the Postgres container, so the database port does
// not need to be published, and returns one string per row.
func psql(t *testing.T, stack *compose.DockerCompose, db, query string) []string {
	t.Helper()
	ctx := context.Background()
	pg, err := stack.ServiceContainer(ctx, "postgres")
	if err != nil {
		t.Fatalf("find postgres: %v", err)
	}
	code, out, err := pg.Exec(ctx, []string{"psql", "-U", "postgres", "-d", db, "-tA", "-v", "ON_ERROR_STOP=1", "-c", query}, exec.Multiplexed())
	if err != nil {
		t.Fatalf("exec psql: %v", err)
	}
	b, _ := io.ReadAll(out)
	if code != 0 {
		t.Fatalf("psql exited %d: %s", code, b)
	}
	var rows []string
	for line := range strings.Lines(strings.TrimSpace(string(b))) {
		if line = strings.TrimSpace(line); line != "" {
			rows = append(rows, line)
		}
	}
	return rows
}

func decode(t *testing.T, resp *http.Response, wantCode int, v any) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantCode {
		t.Fatalf("%s %s: status %d, want %d; body %s", resp.Request.Method, resp.Request.URL.Path, resp.StatusCode, wantCode, b)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
}
