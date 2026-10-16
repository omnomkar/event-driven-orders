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

// Standard test card number only.
const orderBody = `{
	"customer_id": "e2e-customer",
	"items": [
		{"product_id": "sku-keyboard", "quantity": 1},
		{"product_id": "sku-cable", "quantity": 3}
	],
	"card": {"number": "4242 4242 4242 4242", "exp_month": 12, "exp_year": 2030}
}`

// Seeded prices: one keyboard at 8999 plus three cables at 999.
const wantTotalCents = 8999 + 3*999

type order struct {
	OrderID    string `json:"order_id"`
	Status     string `json:"status"`
	TotalCents int64  `json:"total_cents"`
	Currency   string `json:"currency"`
}

// TestOrderFlow places an order through the gateway and follows the saga to
// CONFIRMED: inventory reserves stock, payment charges the total, and the
// order service records PENDING, INVENTORY_RESERVED, and CONFIRMED in order.
func TestOrderFlow(t *testing.T) {
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

	// Place the order. The gateway answers before the saga runs.
	resp, err := http.Post(baseURL+"/orders", "application/json", strings.NewReader(orderBody))
	if err != nil {
		t.Fatalf("POST /orders: %v", err)
	}
	var created order
	decode(t, resp, http.StatusAccepted, &created)
	if created.Status != "PENDING" || created.OrderID == "" {
		t.Fatalf("POST /orders returned %+v, want a PENDING order", created)
	}
	t.Logf("order %s accepted", created.OrderID)

	// Poll until the saga reaches a final state.
	var got order
	deadline := time.Now().Add(sagaTimeout)
	for {
		resp, err := http.Get(baseURL + "/orders/" + created.OrderID)
		if err != nil {
			t.Fatalf("GET /orders/{id}: %v", err)
		}
		decode(t, resp, http.StatusOK, &got)
		if got.Status == "CONFIRMED" || got.Status == "CANCELLED" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("order still %s after %s", got.Status, sagaTimeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got.Status != "CONFIRMED" || got.TotalCents != wantTotalCents || got.Currency != "USD" {
		t.Fatalf("final order %+v, want CONFIRMED for %d USD", got, wantTotalCents)
	}

	history := psql(t, stack, "orders", fmt.Sprintf(
		`SELECT status FROM order_status_history WHERE order_id = '%s' ORDER BY id`, created.OrderID))
	if want := []string{"PENDING", "INVENTORY_RESERVED", "CONFIRMED"}; !slices.Equal(history, want) {
		t.Errorf("order_status_history = %v, want %v", history, want)
	}

	payments := psql(t, stack, "payments", fmt.Sprintf(
		`SELECT status || ' ' || amount_cents FROM payments WHERE order_id = '%s'`, created.OrderID))
	if want := []string{fmt.Sprintf("SUCCEEDED %d", wantTotalCents)}; !slices.Equal(payments, want) {
		t.Errorf("payments for the order = %v, want exactly %v", payments, want)
	}
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
