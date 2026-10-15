package health

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Probe calls /readyz on a server listening on addr (for example ":8081") and
// returns nil only on a 200 response. The healthcheck subcommand uses it,
// because distroless images have no curl.
func Probe(ctx context.Context, addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("parse address %q: %w", addr, err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	url := "http://" + net.JoinHostPort(host, port) + "/readyz"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("get %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("get %s: status %d", url, resp.StatusCode)
	}
	return nil
}

// ProbeMain runs Probe with a short timeout and returns a process exit code:
// 0 when ready, 1 otherwise.
func ProbeMain(addr string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := Probe(ctx, addr); err != nil {
		fmt.Println("not ready:", err)
		return 1
	}
	return 0
}
