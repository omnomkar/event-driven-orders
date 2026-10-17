// Command gateway runs the public REST API. It serves health and metrics
// endpoints on the same port and calls the backend services over gRPC.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/omnomkar/event-driven-orders/pkg/config"
	"github.com/omnomkar/event-driven-orders/pkg/health"
	"github.com/omnomkar/event-driven-orders/pkg/logging"
	"github.com/omnomkar/event-driven-orders/pkg/metrics"
	"github.com/omnomkar/event-driven-orders/pkg/server"
	inventoryv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/inventory/v1"
	orderv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/order/v1"
	paymentv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/payment/v1"
	"github.com/omnomkar/event-driven-orders/services/gateway/internal/api"
)

const (
	serviceName     = "gateway"
	defaultHTTPAddr = ":8080"
	checkTimeout    = 2 * time.Second
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(health.ProbeMain(config.New().String("HTTP_ADDR", defaultHTTPAddr)))
	}

	logger := logging.New(os.Stdout, serviceName)
	slog.SetDefault(logger)
	if err := run(logger); err != nil {
		logger.Error("exiting", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg := config.New()
	httpAddr := cfg.String("HTTP_ADDR", defaultHTTPAddr)
	backends := map[string]string{
		"order":     cfg.Required("ORDER_GRPC_ADDR"),
		"payment":   cfg.Required("PAYMENT_GRPC_ADDR"),
		"inventory": cfg.Required("INVENTORY_GRPC_ADDR"),
	}
	shutdownTimeout := cfg.Duration("SHUTDOWN_TIMEOUT", 10*time.Second)
	if err := cfg.Err(); err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// ctx is cancelled on SIGINT or SIGTERM, which starts shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// grpc.NewClient does not dial until the first call, so the gateway can
	// start before the backends; /readyz reports which ones are not serving.
	// Plaintext is a local-only choice; mTLS is planned for a later stage.
	checks := health.NewRegistry()
	conns := make(map[string]*grpc.ClientConn, len(backends))
	for name, addr := range backends {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("create %s client: %w", name, err)
		}
		defer func() { _ = conn.Close() }()
		checks.Add(name, checkTimeout, health.GRPCCheck(conn))
		conns[name] = conn
	}

	// Public REST routes share the server with /healthz, /readyz, and /metrics.
	mux := health.AdminMux(checks)
	(&api.API{
		Orders:    orderv1.NewOrderServiceClient(conns["order"]),
		Payments:  paymentv1.NewPaymentServiceClient(conns["payment"]),
		Inventory: inventoryv1.NewInventoryServiceClient(conns["inventory"]),
		Logger:    logger,
	}).Register(mux)
	// Every request, public or admin, is timed by its route pattern.
	handler := metrics.NewHTTP(prometheus.DefaultRegisterer).Middleware(mux)
	logger.Info("started", "http_addr", httpAddr)
	err := server.RunHTTP(ctx, server.NewHTTP(httpAddr, handler), shutdownTimeout)
	logger.Info("stopped")
	return err
}
