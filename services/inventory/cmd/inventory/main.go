// Command inventory runs the inventory service: a gRPC API plus an admin HTTP server
// for health checks and metrics.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	grpchealth "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/omnomkar/event-driven-orders/pkg/config"
	"github.com/omnomkar/event-driven-orders/pkg/health"
	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/logging"
	"github.com/omnomkar/event-driven-orders/pkg/server"
	inventoryv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/inventory/v1"
)

const (
	serviceName      = "inventory"
	defaultAdminAddr = ":8083"
	checkTimeout     = 2 * time.Second
	healthSyncPeriod = 5 * time.Second
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(health.ProbeMain(config.New().String("ADMIN_ADDR", defaultAdminAddr)))
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
	grpcAddr := cfg.String("GRPC_ADDR", ":9003")
	adminAddr := cfg.String("ADMIN_ADDR", defaultAdminAddr)
	databaseURL := cfg.Required("DATABASE_URL")
	brokers := cfg.RequiredList("KAFKA_BROKERS")
	redisAddr := cfg.Required("REDIS_ADDR")
	shutdownTimeout := cfg.Duration("SHUTDOWN_TIMEOUT", 10*time.Second)
	if err := cfg.Err(); err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// ctx is cancelled on SIGINT or SIGTERM, which starts shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// pgxpool connects lazily, so the service starts even if Postgres is not
	// up yet; /readyz reports it until it is.
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("create postgres pool: %w", err)
	}
	defer pool.Close()

	kc, err := kafka.NewClient(kafka.Config{Brokers: brokers, ClientID: serviceName})
	if err != nil {
		return err
	}
	defer kc.Close()

	// go-redis connects lazily, like pgxpool.
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()

	checks := health.NewRegistry()
	checks.Add("postgres", checkTimeout, pool.Ping)
	checks.Add("kafka", checkTimeout, kafka.ReadyCheck(kc))
	checks.Add("redis", checkTimeout, func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	})

	grpcServer := grpc.NewServer()
	healthServer := grpchealth.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	inventoryv1.RegisterInventoryServiceServer(grpcServer, &inventoryServer{})

	// errgroup runs each server in its own goroutine. If one fails, gctx is
	// cancelled so the others shut down too; Wait returns the first error.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return server.RunGRPC(gctx, grpcServer, grpcAddr, shutdownTimeout)
	})
	g.Go(func() error {
		return server.RunHTTP(gctx, server.NewHTTP(adminAddr, health.AdminMux(checks)), shutdownTimeout)
	})
	g.Go(func() error {
		health.SyncGRPCHealth(gctx, checks, healthServer, healthSyncPeriod)
		return nil
	})

	logger.Info("started", "grpc_addr", grpcAddr, "admin_addr", adminAddr)
	err = g.Wait()
	logger.Info("stopped")
	return err
}

// inventoryServer implements inventoryv1.InventoryServiceServer. The embedded
// Unimplemented type answers every RPC not yet written with codes.Unimplemented.
type inventoryServer struct {
	inventoryv1.UnimplementedInventoryServiceServer
}
