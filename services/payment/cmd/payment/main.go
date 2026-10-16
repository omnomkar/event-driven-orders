// Command payment runs the payment service: a gRPC API for card tokenization,
// a consumer that charges payments, the outbox relay, and an admin HTTP
// server for health checks and metrics.
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
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	grpchealth "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/omnomkar/event-driven-orders/pkg/config"
	"github.com/omnomkar/event-driven-orders/pkg/health"
	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/logging"
	"github.com/omnomkar/event-driven-orders/pkg/outbox"
	"github.com/omnomkar/event-driven-orders/pkg/postgres"
	"github.com/omnomkar/event-driven-orders/pkg/server"
	paymentv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/payment/v1"
	"github.com/omnomkar/event-driven-orders/services/payment/internal/payments"
	"github.com/omnomkar/event-driven-orders/services/payment/migrations"
)

const (
	serviceName      = "payment"
	defaultAdminAddr = ":8082"
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
	grpcAddr := cfg.String("GRPC_ADDR", ":9002")
	adminAddr := cfg.String("ADMIN_ADDR", defaultAdminAddr)
	databaseURL := cfg.Required("DATABASE_URL")
	brokers := cfg.RequiredList("KAFKA_BROKERS")
	shutdownTimeout := cfg.Duration("SHUTDOWN_TIMEOUT", 10*time.Second)
	migrateTimeout := cfg.Duration("MIGRATE_TIMEOUT", 60*time.Second)
	outboxInterval := cfg.Duration("OUTBOX_POLL_INTERVAL", 200*time.Millisecond)
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

	// The schema must be current before the service serves anything.
	migrateCtx, cancelMigrate := context.WithTimeout(ctx, migrateTimeout)
	err = postgres.Migrate(migrateCtx, pool, migrations.FS, logger)
	cancelMigrate()
	if err != nil {
		return err
	}

	kafkaCfg := kafka.Config{Brokers: brokers, ClientID: serviceName}
	kc, err := kafka.NewClient(kafkaCfg)
	if err != nil {
		return err
	}
	defer kc.Close()

	store := payments.NewStore(pool)
	consumer, err := kafka.NewConsumer(kafkaCfg,
		kafka.ConsumerConfig{Group: serviceName, Topics: []string{kafka.TopicPaymentCommands}},
		store.CommandHandler(logger), logger)
	if err != nil {
		return err
	}
	defer consumer.Close()
	relay := outbox.NewRelay(pool, kc, outbox.RelayConfig{PollInterval: outboxInterval}, logger)

	checks := health.NewRegistry()
	checks.Add("postgres", checkTimeout, pool.Ping)
	checks.Add("kafka", checkTimeout, kafka.ReadyCheck(kc))

	grpcServer := grpc.NewServer()
	healthServer := grpchealth.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	paymentv1.RegisterPaymentServiceServer(grpcServer, payments.NewServer(store, logger))

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
		return consumer.Run(gctx)
	})
	g.Go(func() error {
		return relay.Run(gctx)
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
