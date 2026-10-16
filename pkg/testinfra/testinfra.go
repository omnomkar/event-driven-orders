//go:build integration

// Package testinfra starts real Postgres and Kafka containers for integration
// tests. It is compiled only with the integration build tag.
package testinfra

import (
	"context"
	"io"
	"io/fs"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/omnomkar/event-driven-orders/pkg/postgres"
)

const (
	// Same Postgres image as the Compose stack.
	postgresImage = "postgres:18.4-alpine"
	// The testcontainers Kafka module supports only Confluent's packaging of
	// Kafka (3.5 here). The Compose stack runs apache/kafka 4.3.1; both speak
	// the same client protocol for everything these tests use.
	kafkaImage = "confluentinc/confluent-local:7.5.0"
)

// Postgres starts a Postgres container, applies migrations from fsys (if not
// nil), and returns a pool. The container is removed when the test ends.
func Postgres(t *testing.T, fsys fs.FS) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	ctr, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase("test"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if fsys != nil {
		mctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := postgres.Migrate(mctx, pool, fsys, Logger()); err != nil {
			t.Fatalf("migrate: %v", err)
		}
	}
	return pool
}

// Kafka starts a single-broker Kafka container, creates topics (3 partitions
// each), and returns the broker addresses.
func Kafka(t *testing.T, topics ...string) []string {
	t.Helper()
	ctx := context.Background()

	ctr, err := tckafka.Run(ctx, kafkaImage, tckafka.WithClusterID("test-cluster"))
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	brokers, err := ctr.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}

	if len(topics) > 0 {
		cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
		if err != nil {
			t.Fatalf("kafka admin client: %v", err)
		}
		defer cl.Close()
		resp, err := kadm.NewClient(cl).CreateTopics(ctx, 3, 1, nil, topics...)
		if err != nil {
			t.Fatalf("create topics: %v", err)
		}
		if err := resp.Error(); err != nil {
			t.Fatalf("create topics: %v", err)
		}
	}
	return brokers
}

// Consume reads records from topics, from the beginning, until it has n
// records or the timeout passes.
func Consume(t *testing.T, brokers []string, n int, timeout time.Duration, topics ...string) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		out = append(out, fetches.Records()...)
	}
	return out
}

// Logger returns a logger that discards output, to keep test logs readable.
func Logger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
