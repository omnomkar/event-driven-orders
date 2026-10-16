//go:build integration

package outbox_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/outbox"
	"github.com/omnomkar/event-driven-orders/pkg/testinfra"
)

const topic = "test.events"

// The same table shape every service creates in its own migrations.
const createOutbox = `
CREATE TABLE outbox (
    id         uuid PRIMARY KEY,
    topic      text NOT NULL,
    key        text NOT NULL,
    payload    bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    sent_at    timestamptz
);
CREATE INDEX outbox_unsent_idx ON outbox (created_at) WHERE sent_at IS NULL;`

func setup(t *testing.T) (*pgxpool.Pool, []string) {
	t.Helper()
	pool := testinfra.Postgres(t, nil)
	if _, err := pool.Exec(context.Background(), createOutbox); err != nil {
		t.Fatalf("create outbox table: %v", err)
	}
	return pool, testinfra.Kafka(t, topic)
}

// addRows writes n messages, keyed order-0 .. order-(n-1), in one transaction.
func addRows(t *testing.T, pool *pgxpool.Pool, n int) {
	t.Helper()
	err := pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
		for i := range n {
			key := fmt.Sprintf("order-%d", i)
			if err := outbox.Add(context.Background(), tx, topic, key, wrapperspb.String(key)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("add outbox rows: %v", err)
	}
}

func unsent(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&n); err != nil {
		t.Fatalf("count unsent: %v", err)
	}
	return n
}

func TestRelayPublishesPendingRowsAndMarksThemSent(t *testing.T) {
	pool, brokers := setup(t)
	addRows(t, pool, 3)

	client, err := kafka.NewClient(kafka.Config{Brokers: brokers, ClientID: "relay-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	relay := outbox.NewRelay(pool, client, outbox.RelayConfig{}, testinfra.Logger())

	n, err := relay.RelayOnce(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("RelayOnce = %d, %v; want 3, nil", n, err)
	}
	if got := unsent(t, pool); got != 0 {
		t.Errorf("%d rows still unsent after relay", got)
	}

	recs := testinfra.Consume(t, brokers, 3, 20*time.Second, topic)
	if len(recs) != 3 {
		t.Fatalf("consumed %d records, want 3", len(recs))
	}
	for _, r := range recs {
		var msg wrapperspb.StringValue
		if err := proto.Unmarshal(r.Value, &msg); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		if msg.GetValue() != string(r.Key) {
			t.Errorf("record key %q carries payload %q; want them equal", r.Key, msg.GetValue())
		}
	}

	// A second pass finds nothing left to send.
	if n, err := relay.RelayOnce(context.Background()); err != nil || n != 0 {
		t.Errorf("second RelayOnce = %d, %v; want 0, nil", n, err)
	}
}

// Two relays polling the same table at once must publish every row exactly
// once between them. FOR UPDATE SKIP LOCKED is what guarantees this.
func TestConcurrentRelaysDoNotDoubleSend(t *testing.T) {
	pool, brokers := setup(t)
	const rows = 200
	addRows(t, pool, rows)

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		total int
	)
	for i := range 2 {
		client, err := kafka.NewClient(kafka.Config{Brokers: brokers, ClientID: fmt.Sprintf("relay-%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		relay := outbox.NewRelay(pool, client, outbox.RelayConfig{BatchSize: 10}, testinfra.Logger())
		wg.Go(func() {
			for {
				n, err := relay.RelayOnce(context.Background())
				if err != nil {
					t.Errorf("relay %d: %v", i, err)
					return
				}
				if n == 0 {
					return
				}
				mu.Lock()
				total += n
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if total != rows {
		t.Errorf("relays marked %d rows sent, want %d", total, rows)
	}
	if got := unsent(t, pool); got != 0 {
		t.Errorf("%d rows still unsent", got)
	}
	recs := testinfra.Consume(t, brokers, rows+1, 10*time.Second, topic)
	if len(recs) != rows {
		t.Errorf("Kafka has %d records, want exactly %d", len(recs), rows)
	}
}
