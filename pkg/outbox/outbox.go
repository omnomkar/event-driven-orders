// Package outbox implements the transactional outbox pattern. A service
// writes the messages it wants to publish into an outbox table inside the
// same transaction as its state change, and a relay later publishes those
// rows to Kafka. Either the state change and its messages are both stored,
// or neither is.
//
// Each service owns an outbox table in its own database, created by its own
// migrations, with this shape:
//
//	CREATE TABLE outbox (
//	    id         uuid PRIMARY KEY,
//	    topic      text NOT NULL,
//	    key        text NOT NULL,
//	    payload    bytea NOT NULL,
//	    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
//	    sent_at    timestamptz
//	);
//	CREATE INDEX outbox_unsent_idx ON outbox (created_at) WHERE sent_at IS NULL;
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// pendingRows is registered with the default Prometheus registry. Each
// service has one outbox and one relay, so it needs no labels.
var pendingRows = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "outbox_pending_rows",
	Help: "Outbox rows not yet published to Kafka.",
})

// Add serializes msg and stores it in the outbox inside tx, the caller's
// transaction. It never opens or commits a transaction of its own, so the
// message is stored only if the caller's state change commits. key is the
// Kafka partition key, which is the order ID everywhere in this system.
func Add(ctx context.Context, tx pgx.Tx, topic, key string, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal outbox message for %s: %w", topic, err)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (id, topic, key, payload) VALUES ($1, $2, $3, $4)`,
		uuid.New(), topic, key, payload)
	if err != nil {
		return fmt.Errorf("insert outbox row for %s: %w", topic, err)
	}
	return nil
}

// RelayConfig tunes the relay. Zero values take the defaults.
type RelayConfig struct {
	// PollInterval is how long the relay waits between polls. Default 200ms.
	PollInterval time.Duration
	// BatchSize is the most rows published per poll. Default 100.
	BatchSize int
	// ProduceTimeout bounds one batch's publish to Kafka. Default 10s.
	ProduceTimeout time.Duration
	// PendingInterval is how often the relay counts unsent rows for the
	// outbox_pending_rows gauge. Default 5s.
	PendingInterval time.Duration
}

// Relay publishes unsent outbox rows to Kafka and marks them sent.
type Relay struct {
	pool   *pgxpool.Pool
	client *kgo.Client
	cfg    RelayConfig
	logger *slog.Logger
}

// NewRelay returns a Relay that reads from pool's outbox table and produces
// with client.
func NewRelay(pool *pgxpool.Pool, client *kgo.Client, cfg RelayConfig, logger *slog.Logger) *Relay {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 200 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.ProduceTimeout <= 0 {
		cfg.ProduceTimeout = 10 * time.Second
	}
	if cfg.PendingInterval <= 0 {
		cfg.PendingInterval = 5 * time.Second
	}
	return &Relay{pool: pool, client: client, cfg: cfg, logger: logger.With("component", "outbox_relay")}
}

// Run polls until ctx is cancelled. A full batch is followed immediately by
// another poll, so a backlog drains without waiting for the interval. Errors
// are logged and retried on the next poll; they never stop the relay.
func (r *Relay) Run(ctx context.Context) error {
	var lastCount time.Time
	for {
		if time.Since(lastCount) >= r.cfg.PendingInterval {
			lastCount = time.Now()
			if err := r.updatePending(ctx); err != nil && ctx.Err() == nil {
				r.logger.Warn("count pending outbox rows failed", "error", err)
			}
		}
		n, err := r.RelayOnce(ctx)
		if err != nil && ctx.Err() == nil {
			r.logger.Error("relay batch failed", "error", err, "published", n)
		}
		if n == r.cfg.BatchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.cfg.PollInterval):
		}
	}
}

// updatePending sets outbox_pending_rows. The partial index on unsent rows
// keeps the count cheap even when the table holds many sent rows.
func (r *Relay) updatePending(ctx context.Context) error {
	var n int64
	if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&n); err != nil {
		return fmt.Errorf("count unsent outbox rows: %w", err)
	}
	pendingRows.Set(float64(n))
	return nil
}

// RelayOnce publishes one batch of unsent rows and returns how many it
// marked sent.
//
// The rows are selected with FOR UPDATE SKIP LOCKED inside a transaction that
// stays open until they are marked sent. Two relay instances therefore never
// publish the same row at the same time: the second one skips rows the first
// has locked and takes the next ones instead.
//
// Delivery is at-least-once. If the process crashes after Kafka acknowledges
// a record but before the transaction commits, the row is still unsent and is
// published again on the next poll. Consumers deduplicate by message_id.
func (r *Relay) RelayOnce(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin relay transaction: %w", err)
	}
	// Rollback after a successful Commit is a no-op, so this is safe to defer.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	rows, err := tx.Query(ctx, `
		SELECT id, topic, key, payload
		FROM outbox
		WHERE sent_at IS NULL
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, r.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("select unsent outbox rows: %w", err)
	}
	var (
		records []*kgo.Record
		// ProduceSync reports results in completion order, not input order,
		// so each record is matched back to its row by pointer.
		idOf = make(map[*kgo.Record]uuid.UUID)
	)
	for rows.Next() {
		var (
			id      uuid.UUID
			topic   string
			key     string
			payload []byte
		)
		if err := rows.Scan(&id, &topic, &key, &payload); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan outbox row: %w", err)
		}
		rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: payload}
		records = append(records, rec)
		idOf[rec] = id
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read outbox rows: %w", err)
	}
	if len(records) == 0 {
		return 0, nil
	}

	// ProduceSync keeps the order of records that share a partition, so one
	// order's messages reach Kafka in the order they were written.
	produceCtx, cancel := context.WithTimeout(ctx, r.cfg.ProduceTimeout)
	results := r.client.ProduceSync(produceCtx, records...)
	cancel()

	// Mark only the rows Kafka acknowledged. Failed rows stay unsent and are
	// retried on the next poll.
	sent := make([]uuid.UUID, 0, len(records))
	for _, res := range results {
		if res.Err == nil {
			sent = append(sent, idOf[res.Record])
		}
	}
	if len(sent) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE id = ANY($1)`, sent); err != nil {
			return 0, fmt.Errorf("mark outbox rows sent: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit relay transaction: %w", err)
		}
	}
	if err := results.FirstErr(); err != nil {
		return len(sent), fmt.Errorf("produce %d of %d outbox rows failed: %w", len(records)-len(sent), len(records), err)
	}
	return len(sent), nil
}
