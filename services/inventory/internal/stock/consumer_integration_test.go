//go:build integration

package stock

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/testinfra"
	commonv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/common/v1"
	eventsv1 "github.com/omnomkar/event-driven-orders/proto/gen/go/eventorders/events/v1"
	"github.com/omnomkar/event-driven-orders/services/inventory/migrations"
)

// The real inventory consumer, with its default failure handler, meets an
// undecodable record followed by a good ReserveStock on the same partition.
// The bad record lands in inventory.commands.dlq with its original bytes and
// failure headers, and the good one behind it is still processed.
func TestPoisonMessageGoesToDLQAndConsumerContinues(t *testing.T) {
	pool := testinfra.Postgres(t, migrations.FS)
	dlq := kafka.DLQTopic(kafka.TopicInventoryCommands)
	brokers := testinfra.Kafka(t, kafka.TopicInventoryCommands, dlq)
	store := NewStore(pool)

	c, err := kafka.NewConsumer(kafka.Config{Brokers: brokers, ClientID: "inventory-test"},
		kafka.ConsumerConfig{Group: "inventory", Topics: []string{kafka.TopicInventoryCommands}},
		store.CommandHandler(testinfra.Logger()), testinfra.Logger())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	orderID := uuid.NewString()
	poison := []byte("\xff\xfenot protobuf")
	good, err := proto.Marshal(&eventsv1.InventoryCommand{
		MessageId: uuid.NewString(),
		OrderId:   orderID,
		Command: &eventsv1.InventoryCommand_ReserveStock{ReserveStock: &eventsv1.ReserveStock{
			Items: []*commonv1.LineItem{{ProductId: "sku-cable", Quantity: 2}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	// Same key, so both records share a partition and the good one is
	// queued behind the poison one.
	if err := producer.ProduceSync(context.Background(),
		&kgo.Record{Topic: kafka.TopicInventoryCommands, Key: []byte(orderID), Value: poison},
		&kgo.Record{Topic: kafka.TopicInventoryCommands, Key: []byte(orderID), Value: good},
	).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for count(t, pool, `SELECT count(*) FROM reservations WHERE order_id = $1`, orderID) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the good ReserveStock behind the poison record was never processed")
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("consumer run: %v", err)
	}

	dead := testinfra.Consume(t, brokers, 1, 10*time.Second, dlq)
	if len(dead) != 1 {
		t.Fatalf("dead-letter records = %d, want 1", len(dead))
	}
	r := dead[0]
	if !bytes.Equal(r.Value, poison) || string(r.Key) != orderID {
		t.Errorf("dead record value %q key %q, want the original bytes and key", r.Value, r.Key)
	}
	h := map[string]string{}
	for _, hd := range r.Headers {
		h[hd.Key] = string(hd.Value)
	}
	if h[kafka.HeaderSourceTopic] != kafka.TopicInventoryCommands || h[kafka.HeaderAttempts] != "1" ||
		h[kafka.HeaderSourceOffset] != "0" || h[kafka.HeaderSourcePartition] == "" || h[kafka.HeaderError] == "" {
		t.Errorf("dead record headers = %v; want source topic, partition, offset 0, 1 attempt, and the error", h)
	}
}
