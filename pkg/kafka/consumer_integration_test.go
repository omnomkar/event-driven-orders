//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/testinfra"
)

const topic = "test.commands"

// recorder collects the keys a consumer handled and the keys it gave up on.
type recorder struct {
	mu      sync.Mutex
	handled []string
	failed  []string
}

func (r *recorder) handle(_ context.Context, rec *kgo.Record) error {
	if string(rec.Value) == "poison" {
		return kafka.Permanent(errors.New("cannot decode"))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handled = append(r.handled, string(rec.Key))
	return nil
}

func (r *recorder) onFailure(_ context.Context, rec *kgo.Record, _ int, _ error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed = append(r.failed, string(rec.Key))
	return nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.handled) + len(r.failed)
}

// runUntil runs a consumer in the group until it has seen want records, then
// shuts it down and returns what it saw.
func runUntil(t *testing.T, brokers []string, want int) *recorder {
	t.Helper()
	rec := &recorder{}
	c, err := kafka.NewConsumer(
		kafka.Config{Brokers: brokers, ClientID: "consumer-test"},
		kafka.ConsumerConfig{Group: "test-group", Topics: []string{topic}, OnFailure: rec.onFailure},
		rec.handle, testinfra.Logger())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.Now().Add(30 * time.Second)
	for rec.count() < want && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("consumer run: %v", err)
	}
	return rec
}

func produce(t *testing.T, brokers []string, keys ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var recs []*kgo.Record
	for _, k := range keys {
		value := "ok"
		if k == "order-poison" {
			value = "poison"
		}
		recs = append(recs, &kgo.Record{Topic: topic, Key: []byte(k), Value: []byte(value)})
	}
	if err := cl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// Records handled by one consumer are committed, so a restarted consumer in
// the same group resumes after them instead of handling them again. A record
// that fails permanently goes to OnFailure once and is committed too.
func TestConsumerCommitsHandledRecords(t *testing.T) {
	brokers := testinfra.Kafka(t, topic)

	var first []string
	for i := range 5 {
		first = append(first, fmt.Sprintf("order-%d", i))
	}
	produce(t, brokers, append(first, "order-poison")...)

	r1 := runUntil(t, brokers, 6)
	if len(r1.handled) != 5 || len(r1.failed) != 1 || r1.failed[0] != "order-poison" {
		t.Fatalf("first run handled %v, failed %v; want 5 handled and order-poison failed", r1.handled, r1.failed)
	}

	produce(t, brokers, "order-new")
	r2 := runUntil(t, brokers, 1)
	if len(r2.handled) != 1 || r2.handled[0] != "order-new" || len(r2.failed) != 0 {
		t.Errorf("restarted consumer handled %v, failed %v; want only order-new", r2.handled, r2.failed)
	}
}
