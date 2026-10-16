//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/omnomkar/event-driven-orders/pkg/kafka"
	"github.com/omnomkar/event-driven-orders/pkg/testinfra"
)

// A consumer with the default failure handler sends a permanently failing
// record straight to the dead-letter topic and a transiently failing one
// there after its last attempt, both with their original bytes and the
// failure headers, and keeps handling the records behind them. Replay then
// publishes the dead-lettered records back to the source topic exactly once.
func TestDeadLetterAndReplay(t *testing.T) {
	source := kafka.TopicInventoryCommands
	dlq := kafka.DLQTopic(source)
	topics := []string{source}
	for _, t := range kafka.Topics {
		topics = append(topics, kafka.DLQTopic(t))
	}
	brokers := testinfra.Kafka(t, topics...)

	var (
		mu      sync.Mutex
		handled []string
		calls   = map[string]int{}
	)
	handler := func(_ context.Context, rec *kgo.Record) error {
		mu.Lock()
		defer mu.Unlock()
		calls[string(rec.Key)]++
		switch string(rec.Value) {
		case "poison":
			return kafka.Permanent(errors.New("cannot decode"))
		case "flaky":
			return errors.New("database unavailable")
		}
		handled = append(handled, string(rec.Key))
		return nil
	}
	c, err := kafka.NewConsumer(
		kafka.Config{Brokers: brokers, ClientID: "dlq-test"},
		kafka.ConsumerConfig{
			Group:       "dlq-test",
			Topics:      []string{source},
			MaxAttempts: 3,
			Backoff:     kafka.Backoff{Base: 10 * time.Millisecond, Cap: 50 * time.Millisecond}.Delay,
		},
		handler, testinfra.Logger())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// One key, so all four records share a partition and keep their order.
	produceValues(t, brokers, source, "order-1", "ok", "poison", "flaky", "ok")
	deadline := time.Now().Add(30 * time.Second)
	for {
		mu.Lock()
		n := len(handled)
		mu.Unlock()
		if n == 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("consumer run: %v", err)
	}
	if len(handled) != 2 {
		t.Fatalf("handled %d good records, want 2 (the one after the failures too)", len(handled))
	}

	dead := testinfra.Consume(t, brokers, 2, 10*time.Second, dlq)
	if len(dead) != 2 {
		t.Fatalf("dead-letter records = %d, want 2", len(dead))
	}
	for i, want := range []struct{ value, attempts string }{{"poison", "1"}, {"flaky", "3"}} {
		r := dead[i]
		h := headers(r)
		if string(r.Value) != want.value || string(r.Key) != "order-1" {
			t.Errorf("dead record %d = %q key %q, want the original %q", i, r.Value, r.Key, want.value)
		}
		if h[kafka.HeaderAttempts] != want.attempts || h[kafka.HeaderSourceTopic] != source ||
			h[kafka.HeaderSourcePartition] == "" || h[kafka.HeaderSourceOffset] == "" || h[kafka.HeaderError] == "" {
			t.Errorf("dead record %d headers = %v, want attempts %s and the source position", i, h, want.attempts)
		}
	}
	if !strings.Contains(headers(dead[0])[kafka.HeaderError], "cannot decode") {
		t.Errorf("error header = %q, want the handler's error", headers(dead[0])[kafka.HeaderError])
	}
	counts, err := kafka.DLQCount(context.Background(), brokers)
	if err != nil || counts[dlq] != 2 {
		t.Errorf("DLQCount = %v, %v; want 2 in %s", counts, err, dlq)
	}

	// Replay sends both back with their original bytes and no DLQ headers.
	// A second replay finds nothing new.
	for i, want := range []int{2, 0} {
		n, err := kafka.ReplayDLQ(context.Background(), brokers, dlq)
		if err != nil || n != want {
			t.Fatalf("replay %d = %d, %v; want %d", i+1, n, err, want)
		}
	}
	all := testinfra.Consume(t, brokers, 6, 10*time.Second, source)
	var values []string
	for _, r := range all {
		values = append(values, string(r.Value))
		if r.Offset >= 4 && len(r.Headers) != 0 {
			t.Errorf("replayed record kept headers %v", r.Headers)
		}
	}
	if want := []string{"ok", "poison", "flaky", "ok", "poison", "flaky"}; !slices.Equal(values, want) {
		t.Errorf("source topic = %v, want %v", values, want)
	}

	if _, err := kafka.ReplayDLQ(context.Background(), brokers, source); err == nil {
		t.Error("replaying a topic that is not a dead-letter topic succeeded")
	}
}

func produceValues(t *testing.T, brokers []string, topic, key string, values ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var recs []*kgo.Record
	for _, v := range values {
		recs = append(recs, &kgo.Record{Topic: topic, Key: []byte(key), Value: []byte(v)})
	}
	if err := cl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

func headers(r *kgo.Record) map[string]string {
	m := make(map[string]string, len(r.Headers))
	for _, h := range r.Headers {
		m[h.Key] = string(h.Value)
	}
	return m
}
