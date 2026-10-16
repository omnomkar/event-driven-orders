package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Headers added to a dead-lettered record. The record keeps its original
// key (the order ID) and its original value, byte for byte, so it can be
// inspected and replayed exactly as it was received.
const (
	HeaderError           = "error"
	HeaderAttempts        = "attempts"
	HeaderSourceTopic     = "source_topic"
	HeaderSourcePartition = "source_partition"
	HeaderSourceOffset    = "source_offset"
)

var dlqHeaders = []string{HeaderError, HeaderAttempts, HeaderSourceTopic, HeaderSourcePartition, HeaderSourceOffset}

// dlqPublishTimeout bounds one dead-letter publish. On timeout the consumer
// retries the publish; the source record is not committed until it succeeds.
const dlqPublishTimeout = 10 * time.Second

// DeadLetter returns a FailureHandler that publishes the failed record to
// its topic's dead-letter topic, with headers saying why it failed and where
// it came from. It returns an error if Kafka does not acknowledge the
// publish, so the record is never committed without being kept somewhere.
func DeadLetter(client *kgo.Client, logger *slog.Logger) FailureHandler {
	return func(ctx context.Context, rec *kgo.Record, attempts int, cause error) error {
		dl := &kgo.Record{
			Topic: DLQTopic(rec.Topic),
			Key:   rec.Key,
			Value: rec.Value,
			Headers: append(withoutDLQHeaders(rec.Headers),
				kgo.RecordHeader{Key: HeaderError, Value: []byte(cause.Error())},
				kgo.RecordHeader{Key: HeaderAttempts, Value: []byte(strconv.Itoa(attempts))},
				kgo.RecordHeader{Key: HeaderSourceTopic, Value: []byte(rec.Topic)},
				kgo.RecordHeader{Key: HeaderSourcePartition, Value: []byte(strconv.Itoa(int(rec.Partition)))},
				kgo.RecordHeader{Key: HeaderSourceOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
			),
		}
		pctx, cancel := context.WithTimeout(ctx, dlqPublishTimeout)
		defer cancel()
		if err := client.ProduceSync(pctx, dl).FirstErr(); err != nil {
			return fmt.Errorf("publish to %s: %w", dl.Topic, err)
		}
		logger.Error("record dead-lettered",
			"order_id", string(rec.Key),
			"topic", rec.Topic,
			"partition", rec.Partition,
			"offset", rec.Offset,
			"attempts", attempts,
			"permanent", IsPermanent(cause),
			"dlq", dl.Topic,
			"error", cause)
		return nil
	}
}

// withoutDLQHeaders copies headers, dropping any left by an earlier trip
// through a dead-letter topic, so a replayed record that fails again carries
// only its latest failure.
func withoutDLQHeaders(headers []kgo.RecordHeader) []kgo.RecordHeader {
	return slices.DeleteFunc(slices.Clone(headers), func(h kgo.RecordHeader) bool {
		return slices.Contains(dlqHeaders, h.Key)
	})
}

// header returns the value of the named header, or "".
func header(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
