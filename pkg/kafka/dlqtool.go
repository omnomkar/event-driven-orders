package kafka

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/omnomkar/event-driven-orders/pkg/config"
)

// ReplayGroup is the consumer group whose committed offsets record how far
// each dead-letter topic has been replayed, so a second replay does not
// publish the same records again.
const ReplayGroup = "dlq-replay"

// DLQCount returns how many records each dead-letter topic holds.
func DLQCount(ctx context.Context, brokers []string) (map[string]int64, error) {
	adm, closeFn, err := admin(brokers)
	if err != nil {
		return nil, err
	}
	defer closeFn()
	out := make(map[string]int64)
	for _, src := range Topics {
		topic := DLQTopic(src)
		starts, ends, err := bounds(ctx, adm, topic)
		if err != nil {
			return nil, err
		}
		for p, end := range ends {
			out[topic] += end - starts[p]
		}
	}
	return out, nil
}

// ListDLQ writes every record in every dead-letter topic to w, one line each:
// position, order ID, why it failed, and where it came from. Values are
// Protobuf, so only their size is printed.
func ListDLQ(ctx context.Context, brokers []string, w io.Writer) error {
	adm, closeFn, err := admin(brokers)
	if err != nil {
		return err
	}
	defer closeFn()
	for _, src := range Topics {
		topic := DLQTopic(src)
		recs, _, err := readFrom(ctx, adm, brokers, topic, nil)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(w, "%s: %d record(s)\n", topic, len(recs))
		for _, r := range recs {
			_, _ = fmt.Fprintf(w, "  %d/%d order_id=%s attempts=%s source=%s/%s/%s bytes=%d error=%q\n",
				r.Partition, r.Offset, r.Key, header(r, HeaderAttempts),
				header(r, HeaderSourceTopic), header(r, HeaderSourcePartition), header(r, HeaderSourceOffset),
				len(r.Value), header(r, HeaderError))
		}
	}
	return nil
}

// ReplayDLQ publishes the records in dlqTopic that have not been replayed yet
// back to their source topic, with their original key and value, and then
// records them as replayed. It returns how many it published.
//
// Replay is safe because every consumer deduplicates by message_id: a
// record whose effects were in fact applied before it failed is skipped, and
// replaying the same record twice (after a crash between publishing and
// recording) has no extra effect.
func ReplayDLQ(ctx context.Context, brokers []string, dlqTopic string) (int, error) {
	source, ok := strings.CutSuffix(dlqTopic, dlqSuffix)
	if !ok || !slices.Contains(Topics, source) {
		return 0, fmt.Errorf("%q is not a dead-letter topic", dlqTopic)
	}
	adm, closeFn, err := admin(brokers)
	if err != nil {
		return 0, err
	}
	defer closeFn()

	committed, err := adm.FetchOffsets(ctx, ReplayGroup)
	if err != nil {
		return 0, fmt.Errorf("fetch replay offsets: %w", err)
	}
	from := make(map[int32]int64)
	committed.Each(func(o kadm.OffsetResponse) {
		if o.Topic == dlqTopic && o.Err == nil {
			from[o.Partition] = o.At
		}
	})
	recs, ends, err := readFrom(ctx, adm, brokers, dlqTopic, from)
	if err != nil {
		return 0, err
	}

	if len(recs) > 0 {
		out := make([]*kgo.Record, len(recs))
		for i, r := range recs {
			out[i] = &kgo.Record{Topic: source, Key: r.Key, Value: r.Value, Headers: withoutDLQHeaders(r.Headers)}
		}
		producer, err := NewClient(Config{Brokers: brokers, ClientID: "dlq-replay"})
		if err != nil {
			return 0, err
		}
		err = producer.ProduceSync(ctx, out...).FirstErr()
		producer.Close()
		if err != nil {
			return 0, fmt.Errorf("publish to %s: %w", source, err)
		}
	}

	var done kadm.Offsets
	for p, end := range ends {
		done.Add(kadm.Offset{Topic: dlqTopic, Partition: p, At: end, LeaderEpoch: -1})
	}
	resp, err := adm.CommitOffsets(ctx, ReplayGroup, done)
	if err == nil {
		err = resp.Error()
	}
	if err != nil {
		return len(recs), fmt.Errorf("record replay progress (records were published): %w", err)
	}
	return len(recs), nil
}

// DLQMain runs the dlq subcommand and returns a process exit code:
//
//	dlq count           total records across every dead-letter topic
//	dlq list            every dead-letter record, one per line
//	dlq replay <topic>  publish <topic>'s unreplayed records to its source topic
//
// It reads KAFKA_BROKERS from the environment. Services run it inside their
// containers, because Kafka is reachable only on the Compose network and the
// distroless images have no other tools.
func DLQMain(args []string, stdout, stderr io.Writer) int {
	cfg := config.New()
	brokers := cfg.RequiredList("KAFKA_BROKERS")
	if err := cfg.Err(); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var err error
	switch {
	case len(args) == 1 && args[0] == "count":
		var counts map[string]int64
		if counts, err = DLQCount(ctx, brokers); err == nil {
			var total int64
			for _, n := range counts {
				total += n
			}
			_, _ = fmt.Fprintln(stdout, total)
		}
	case len(args) == 1 && args[0] == "list":
		err = ListDLQ(ctx, brokers, stdout)
	case len(args) == 2 && args[0] == "replay":
		var n int
		if n, err = ReplayDLQ(ctx, brokers, args[1]); err == nil {
			_, _ = fmt.Fprintf(stdout, "replayed %d record(s) from %s\n", n, args[1])
		}
	default:
		_, _ = fmt.Fprintln(stderr, "usage: dlq count | dlq list | dlq replay <topic>.dlq")
		return 2
	}
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "dlq:", err)
		return 1
	}
	return 0
}

func admin(brokers []string) (*kadm.Client, func(), error) {
	cl, err := NewClient(Config{Brokers: brokers, ClientID: "dlq-tool"})
	if err != nil {
		return nil, nil, err
	}
	return kadm.NewClient(cl), cl.Close, nil
}

// bounds returns each partition's first and next-to-be-written offsets.
func bounds(ctx context.Context, adm *kadm.Client, topic string) (starts, ends map[int32]int64, err error) {
	s, err := adm.ListStartOffsets(ctx, topic)
	if err == nil {
		err = s.Error()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("list start offsets of %s: %w", topic, err)
	}
	e, err := adm.ListEndOffsets(ctx, topic)
	if err == nil {
		err = e.Error()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("list end offsets of %s: %w", topic, err)
	}
	starts, ends = make(map[int32]int64), make(map[int32]int64)
	s.Each(func(o kadm.ListedOffset) { starts[o.Partition] = o.Offset })
	e.Each(func(o kadm.ListedOffset) { ends[o.Partition] = o.Offset })
	return starts, ends, nil
}

// readFrom reads topic from the given offset in each partition (or the
// start, if none is given) up to the end offsets at the time of the call,
// which it also returns. Records written after the call are not read.
func readFrom(ctx context.Context, adm *kadm.Client, brokers []string, topic string, from map[int32]int64) ([]*kgo.Record, map[int32]int64, error) {
	starts, ends, err := bounds(ctx, adm, topic)
	if err != nil {
		return nil, nil, err
	}
	assign := make(map[int32]kgo.Offset)
	var want int64
	for p, end := range ends {
		start := max(starts[p], from[p])
		if start < end {
			assign[p] = kgo.NewOffset().At(start)
			want += end - start
		}
	}
	if want == 0 {
		return nil, ends, nil
	}

	cl, err := NewClient(Config{Brokers: brokers, ClientID: "dlq-tool"},
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: assign}))
	if err != nil {
		return nil, nil, err
	}
	defer cl.Close()
	var recs []*kgo.Record
	for int64(len(recs)) < want {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, nil, fmt.Errorf("read %s: got %d of %d records: %w", topic, len(recs), want, err)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, nil, fmt.Errorf("read %s: %w", topic, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Offset < ends[r.Partition] {
				recs = append(recs, r)
			}
		})
	}
	slices.SortFunc(recs, func(a, b *kgo.Record) int {
		if a.Partition != b.Partition {
			return int(a.Partition - b.Partition)
		}
		return int(a.Offset - b.Offset)
	})
	return recs, ends, nil
}
