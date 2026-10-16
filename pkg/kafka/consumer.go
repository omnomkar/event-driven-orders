package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Handler processes one record. It should do all of its work inside one
// database transaction and return only after that transaction has committed
// (or return an error, in which case nothing it did may be kept). The record's
// offset is committed only after Handler returns nil, which makes consumption
// at-least-once: a crash before the offset commit redelivers the record.
type Handler func(ctx context.Context, rec *kgo.Record) error

// FailureHandler is called for a record that failed permanently or ran out of
// retries. After it returns, the record's offset is committed and the
// consumer moves on, so it must not drop the record silently.
type FailureHandler func(ctx context.Context, rec *kgo.Record, err error)

// permanentError marks an error that retrying cannot fix, such as a payload
// that does not decode or an event that is not valid in the order's state.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err so the consumer skips the remaining retries and passes
// the record straight to its FailureHandler.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err, or any error it wraps, came from Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// ConsumerConfig configures a consumer group member. Zero values take the
// defaults.
type ConsumerConfig struct {
	Group  string
	Topics []string
	// MaxAttempts is how many times a record is handled before it is passed
	// to OnFailure. Default 3.
	MaxAttempts int
	// Backoff returns the wait before retry number attempt (1 for the first
	// retry). Default: a constant 100ms.
	Backoff func(attempt int) time.Duration
	// OnFailure receives records that failed permanently or ran out of
	// attempts. Default: LogFailure.
	OnFailure FailureHandler
}

// Consumer reads records from a consumer group and hands each one to a
// Handler, one at a time, in partition order.
type Consumer struct {
	client  *kgo.Client
	cfg     ConsumerConfig
	handler Handler
	logger  *slog.Logger
}

// NewConsumer joins cc.Group with auto-commit disabled. A new group starts
// from the earliest offset, so messages produced before the service first
// started are not skipped.
func NewConsumer(cfg Config, cc ConsumerConfig, h Handler, logger *slog.Logger) (*Consumer, error) {
	if cc.Group == "" || len(cc.Topics) == 0 {
		return nil, errors.New("consumer needs a group and at least one topic")
	}
	logger = logger.With("component", "consumer", "group", cc.Group)
	cc = withDefaults(cc, logger)

	client, err := NewClient(cfg,
		kgo.ConsumerGroup(cc.Group),
		kgo.ConsumeTopics(cc.Topics...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Hold off rebalances between a poll and its commit, so a partition
		// cannot move to another member while its records are being handled.
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: client, cfg: cc, handler: h, logger: logger}, nil
}

func withDefaults(cc ConsumerConfig, logger *slog.Logger) ConsumerConfig {
	if cc.MaxAttempts <= 0 {
		cc.MaxAttempts = 3
	}
	if cc.Backoff == nil {
		// TODO(stage 3): exponential backoff with jitter.
		cc.Backoff = func(int) time.Duration { return 100 * time.Millisecond }
	}
	if cc.OnFailure == nil {
		// TODO(stage 3): replace LogFailure with a handler that publishes the
		// record to rec.Topic + ".dlq", so nothing is dropped. This is the
		// single place every service's failed records go through.
		cc.OnFailure = LogFailure(logger)
	}
	return cc
}

// LogFailure returns a FailureHandler that logs the failed record. It is a
// placeholder until the dead-letter topic exists: the record is logged with
// its position so it can be found and replayed by hand.
func LogFailure(logger *slog.Logger) FailureHandler {
	return func(_ context.Context, rec *kgo.Record, err error) {
		logger.Error("record failed, skipping",
			"order_id", string(rec.Key),
			"topic", rec.Topic,
			"partition", rec.Partition,
			"offset", rec.Offset,
			"permanent", IsPermanent(err),
			"error", err)
	}
}

// Client returns the underlying franz-go client, for readiness checks.
func (c *Consumer) Client() *kgo.Client { return c.client }

// Close leaves the group and closes the client.
func (c *Consumer) Close() { c.client.Close() }

// Run polls and handles records until ctx is cancelled. Offsets are committed
// after every handled batch, and only for records that were handled.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			c.client.AllowRebalance()
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.logger.Warn("fetch error", "topic", topic, "partition", partition, "error", err)
		})

		var done []*kgo.Record
		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			if !c.process(ctx, rec) {
				break // shutting down; leave the rest uncommitted
			}
			done = append(done, rec)
		}

		if len(done) > 0 {
			// Commit even during shutdown, so handled records are not redelivered.
			commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			if err := c.client.CommitRecords(commitCtx, done...); err != nil {
				c.logger.Error("commit offsets failed; records will be redelivered", "error", err)
			}
			cancel()
		}
		c.client.AllowRebalance()
	}
}

// process handles rec with retries. It returns false only if ctx was
// cancelled before the record was finished, in which case the record must not
// be committed.
func (c *Consumer) process(ctx context.Context, rec *kgo.Record) bool {
	return handleWithRetry(ctx, rec, c.handler, c.cfg, c.logger)
}

// handleWithRetry is the retry policy, separate from Kafka so it can be unit
// tested with plain records.
func handleWithRetry(ctx context.Context, rec *kgo.Record, h Handler, cfg ConsumerConfig, logger *slog.Logger) bool {
	attempt := 0
	for {
		attempt++
		err := h(ctx, rec)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		if IsPermanent(err) || attempt >= cfg.MaxAttempts {
			cfg.OnFailure(ctx, rec, fmt.Errorf("after %d attempt(s): %w", attempt, err))
			return true
		}
		logger.Warn("record failed, retrying",
			"order_id", string(rec.Key), "topic", rec.Topic, "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(cfg.Backoff(attempt)):
		}
	}
}
