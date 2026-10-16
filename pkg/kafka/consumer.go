package kafka

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/omnomkar/event-driven-orders/pkg/config"
)

// Handler processes one record. It should do all of its work inside one
// database transaction and return only after that transaction has committed
// (or return an error, in which case nothing it did may be kept). The record's
// offset is committed only after Handler returns nil, which makes consumption
// at-least-once: a crash before the offset commit redelivers the record.
type Handler func(ctx context.Context, rec *kgo.Record) error

// FailureHandler is called for a record that failed permanently or ran out of
// attempts. The record's offset is committed only after it returns nil, so it
// must not drop the record silently: if it returns an error, the consumer
// calls it again (with backoff) until it succeeds or the consumer shuts down.
type FailureHandler func(ctx context.Context, rec *kgo.Record, attempts int, err error) error

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
	// to OnFailure. Default 5.
	MaxAttempts int
	// Backoff returns the wait before retry number attempt (1 for the first
	// retry). Default: exponential with full jitter, base 100ms, cap 5s.
	Backoff func(attempt int) time.Duration
	// OnFailure receives records that failed permanently or ran out of
	// attempts. Default: DeadLetter, which publishes them to Topic + ".dlq".
	OnFailure FailureHandler
	// SessionTimeout is how long the group waits for a silent member before
	// giving its partitions to others. A shorter timeout means a crashed
	// instance's partitions move sooner. Default: franz-go's 45s.
	SessionTimeout time.Duration
}

// Default retry settings, used when ConsumerConfig leaves them unset.
const (
	DefaultMaxAttempts = 5
	DefaultRetryBase   = 100 * time.Millisecond
	DefaultRetryCap    = 5 * time.Second
)

// ConsumerSettings reads the retry and group settings every consumer shares
// from the environment: CONSUMER_MAX_ATTEMPTS, CONSUMER_RETRY_BASE,
// CONSUMER_RETRY_CAP, and KAFKA_SESSION_TIMEOUT. The caller sets Group and
// Topics.
func ConsumerSettings(l *config.Loader) ConsumerConfig {
	b := Backoff{
		Base: l.Duration("CONSUMER_RETRY_BASE", DefaultRetryBase),
		Cap:  l.Duration("CONSUMER_RETRY_CAP", DefaultRetryCap),
	}
	return ConsumerConfig{
		MaxAttempts:    l.Int("CONSUMER_MAX_ATTEMPTS", DefaultMaxAttempts),
		Backoff:        b.Delay,
		SessionTimeout: l.Duration("KAFKA_SESSION_TIMEOUT", 0),
	}
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

	opts := []kgo.Opt{
		kgo.ConsumerGroup(cc.Group),
		kgo.ConsumeTopics(cc.Topics...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Hold off rebalances between a poll and its commit, so a partition
		// cannot move to another member while its records are being handled.
		kgo.BlockRebalanceOnPoll(),
	}
	if cc.SessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(cc.SessionTimeout))
	}
	client, err := NewClient(cfg, opts...)
	if err != nil {
		return nil, err
	}
	cc = withDefaults(cc)
	if cc.OnFailure == nil {
		// The same client consumes the source topic and produces to its
		// dead-letter topic.
		cc.OnFailure = DeadLetter(client, logger)
	}
	return &Consumer{client: client, cfg: cc, handler: h, logger: logger}, nil
}

func withDefaults(cc ConsumerConfig) ConsumerConfig {
	if cc.MaxAttempts <= 0 {
		cc.MaxAttempts = DefaultMaxAttempts
	}
	if cc.Backoff == nil {
		cc.Backoff = Backoff{Base: DefaultRetryBase, Cap: DefaultRetryCap}.Delay
	}
	return cc
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
//
// Retries happen in place: while a record waits for its next attempt, the
// records behind it on the same partition wait too. That keeps one order's
// messages in order, at the cost of blocking the partition during backoff.
func handleWithRetry(ctx context.Context, rec *kgo.Record, h Handler, cfg ConsumerConfig, logger *slog.Logger) bool {
	attempt := 0
	var err error
	for {
		attempt++
		err = h(ctx, rec)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		if IsPermanent(err) || attempt >= cfg.MaxAttempts {
			break
		}
		logger.Warn("record failed, retrying",
			"order_id", string(rec.Key), "topic", rec.Topic, "attempt", attempt, "error", err)
		if !sleep(ctx, cfg.Backoff(attempt)) {
			return false
		}
	}

	// The record is done with retries. It may be committed only once the
	// failure handler has taken it, so a failing dead-letter publish is
	// retried rather than skipped.
	for try := 1; ; try++ {
		ferr := cfg.OnFailure(ctx, rec, attempt, err)
		if ferr == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		logger.Error("failure handler failed, retrying",
			"order_id", string(rec.Key), "topic", rec.Topic, "offset", rec.Offset, "error", ferr)
		if !sleep(ctx, cfg.Backoff(try)) {
			return false
		}
	}
}

// sleep waits for d and reports whether it finished before ctx was cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
