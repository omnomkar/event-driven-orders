package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestHandleWithRetry(t *testing.T) {
	errTransient := errors.New("database briefly unavailable")

	tests := []struct {
		name         string
		results      []error // what the handler returns on each call
		wantCalls    int
		wantFailure  bool
		wantComplete bool
	}{
		{name: "success first try", results: []error{nil}, wantCalls: 1, wantComplete: true},
		{name: "transient then success", results: []error{errTransient, nil}, wantCalls: 2, wantComplete: true},
		{name: "retries exhausted", results: []error{errTransient, errTransient, errTransient}, wantCalls: 3, wantFailure: true, wantComplete: true},
		{name: "permanent skips retries", results: []error{Permanent(errors.New("bad payload"))}, wantCalls: 1, wantFailure: true, wantComplete: true},
		{name: "wrapped permanent is still permanent", results: []error{fmt.Errorf("handle: %w", Permanent(errors.New("bad")))}, wantCalls: 1, wantFailure: true, wantComplete: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			h := func(context.Context, *kgo.Record) error {
				err := tt.results[calls]
				calls++
				return err
			}
			var (
				failed   error
				attempts int
			)
			cfg := ConsumerConfig{
				MaxAttempts: 3,
				Backoff:     func(int) time.Duration { return time.Millisecond },
				OnFailure: func(_ context.Context, _ *kgo.Record, n int, err error) error {
					failed, attempts = err, n
					return nil
				},
			}

			complete := handleWithRetry(context.Background(), &kgo.Record{Key: []byte("order-1")}, h, cfg, discardLogger())

			if calls != tt.wantCalls {
				t.Errorf("handler called %d times, want %d", calls, tt.wantCalls)
			}
			if (failed != nil) != tt.wantFailure {
				t.Errorf("OnFailure called = %v, want %v (err %v)", failed != nil, tt.wantFailure, failed)
			}
			if failed != nil && attempts != tt.wantCalls {
				t.Errorf("OnFailure got attempts = %d, want %d", attempts, tt.wantCalls)
			}
			if complete != tt.wantComplete {
				t.Errorf("complete = %v, want %v", complete, tt.wantComplete)
			}
		})
	}
}

// Each retry waits Backoff(n) for the nth retry: the schedule is driven by
// the attempt number, and the last attempt is not followed by a wait.
func TestHandleWithRetryUsesBackoffSchedule(t *testing.T) {
	var waits []int
	cfg := ConsumerConfig{
		MaxAttempts: 4,
		Backoff: func(n int) time.Duration {
			waits = append(waits, n)
			return time.Millisecond
		},
		OnFailure: func(context.Context, *kgo.Record, int, error) error { return nil },
	}
	h := func(context.Context, *kgo.Record) error { return errors.New("still down") }
	handleWithRetry(context.Background(), &kgo.Record{}, h, cfg, discardLogger())
	if want := []int{1, 2, 3}; !slices.Equal(waits, want) {
		t.Errorf("backoff called for retries %v, want %v", waits, want)
	}
}

// If the failure handler cannot take the record (the dead-letter publish
// fails), it is called again until it succeeds; the record is complete only
// then, so it is never committed without being kept.
func TestHandleWithRetryRetriesFailureHandler(t *testing.T) {
	calls := 0
	cfg := ConsumerConfig{
		MaxAttempts: 1,
		Backoff:     func(int) time.Duration { return time.Millisecond },
		OnFailure: func(context.Context, *kgo.Record, int, error) error {
			calls++
			if calls < 3 {
				return errors.New("dlq unavailable")
			}
			return nil
		},
	}
	h := func(context.Context, *kgo.Record) error { return Permanent(errors.New("bad payload")) }
	if !handleWithRetry(context.Background(), &kgo.Record{}, h, cfg, discardLogger()) {
		t.Error("record not complete after the failure handler succeeded")
	}
	if calls != 3 {
		t.Errorf("failure handler called %d times, want 3", calls)
	}

	// During shutdown a failing handler leaves the record uncommitted.
	ctx, cancel := context.WithCancel(context.Background())
	cfg.OnFailure = func(context.Context, *kgo.Record, int, error) error {
		cancel()
		return errors.New("dlq unavailable")
	}
	if handleWithRetry(ctx, &kgo.Record{}, h, cfg, discardLogger()) {
		t.Error("record reported complete although the failure handler never succeeded")
	}
}

func TestHandleWithRetryStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	h := func(context.Context, *kgo.Record) error {
		calls++
		cancel()
		return errors.New("interrupted")
	}
	cfg := ConsumerConfig{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return time.Hour },
		OnFailure: func(context.Context, *kgo.Record, int, error) error {
			t.Error("OnFailure must not run during shutdown")
			return nil
		},
	}

	if handleWithRetry(ctx, &kgo.Record{}, h, cfg, discardLogger()) {
		t.Error("record reported complete after cancellation; it would be committed without being handled")
	}
	if calls != 1 {
		t.Errorf("handler called %d times, want 1", calls)
	}
}

func TestPermanent(t *testing.T) {
	if Permanent(nil) != nil {
		t.Error("Permanent(nil) should be nil")
	}
	base := errors.New("bad payload")
	err := Permanent(base)
	if !IsPermanent(err) || !errors.Is(err, base) {
		t.Errorf("Permanent(base) = %v: IsPermanent %v, Is(base) %v", err, IsPermanent(err), errors.Is(err, base))
	}
	if IsPermanent(base) {
		t.Error("a plain error is not permanent")
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
