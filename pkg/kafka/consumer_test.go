package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
			var failed error
			cfg := ConsumerConfig{
				MaxAttempts: 3,
				Backoff:     func(int) time.Duration { return time.Millisecond },
				OnFailure:   func(_ context.Context, _ *kgo.Record, err error) { failed = err },
			}

			complete := handleWithRetry(context.Background(), &kgo.Record{Key: []byte("order-1")}, h, cfg, discardLogger())

			if calls != tt.wantCalls {
				t.Errorf("handler called %d times, want %d", calls, tt.wantCalls)
			}
			if (failed != nil) != tt.wantFailure {
				t.Errorf("OnFailure called = %v, want %v (err %v)", failed != nil, tt.wantFailure, failed)
			}
			if complete != tt.wantComplete {
				t.Errorf("complete = %v, want %v", complete, tt.wantComplete)
			}
		})
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
		OnFailure:   func(context.Context, *kgo.Record, error) { t.Error("OnFailure must not run during shutdown") },
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
