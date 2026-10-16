package kafka

import (
	"testing"
	"time"
)

func TestBackoffCeilingDoublesUpToCap(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Cap: 5 * time.Second}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond}, // treated as the first retry
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{6, 3200 * time.Millisecond},
		{7, 5 * time.Second}, // 6.4s, capped
		{1000, 5 * time.Second},
	}
	for _, tt := range tests {
		if got := b.Ceiling(tt.attempt); got != tt.want {
			t.Errorf("Ceiling(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

// With an injected random source the delay is exactly r * ceiling, so the
// jitter can be checked without flakiness.
func TestBackoffDelayIsJitteredBelowCeiling(t *testing.T) {
	tests := []struct {
		name    string
		r       float64
		attempt int
		want    time.Duration
	}{
		{"zero draw waits nothing", 0, 3, 0},
		{"half draw waits half the ceiling", 0.5, 3, 200 * time.Millisecond},
		{"quarter draw on the first retry", 0.25, 1, 25 * time.Millisecond},
		{"half draw at the cap", 0.5, 10, 2500 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Backoff{Base: 100 * time.Millisecond, Cap: 5 * time.Second, Rand: func() float64 { return tt.r }}
			if got := b.Delay(tt.attempt); got != tt.want {
				t.Errorf("Delay(%d) with r=%v = %v, want %v", tt.attempt, tt.r, got, tt.want)
			}
		})
	}
}

// The default source stays inside [0, ceiling) for every attempt.
func TestBackoffDelayDefaultSourceInRange(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Cap: 5 * time.Second}
	for attempt := 1; attempt <= 10; attempt++ {
		for range 100 {
			if d := b.Delay(attempt); d < 0 || d >= b.Ceiling(attempt) {
				t.Fatalf("Delay(%d) = %v, outside [0, %v)", attempt, d, b.Ceiling(attempt))
			}
		}
	}
}
