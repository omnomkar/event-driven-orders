package kafka

import (
	"math/rand/v2"
	"time"
)

// Backoff computes retry delays with exponential growth and full jitter.
//
// Retry n waits a random duration in [0, min(Cap, Base * 2^(n-1))). The
// ceiling doubles with each retry, so a struggling dependency gets more and
// more breathing room, and the randomness ("full jitter") spreads retries out
// so many consumers that failed at the same moment do not all retry at the
// same moment too.
type Backoff struct {
	Base time.Duration
	Cap  time.Duration
	// Rand returns a value in [0, 1). Tests inject a fixed source; nil uses
	// math/rand/v2.
	Rand func() float64
}

// Ceiling returns the upper bound of the delay before retry number attempt
// (1 for the first retry): min(Cap, Base * 2^(attempt-1)).
func (b Backoff) Ceiling(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := b.Base
	// Double until reaching the cap. Stopping at the cap, instead of
	// computing 2^(attempt-1) directly, means a large attempt number can
	// never overflow.
	for i := 1; i < attempt && d < b.Cap; i++ {
		d *= 2
	}
	return min(d, b.Cap)
}

// Delay returns a random wait before retry number attempt.
func (b Backoff) Delay(attempt int) time.Duration {
	r := b.Rand
	if r == nil {
		r = rand.Float64
	}
	return time.Duration(r() * float64(b.Ceiling(attempt)))
}
