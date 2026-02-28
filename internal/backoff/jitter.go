package backoff

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// RetryWithJitter implements Full Jitter exponential backoff (AWS recommended).
// sleep = random_between(0, min(cap, base * 2^attempt))
// Base: 1s, Cap: 30s.
func RetryWithJitter[T any](ctx context.Context, maxAttempts int, fn func() (T, error)) (T, error) {
	const (
		base = 1 * time.Second
		cap  = 30 * time.Second
	)

	var zero T
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		result, err := fn()
		if err == nil {
			return result, nil
		}
		lastErr = err

		if attempt == maxAttempts-1 {
			break
		}

		expBackoff := time.Duration(float64(base) * math.Pow(2, float64(attempt)))
		if expBackoff > cap {
			expBackoff = cap
		}
		jitter := time.Duration(rand.Int63n(int64(expBackoff)))

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(jitter):
		}
	}

	return zero, lastErr
}
