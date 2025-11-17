package consumer

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// OrPauseErrors returns a PauseDecider that matches if error is any of the provided targets.
func OrPauseErrors(targets ...error) PausePredicate {
	return func(err error) bool {
		for _, t := range targets {
			if errors.Is(err, t) {
				return true
			}
		}
		return false
	}
}

// safeCloseReader closes the reader if it is non-nil and not a typed-nil value.
func safeCloseReader(r Reader) {
	if r == nil {
		return
	}

	defer func() { _ = recover() }()
	r.Close()
}

// applyJitter returns a duration randomized within +/-20% of the base duration.
func applyJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}

	// jitter in [-0.2, +0.2]
	const jitterFactor = 0.2
	// rand.Float64() in [0,1); shift to [-0.5, +0.5) then scale
	delta := (rand.Float64() - 0.5) * 2 * jitterFactor
	jittered := float64(d) * (1 + delta)

	if jittered < float64(time.Millisecond) {
		jittered = float64(time.Millisecond)
	}

	return time.Duration(jittered)
}

// TIDFromCtx retrieves a trace id from context if present
func TIDFromCtx(ctx context.Context) string {
	if v := ctx.Value(TraceIDKey); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
