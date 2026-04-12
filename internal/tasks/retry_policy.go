package tasks

import (
	"math"
	"time"
)

type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

func DefaultRetryPolicy(maxAttempts int) RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    maxAttempts,
		InitialBackoff: time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2,
	}
}

func (p RetryPolicy) ShouldRetry(attempt int) bool {
	if p.MaxAttempts <= 0 {
		return false
	}
	return attempt < p.MaxAttempts
}

func (p RetryPolicy) NextDelay(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	if p.InitialBackoff <= 0 {
		p.InitialBackoff = time.Second
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = 30 * time.Second
	}
	if p.Multiplier < 1 {
		p.Multiplier = 1
	}

	backoff := float64(p.InitialBackoff) * math.Pow(p.Multiplier, float64(attempt-1))
	delay := time.Duration(backoff)
	if delay > p.MaxBackoff {
		return p.MaxBackoff
	}
	return delay
}
