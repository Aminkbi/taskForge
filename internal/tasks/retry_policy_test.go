package tasks

import (
	"testing"
	"time"
)

func TestRetryPolicyShouldRetry(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		maxAttempts int
		attempt     int
		want        bool
	}{
		{name: "disabled when max attempts is zero", maxAttempts: 0, attempt: 1, want: false},
		{name: "allows first retry", maxAttempts: 3, attempt: 1, want: true},
		{name: "allows last retry", maxAttempts: 3, attempt: 3, want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			policy := DefaultRetryPolicy(tc.maxAttempts)
			if got := policy.ShouldRetry(tc.attempt); got != tc.want {
				t.Fatalf("ShouldRetry(%d) = %v, want %v", tc.attempt, got, tc.want)
			}
		})
	}
}

func TestRetryPolicyNextDelay(t *testing.T) {
	t.Parallel()

	policy := RetryPolicy{
		MaxAttempts:    5,
		InitialBackoff: time.Second,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2,
	}

	testCases := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 4 * time.Second},
		{attempt: 4, want: 5 * time.Second},
	}

	for _, tc := range testCases {
		t.Run(tc.want.String(), func(t *testing.T) {
			if got := policy.NextDelay(tc.attempt); got != tc.want {
				t.Fatalf("NextDelay(%d) = %v, want %v", tc.attempt, got, tc.want)
			}
		})
	}
}
