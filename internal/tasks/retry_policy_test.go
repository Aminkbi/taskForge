package tasks

import (
	"testing"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
)

func TestRetryPolicyShouldRetry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	firstEnqueuedAt := now.Add(-time.Minute)

	testCases := []struct {
		name          string
		policy        RetryPolicy
		deliveryCount int
		firstQueuedAt time.Time
		want          bool
	}{
		{name: "disabled when max deliveries is zero", policy: RetryPolicy{}, deliveryCount: 1, firstQueuedAt: firstEnqueuedAt, want: false},
		{name: "allows retry before delivery cap", policy: DefaultRetryPolicy(3), deliveryCount: 1, firstQueuedAt: firstEnqueuedAt, want: true},
		{name: "stops at delivery cap", policy: DefaultRetryPolicy(3), deliveryCount: 3, firstQueuedAt: firstEnqueuedAt, want: false},
		{name: "stops at max task age", policy: RetryPolicy{MaxDeliveries: 5, MaxTaskAge: 30 * time.Second}, deliveryCount: 1, firstQueuedAt: now.Add(-time.Minute), want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.policy.ShouldRetry(tc.deliveryCount, tc.firstQueuedAt, now); got != tc.want {
				t.Fatalf("ShouldRetry(%d) = %v, want %v", tc.deliveryCount, got, tc.want)
			}
		})
	}
}

func TestRetryPolicyNextDelay(t *testing.T) {
	t.Parallel()

	policy := RetryPolicy{
		MaxDeliveries:  5,
		InitialBackoff: time.Second,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2,
	}

	msg := broker.TaskMessage{ID: "task-1"}
	testCases := []struct {
		deliveryCount int
		want          time.Duration
	}{
		{deliveryCount: 1, want: time.Second},
		{deliveryCount: 2, want: 2 * time.Second},
		{deliveryCount: 3, want: 4 * time.Second},
		{deliveryCount: 4, want: 5 * time.Second},
	}

	for _, tc := range testCases {
		t.Run(tc.want.String(), func(t *testing.T) {
			if got := policy.NextDelay(msg, tc.deliveryCount); got != tc.want {
				t.Fatalf("NextDelay(%d) = %v, want %v", tc.deliveryCount, got, tc.want)
			}
		})
	}
}

func TestRetryPolicyJitterWithinBounds(t *testing.T) {
	t.Parallel()

	policy := RetryPolicy{
		MaxDeliveries:  5,
		InitialBackoff: time.Second,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2,
		Jitter:         0.25,
	}

	delay := policy.NextDelay(broker.TaskMessage{ID: "task-1"}, 2)
	base := 2 * time.Second
	min := time.Duration(float64(base) * 0.75)
	max := time.Duration(float64(base) * 1.25)
	if delay < min || delay > max {
		t.Fatalf("jittered delay = %v, want within [%v, %v]", delay, min, max)
	}
}

func TestResolveRetryPolicyHeaders(t *testing.T) {
	t.Parallel()

	msg := broker.TaskMessage{
		ID: "task-1",
		Headers: map[string]string{
			HeaderRetryMaxDeliveries:  "5",
			HeaderRetryInitialBackoff: "2s",
			HeaderRetryMaxBackoff:     "20s",
			HeaderRetryMultiplier:     "3",
			HeaderRetryJitter:         "0.1",
			HeaderRetryMaxTaskAge:     "1m",
		},
	}

	policy, err := ResolveRetryPolicy(DefaultRetryPolicy(3), msg)
	if err != nil {
		t.Fatalf("ResolveRetryPolicy() error = %v", err)
	}

	if policy.MaxDeliveries != 5 || policy.InitialBackoff != 2*time.Second || policy.MaxBackoff != 20*time.Second || policy.Multiplier != 3 || policy.Jitter != 0.1 || policy.MaxTaskAge != time.Minute {
		t.Fatalf("ResolveRetryPolicy() = %+v", policy)
	}
}
