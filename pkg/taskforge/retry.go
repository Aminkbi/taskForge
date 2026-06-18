package taskforge

import (
	"time"

	"github.com/aminkbi/taskforge/internal/tasks"
)

type RetryPolicy struct {
	MaxAttempts    int
	MaxDeliveries  int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
	Jitter         float64
	MaxTaskAge     time.Duration
}

func DefaultRetryPolicy(maxDeliveries int) RetryPolicy {
	return retryPolicyFromInternal(tasks.DefaultRetryPolicy(maxDeliveries))
}

func (p RetryPolicy) toInternal() tasks.RetryPolicy {
	return tasks.RetryPolicy{
		MaxAttempts:    p.MaxAttempts,
		MaxDeliveries:  p.MaxDeliveries,
		InitialBackoff: p.InitialBackoff,
		MaxBackoff:     p.MaxBackoff,
		Multiplier:     p.Multiplier,
		Jitter:         p.Jitter,
		MaxTaskAge:     p.MaxTaskAge,
	}
}

func retryPolicyFromInternal(policy tasks.RetryPolicy) RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    policy.MaxAttempts,
		MaxDeliveries:  policy.MaxDeliveries,
		InitialBackoff: policy.InitialBackoff,
		MaxBackoff:     policy.MaxBackoff,
		Multiplier:     policy.Multiplier,
		Jitter:         policy.Jitter,
		MaxTaskAge:     policy.MaxTaskAge,
	}
}
