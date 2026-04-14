package tasks

import (
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
)

const (
	HeaderRetryMaxDeliveries   = "taskforge_retry_max_deliveries"
	HeaderRetryInitialBackoff  = "taskforge_retry_initial_backoff"
	HeaderRetryMaxBackoff      = "taskforge_retry_max_backoff"
	HeaderRetryMultiplier      = "taskforge_retry_multiplier"
	HeaderRetryJitter          = "taskforge_retry_jitter"
	HeaderRetryMaxTaskAge      = "taskforge_retry_max_task_age"
	HeaderRetryScheduledAt     = "taskforge_retry_scheduled_at"
	HeaderRetryDelay           = "taskforge_retry_delay"
	HeaderRetryFailureClass    = "taskforge_retry_failure_class"
	HeaderRetryDeliveryCount   = "taskforge_retry_delivery_count"
	HeaderRetryFirstEnqueuedAt = "taskforge_retry_first_enqueued_at"
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
	return RetryPolicy{
		MaxAttempts:    maxDeliveries,
		MaxDeliveries:  maxDeliveries,
		InitialBackoff: time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2,
		Jitter:         0,
	}
}

func ResolveRetryPolicy(base RetryPolicy, msg broker.TaskMessage) (RetryPolicy, error) {
	policy := base.normalized()
	if msg.MaxAttempts > 0 {
		policy.MaxAttempts = msg.MaxAttempts
		policy.MaxDeliveries = msg.MaxAttempts
	}
	if msg.Headers == nil {
		return policy, nil
	}

	if err := applyIntHeader(msg.Headers, HeaderRetryMaxDeliveries, &policy.MaxDeliveries); err != nil {
		return RetryPolicy{}, err
	}
	if err := applyDurationHeader(msg.Headers, HeaderRetryInitialBackoff, &policy.InitialBackoff); err != nil {
		return RetryPolicy{}, err
	}
	if err := applyDurationHeader(msg.Headers, HeaderRetryMaxBackoff, &policy.MaxBackoff); err != nil {
		return RetryPolicy{}, err
	}
	if err := applyFloatHeader(msg.Headers, HeaderRetryMultiplier, &policy.Multiplier); err != nil {
		return RetryPolicy{}, err
	}
	if err := applyFloatHeader(msg.Headers, HeaderRetryJitter, &policy.Jitter); err != nil {
		return RetryPolicy{}, err
	}
	if err := applyDurationHeader(msg.Headers, HeaderRetryMaxTaskAge, &policy.MaxTaskAge); err != nil {
		return RetryPolicy{}, err
	}

	return policy.normalized(), nil
}

func (p RetryPolicy) ShouldRetry(deliveryCount int, firstEnqueuedAt, now time.Time) bool {
	p = p.normalized()
	maxDeliveries := p.effectiveMaxDeliveries()
	if maxDeliveries <= 0 {
		return false
	}
	if deliveryCount >= maxDeliveries {
		return false
	}
	if p.MaxTaskAge > 0 && !firstEnqueuedAt.IsZero() && now.Sub(firstEnqueuedAt) >= p.MaxTaskAge {
		return false
	}
	return true
}

func (p RetryPolicy) NextDelay(msg broker.TaskMessage, deliveryCount int) time.Duration {
	p = p.normalized()
	if deliveryCount <= 0 {
		deliveryCount = 1
	}

	backoff := float64(p.InitialBackoff) * math.Pow(p.Multiplier, float64(deliveryCount-1))
	delay := time.Duration(backoff)
	if delay > p.MaxBackoff {
		delay = p.MaxBackoff
	}

	if p.Jitter <= 0 {
		return delay
	}

	normalized := deterministicJitter(msg, deliveryCount)
	scale := 1 + ((normalized*2)-1)*p.Jitter
	if scale < 0 {
		scale = 0
	}

	jittered := time.Duration(float64(delay) * scale)
	if jittered > p.MaxBackoff {
		return p.MaxBackoff
	}
	return jittered
}

func (p RetryPolicy) effectiveMaxDeliveries() int {
	if p.MaxDeliveries > 0 {
		return p.MaxDeliveries
	}
	return p.MaxAttempts
}

func (p RetryPolicy) normalized() RetryPolicy {
	if p.InitialBackoff <= 0 {
		p.InitialBackoff = time.Second
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = 30 * time.Second
	}
	if p.Multiplier < 1 {
		p.Multiplier = 1
	}
	if p.Jitter < 0 {
		p.Jitter = 0
	}
	if p.MaxAttempts > 0 && p.MaxDeliveries <= 0 {
		p.MaxDeliveries = p.MaxAttempts
	}
	if p.MaxDeliveries > 0 && p.MaxAttempts <= 0 {
		p.MaxAttempts = p.MaxDeliveries
	}
	return p
}

func applyIntHeader(headers map[string]string, key string, target *int) error {
	value, ok := headers[key]
	if !ok || value == "" {
		return nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("%s: parse int: %w", key, err)
	}
	*target = parsed
	return nil
}

func applyFloatHeader(headers map[string]string, key string, target *float64) error {
	value, ok := headers[key]
	if !ok || value == "" {
		return nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("%s: parse float: %w", key, err)
	}
	*target = parsed
	return nil
}

func applyDurationHeader(headers map[string]string, key string, target *time.Duration) error {
	value, ok := headers[key]
	if !ok || value == "" {
		return nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("%s: parse duration: %w", key, err)
	}
	*target = parsed
	return nil
}

func deterministicJitter(msg broker.TaskMessage, deliveryCount int) float64 {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(msg.ID))
	_, _ = hasher.Write([]byte{':'})
	_, _ = hasher.Write([]byte(strconv.Itoa(deliveryCount)))
	return float64(hasher.Sum32()%10000) / 9999
}
