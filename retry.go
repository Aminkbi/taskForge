package taskforge

import (
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"time"
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
	HeaderScheduledFor         = "taskforge_scheduled_for"
	HeaderReleasedAt           = "taskforge_released_at"
	HeaderReleaseLagMS         = "taskforge_release_lag_ms"
)

type RetryPolicy struct {
	MaxDeliveries  int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
	Jitter         float64
	MaxTaskAge     time.Duration
}

func DefaultRetryPolicy(maxDeliveries int) RetryPolicy {
	return RetryPolicy{
		MaxDeliveries:  maxDeliveries,
		InitialBackoff: time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2,
		Jitter:         0,
	}
}

func ResolveRetryPolicy(base RetryPolicy, msg Task) (RetryPolicy, error) {
	policy := base.normalized()
	if msg.MaxDeliveries > 0 {
		policy.MaxDeliveries = msg.MaxDeliveries
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
	if p.MaxDeliveries <= 0 {
		return false
	}
	if deliveryCount >= p.MaxDeliveries {
		return false
	}
	if p.MaxTaskAge > 0 && !firstEnqueuedAt.IsZero() && now.Sub(firstEnqueuedAt) >= p.MaxTaskAge {
		return false
	}
	return true
}

func (p RetryPolicy) NextDelay(msg Task, deliveryCount int) time.Duration {
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

func ScheduleRetry(delivery Delivery, failureClass string, policy RetryPolicy, now time.Time) (Task, bool, error) {
	resolved, err := ResolveRetryPolicy(policy, delivery.Message)
	if err != nil {
		return Task{}, false, err
	}
	if !resolved.ShouldRetry(delivery.Execution.DeliveryCount, delivery.Execution.FirstEnqueuedAt, now) {
		return Task{}, false, nil
	}

	delay := resolved.NextDelay(delivery.Message, delivery.Execution.DeliveryCount)
	retryAt := now.Add(delay)
	next := delivery.Message.Clone()
	next.Attempt++
	next.ETA = &retryAt
	if next.Headers == nil {
		next.Headers = map[string]string{}
	}
	next.Headers[HeaderRetryScheduledAt] = retryAt.Format(time.RFC3339Nano)
	next.Headers[HeaderRetryDelay] = delay.String()
	next.Headers[HeaderRetryFailureClass] = failureClass
	next.Headers[HeaderRetryDeliveryCount] = strconv.Itoa(delivery.Execution.DeliveryCount)
	next.Headers[HeaderRetryFirstEnqueuedAt] = delivery.Execution.FirstEnqueuedAt.Format(time.RFC3339Nano)
	return next, true, nil
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

func deterministicJitter(msg Task, deliveryCount int) float64 {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(msg.ID))
	_, _ = hasher.Write([]byte{':'})
	_, _ = hasher.Write([]byte(strconv.Itoa(deliveryCount)))
	return float64(hasher.Sum32()%10000) / 9999
}
