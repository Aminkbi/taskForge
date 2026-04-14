package scheduler

import (
	"strconv"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/tasks"
)

func ScheduleRetry(delivery broker.Delivery, failureClass string, policy tasks.RetryPolicy, clk clock.Clock) (broker.TaskMessage, bool, error) {
	now := clk.Now()
	resolved, err := tasks.ResolveRetryPolicy(policy, delivery.Message)
	if err != nil {
		return broker.TaskMessage{}, false, err
	}
	if !resolved.ShouldRetry(delivery.Execution.DeliveryCount, delivery.Execution.FirstEnqueuedAt, now) {
		return broker.TaskMessage{}, false, nil
	}

	delay := resolved.NextDelay(delivery.Message, delivery.Execution.DeliveryCount)
	retryAt := now.Add(delay)

	next := delivery.Message
	next.Attempt++
	next.ETA = &retryAt
	if next.Headers == nil {
		next.Headers = map[string]string{}
	}
	next.Headers[tasks.HeaderRetryScheduledAt] = retryAt.Format(time.RFC3339Nano)
	next.Headers[tasks.HeaderRetryDelay] = delay.String()
	next.Headers[tasks.HeaderRetryFailureClass] = failureClass
	next.Headers[tasks.HeaderRetryDeliveryCount] = strconv.Itoa(delivery.Execution.DeliveryCount)
	next.Headers[tasks.HeaderRetryFirstEnqueuedAt] = delivery.Execution.FirstEnqueuedAt.Format(time.RFC3339Nano)

	return next, true, nil
}
