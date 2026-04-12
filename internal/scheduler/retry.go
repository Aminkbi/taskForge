package scheduler

import (
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/tasks"
)

func ScheduleRetry(msg broker.TaskMessage, policy tasks.RetryPolicy, clk clock.Clock) (broker.TaskMessage, bool) {
	nextAttempt := msg.Attempt + 1
	if !policy.ShouldRetry(nextAttempt) {
		return broker.TaskMessage{}, false
	}

	retryAt := clk.Now().Add(policy.NextDelay(nextAttempt))
	next := msg
	next.Attempt = nextAttempt
	next.ETA = &retryAt
	if next.Headers == nil {
		next.Headers = map[string]string{}
	}
	next.Headers["retry_scheduled_at"] = retryAt.Format(time.RFC3339Nano)
	return next, true
}
