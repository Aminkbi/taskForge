package runtime

import (
	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/clock"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
	"github.com/aminkbi/taskforge/internal/tasks"
)

type outcome string

const (
	outcomeAck        outcome = "ack"
	outcomeRetry      outcome = "retry"
	outcomeDeadLetter outcome = "dead_letter"
)

func decideOutcome(delivery broker.Delivery, policy tasks.RetryPolicy, clk clock.Clock) (outcome, broker.TaskMessage) {
	next, ok := schedulerpkg.ScheduleRetry(delivery.Message, policy, clk)
	if ok {
		return outcomeRetry, next
	}
	return outcomeDeadLetter, broker.TaskMessage{}
}
