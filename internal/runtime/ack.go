package runtime

import (
	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/dlq"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
	"github.com/aminkbi/taskforge/internal/tasks"
)

type outcome string

const (
	outcomeAck        outcome = "ack"
	outcomeRetry      outcome = "retry"
	outcomeDeadLetter outcome = "dead_letter"
)

func decideOutcome(delivery broker.Delivery, failureClass dlq.FailureClass, failure error, policy tasks.RetryPolicy, clk clock.Clock) (outcome, broker.TaskMessage, dlq.Envelope, error) {
	now := clk.Now()
	envelope := dlq.NewEnvelope(delivery, failureClass, failure.Error(), now)

	switch failureClass {
	case dlq.FailureClassTransientRetryable, dlq.FailureClassTimeout:
		next, ok, err := schedulerpkg.ScheduleRetry(delivery, string(failureClass), policy, clk)
		if err != nil {
			return outcomeDeadLetter, broker.TaskMessage{}, envelope, err
		}
		if ok {
			return outcomeRetry, next, envelope, nil
		}
		return outcomeDeadLetter, broker.TaskMessage{}, envelope, nil
	case dlq.FailureClassPermanent, dlq.FailureClassLeaseLost, dlq.FailureClassDecodeValidation:
		return outcomeDeadLetter, broker.TaskMessage{}, envelope, nil
	default:
		return outcomeAck, broker.TaskMessage{}, envelope, nil
	}
}
