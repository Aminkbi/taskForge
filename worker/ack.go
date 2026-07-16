package worker

import (
	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/clock"
)

type outcome string

const (
	outcomeAck        outcome = "ack"
	outcomeRetry      outcome = "retry"
	outcomeDeadLetter outcome = "dead_letter"
)

func decideOutcome(delivery taskforge.Delivery, failureClass taskforge.FailureClass, failure error, policy taskforge.RetryPolicy, clk clock.Clock) (outcome, taskforge.Task, taskforge.DeadLetterEnvelope, error) {
	now := clk.Now()
	envelope := taskforge.NewDeadLetterEnvelope(delivery, failureClass, failure.Error(), now)

	switch failureClass {
	case taskforge.FailureClassTransientRetryable, taskforge.FailureClassTimeout:
		next, ok, err := taskforge.ScheduleRetry(delivery, string(failureClass), policy, now)
		if err != nil {
			return outcomeDeadLetter, taskforge.Task{}, envelope, err
		}
		if ok {
			return outcomeRetry, next, envelope, nil
		}
		return outcomeDeadLetter, taskforge.Task{}, envelope, nil
	case taskforge.FailureClassPermanent, taskforge.FailureClassLeaseLost, taskforge.FailureClassDecodeValidation:
		return outcomeDeadLetter, taskforge.Task{}, envelope, nil
	default:
		return outcomeAck, taskforge.Task{}, envelope, nil
	}
}
