package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/observability"
	"go.opentelemetry.io/otel/trace"
)

// taskFailureResolution carries the state that must survive handler execution
// while the broker delivery is moved to its replacement state. Keeping both
// contexts here is intentional: publishing uses the resolution deadline, but
// requeueing must retain the worker shutdown and lease-loss cancellation.
type taskFailureResolution struct {
	resolutionCtx context.Context
	requeueCtx    context.Context
	span          trace.Span
	delivery      taskforge.Delivery
	brokerLease   *leaseHandle
	next          taskforge.Task
	envelope      taskforge.DeadLetterEnvelope
	failureClass  taskforge.FailureClass
}

func (w *Worker) resolveTaskFailure(resolution taskFailureResolution, action outcome) error {
	switch action {
	case outcomeRetry:
		return w.resolveRetry(resolution)
	case outcomeDeadLetter:
		return w.resolveDeadLetter(resolution)
	default:
		return w.acknowledgeResolved(
			resolution.resolutionCtx,
			resolution.span,
			resolution.delivery,
			resolution.brokerLease,
			"ack_failed_delivery",
		)
	}
}

func (w *Worker) resolveRetry(resolution taskFailureResolution) error {
	retryDelivery, transitionErr := transitionDelivery(resolution.delivery, taskforge.StateRetryScheduled)
	if transitionErr != nil {
		observability.MarkSpanError(resolution.span, transitionErr)
		return fmt.Errorf("worker mark delivery retry_scheduled: %w", transitionErr)
	}
	if w.abandonIfLeaseLost(retryDelivery, resolution.brokerLease, "publish_retry") {
		return nil
	}

	_, publishErr := w.Broker.Publish(resolution.resolutionCtx, resolution.next, taskforge.PublishOptions{
		Source:           taskforge.PublishSourceRetry,
		DeduplicationKey: fmt.Sprintf("retry:%s", resolution.delivery.OwnershipKey()),
	})
	if publishErr == nil {
		w.Metrics.IncRetryScheduled(
			taskforge.EffectiveQueue(resolution.delivery.Message),
			resolution.delivery.Message.Name,
			string(resolution.failureClass),
		)
		return w.acknowledgeResolved(resolution.resolutionCtx, resolution.span, retryDelivery, resolution.brokerLease, "ack_retry_scheduled")
	}

	observability.MarkSpanError(resolution.span, publishErr)
	var admissionErr *taskforge.AdmissionError
	if errors.As(publishErr, &admissionErr) {
		return w.resolveAdmissionRejectedRetry(resolution, publishErr)
	}
	return w.requeueAfterResolutionFailure(
		resolution,
		fmt.Errorf("publish retry task: %w", publishErr),
		"nack_retry_publish_failed",
	)
}

func (w *Worker) resolveAdmissionRejectedRetry(resolution taskFailureResolution, publishErr error) error {
	overloadedEnvelope := taskforge.NewDeadLetterEnvelope(
		resolution.delivery,
		taskforge.FailureClassOverloaded,
		publishErr.Error(),
		w.Clock.Now(),
	)
	deadLetterDelivery, transitionErr := transitionDelivery(resolution.delivery, taskforge.StateDeadLettered)
	if transitionErr != nil {
		observability.MarkSpanError(resolution.span, transitionErr)
		return fmt.Errorf("worker mark delivery dead_lettered after retry rejection: %w", transitionErr)
	}

	if dlqErr := w.publishDeadLetter(resolution.resolutionCtx, overloadedEnvelope); dlqErr != nil {
		observability.MarkSpanError(resolution.span, dlqErr)
		return w.requeueAfterResolutionFailure(
			resolution,
			fmt.Errorf("publish dead-letter task: %w", dlqErr),
			"nack_after_dead_letter_failure",
		)
	}

	w.Metrics.IncDeadLetterResult(
		taskforge.EffectiveQueue(resolution.delivery.Message),
		resolution.delivery.Message.Name,
		string(taskforge.FailureClassOverloaded),
	)
	return w.acknowledgeResolved(
		resolution.resolutionCtx,
		resolution.span,
		deadLetterDelivery,
		resolution.brokerLease,
		"ack_dead_lettered_retry_rejected",
	)
}

func (w *Worker) resolveDeadLetter(resolution taskFailureResolution) error {
	deadLetterDelivery, transitionErr := transitionDelivery(resolution.delivery, taskforge.StateDeadLettered)
	if transitionErr != nil {
		observability.MarkSpanError(resolution.span, transitionErr)
		return fmt.Errorf("worker mark delivery dead_lettered: %w", transitionErr)
	}
	if w.abandonIfLeaseLost(deadLetterDelivery, resolution.brokerLease, "publish_dead_letter") {
		return nil
	}

	if dlqErr := w.publishDeadLetter(resolution.resolutionCtx, resolution.envelope); dlqErr != nil {
		observability.MarkSpanError(resolution.span, dlqErr)
		return w.requeueAfterResolutionFailure(
			resolution,
			fmt.Errorf("publish dead-letter task: %w", dlqErr),
			"nack_dead_letter_publish_failed",
		)
	}

	w.Metrics.IncDeadLetterResult(
		taskforge.EffectiveQueue(resolution.delivery.Message),
		resolution.delivery.Message.Name,
		string(resolution.failureClass),
	)
	return w.acknowledgeResolved(
		resolution.resolutionCtx,
		resolution.span,
		deadLetterDelivery,
		resolution.brokerLease,
		"ack_dead_lettered",
	)
}

func (w *Worker) requeueAfterResolutionFailure(resolution taskFailureResolution, replacementErr error, leaseLossPhase string) error {
	if nackErr := w.requeue(resolution.requeueCtx, resolution.delivery); nackErr != nil {
		if w.leaseOwnershipLost(nackErr) {
			w.logLeaseLoss(resolution.delivery, leaseLossPhase, resolution.brokerLease)
			return replacementErr
		}
		observability.MarkSpanError(resolution.span, nackErr)
		return errors.Join(replacementErr, fmt.Errorf("nack original task: %w", nackErr))
	}
	return replacementErr
}
