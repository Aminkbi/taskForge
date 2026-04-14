package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/dlq"
	"github.com/aminkbi/taskforge/internal/observability"
	"github.com/aminkbi/taskforge/internal/tasks"
)

type Worker struct {
	Broker            broker.Broker
	DeadLetter        dlq.Publisher
	Handler           Handler
	Logger            *slog.Logger
	Metrics           *observability.Metrics
	Clock             clock.Clock
	RetryPolicy       tasks.RetryPolicy
	PoolName          string
	Queue             string
	ConsumerID        string
	LeaseTTL          time.Duration
	Concurrency       int
	Prefetch          int
	GlobalTaskLimiter *TaskTypeLimiter
	PoolTaskLimiter   *TaskTypeLimiter
}

type taskExecution struct {
	delivery broker.Delivery
	release  func()
}

func (w *Worker) Run(ctx context.Context) error {
	if w.Concurrency < 1 {
		return fmt.Errorf("worker concurrency must be >= 1")
	}
	if w.Prefetch == 0 {
		w.Prefetch = w.Concurrency
	}
	if w.Prefetch < w.Concurrency {
		return fmt.Errorf("worker prefetch must be >= concurrency")
	}

	w.Logger.Info(
		"worker runtime started",
		"pool", w.PoolName,
		"queue", w.Queue,
		"concurrency", w.Concurrency,
		"prefetch", w.Prefetch,
	)

	reserved := make(chan broker.Delivery, w.Prefetch)
	dispatch := make(chan taskExecution)
	completed := make(chan struct{}, w.Concurrency)
	permits := make(chan struct{}, w.Prefetch)
	for i := 0; i < w.Prefetch; i++ {
		permits <- struct{}{}
	}
	errCh := make(chan error, w.Concurrency+1)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := w.reserveLoop(ctx, reserved, permits); err != nil {
			errCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := w.dispatchLoop(ctx, reserved, dispatch, completed); err != nil {
			errCh <- err
		}
	}()

	for i := 0; i < w.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.executorLoop(ctx, dispatch, completed, permits); err != nil {
				errCh <- err
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		<-done
		return nil
	case err := <-errCh:
		return err
	}
}

func (w *Worker) reserveLoop(ctx context.Context, deliveries chan<- broker.Delivery, permits chan struct{}) error {
	defer close(deliveries)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-permits:
		}

		delivery, err := w.Broker.Reserve(ctx, w.Queue, w.consumerKey())
		if err != nil {
			switch {
			case errors.Is(err, broker.ErrNoTask):
				permits <- struct{}{}
				continue
			case errors.Is(err, context.Canceled):
				permits <- struct{}{}
				return nil
			default:
				permits <- struct{}{}
				return fmt.Errorf("worker reserve task: %w", err)
			}
		}

		w.Metrics.IncReserved(tasks.EffectiveQueue(delivery.Message))
		select {
		case <-ctx.Done():
			return nil
		case deliveries <- delivery:
		}
	}
}

func (w *Worker) dispatchLoop(ctx context.Context, reserved <-chan broker.Delivery, dispatch chan<- taskExecution, completed <-chan struct{}) error {
	defer close(dispatch)

	pending := make([]broker.Delivery, 0, w.Prefetch)
	reservedClosed := false

	for {
		if len(pending) > 0 {
			if next, ok := w.nextDispatchable(pending); ok {
				pending = append(pending[:next.index], pending[next.index+1:]...)
				select {
				case <-ctx.Done():
					next.release()
					return nil
				case dispatch <- taskExecution{delivery: next.delivery, release: next.release}:
				}
				continue
			}
		}

		if reservedClosed && len(pending) == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-reserved:
			if !ok {
				reservedClosed = true
				continue
			}
			pending = append(pending, delivery)
		case <-completed:
		}
	}
}

func (w *Worker) executorLoop(ctx context.Context, deliveries <-chan taskExecution, completed chan<- struct{}, permits chan<- struct{}) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case execution, ok := <-deliveries:
			if !ok {
				return nil
			}
			err := w.processTask(ctx, execution.delivery)
			execution.release()
			permits <- struct{}{}
			select {
			case completed <- struct{}{}:
			default:
			}
			if err != nil {
				return err
			}
		}
	}
}

func (w *Worker) processTask(ctx context.Context, delivery broker.Delivery) error {
	msg := delivery.Message
	ttl := msg.VisibilityTimeout
	if ttl <= 0 {
		ttl = w.LeaseTTL
	}
	leaseCtx, cancelLease := context.WithCancel(ctx)
	defer cancelLease()

	stopLease := startLeaseExtender(leaseCtx, w.Logger, w.Broker, delivery, ttl)
	defer stopLease()

	execCtx := leaseCtx
	cancelExec := func() {}
	if msg.Timeout != nil && *msg.Timeout > 0 {
		execCtx, cancelExec = context.WithTimeout(leaseCtx, *msg.Timeout)
	}
	defer cancelExec()

	runningDelivery, err := transitionDelivery(delivery, tasks.StateRunning)
	if err != nil {
		return fmt.Errorf("worker mark delivery running: %w", err)
	}

	queue := tasks.EffectiveQueue(msg)
	w.Metrics.IncActiveTask(queue, msg.Name)
	started := time.Now()
	err = w.Handler.HandleTask(execCtx, msg)
	duration := time.Since(started).Seconds()
	w.Metrics.DecActiveTask(queue, msg.Name)

	if err == nil {
		w.Metrics.IncCompleted(queue)
		w.Metrics.ObserveExecution(queue, msg.Name, "succeeded", duration)
		succeededDelivery, transitionErr := transitionDelivery(runningDelivery, tasks.StateSucceeded)
		if transitionErr != nil {
			return fmt.Errorf("worker mark delivery succeeded: %w", transitionErr)
		}
		return w.Broker.Ack(ctx, succeededDelivery)
	}

	w.Logger.Error(
		"task execution failed",
		"task_id", msg.ID,
		"delivery_id", runningDelivery.Execution.DeliveryID,
		"lease_owner", runningDelivery.Execution.LeaseOwner,
		"lease_expires_at", runningDelivery.Execution.LeaseExpiresAt,
		"task_name", msg.Name,
		"attempt", msg.Attempt,
		"error", err,
	)
	w.Metrics.IncFailed(queue)
	w.Metrics.ObserveExecution(queue, msg.Name, "failed", duration)

	failedDelivery := runningDelivery.WithLastError(err.Error())
	failedMessage := failedDelivery.Message
	if failedMessage.Headers == nil {
		failedMessage.Headers = make(map[string]string, 1)
	}
	failedMessage.Headers["last_error"] = err.Error()
	failedDelivery.Message = failedMessage

	failureClass := classifyFailure(execCtx, err)
	action, next, envelope, policyErr := decideOutcome(failedDelivery, failureClass, err, w.RetryPolicy, w.Clock)
	if policyErr != nil {
		return fmt.Errorf("worker decide outcome: %w", policyErr)
	}
	switch action {
	case outcomeRetry:
		retryDelivery, transitionErr := transitionDelivery(failedDelivery, tasks.StateRetryScheduled)
		if transitionErr != nil {
			return fmt.Errorf("worker mark delivery retry_scheduled: %w", transitionErr)
		}
		if publishErr := w.Broker.Publish(ctx, next); publishErr != nil {
			if nackErr := w.Broker.Nack(ctx, failedDelivery, true); nackErr != nil {
				return errors.Join(fmt.Errorf("publish retry task: %w", publishErr), fmt.Errorf("nack original task: %w", nackErr))
			}
			return fmt.Errorf("publish retry task: %w", publishErr)
		}
		w.Metrics.IncRetried(queue)
		return w.Broker.Ack(ctx, retryDelivery)
	case outcomeDeadLetter:
		deadLetterDelivery, transitionErr := transitionDelivery(failedDelivery, tasks.StateDeadLettered)
		if transitionErr != nil {
			return fmt.Errorf("worker mark delivery dead_lettered: %w", transitionErr)
		}
		if w.DeadLetter != nil {
			if dlqErr := w.DeadLetter.PublishDeadLetter(ctx, envelope); dlqErr != nil {
				if nackErr := w.Broker.Nack(ctx, failedDelivery, true); nackErr != nil {
					return errors.Join(fmt.Errorf("publish dead-letter task: %w", dlqErr), fmt.Errorf("nack original task: %w", nackErr))
				}
				return fmt.Errorf("publish dead-letter task: %w", dlqErr)
			}
			w.Metrics.IncDeadLettered(queue)
		}
		return w.Broker.Ack(ctx, deadLetterDelivery)
	default:
		return w.Broker.Ack(ctx, failedDelivery)
	}
}

func (w *Worker) consumerKey() string {
	poolName := w.PoolName
	if poolName == "" {
		poolName = w.Queue
	}
	return fmt.Sprintf("%s-%s", w.ConsumerID, poolName)
}

type dispatchCandidate struct {
	index    int
	delivery broker.Delivery
	release  func()
}

func (w *Worker) nextDispatchable(pending []broker.Delivery) (dispatchCandidate, bool) {
	for i, delivery := range pending {
		releaseGlobal, ok := tryAcquireTaskSlot(w.GlobalTaskLimiter, delivery.Message.Name)
		if !ok {
			continue
		}

		releasePool, ok := tryAcquireTaskSlot(w.PoolTaskLimiter, delivery.Message.Name)
		if !ok {
			releaseGlobal()
			continue
		}

		return dispatchCandidate{
			index:    i,
			delivery: delivery,
			release: func() {
				releasePool()
				releaseGlobal()
			},
		}, true
	}
	return dispatchCandidate{}, false
}

func transitionDelivery(delivery broker.Delivery, next tasks.State) (broker.Delivery, error) {
	current := tasks.State(delivery.Execution.State)
	if err := tasks.ValidateTransition(current, next); err != nil {
		return broker.Delivery{}, err
	}
	delivery.Execution.State = string(next)
	return delivery, nil
}
