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
	"github.com/aminkbi/taskforge/internal/healthcheck"
	"github.com/aminkbi/taskforge/internal/logging"
	"github.com/aminkbi/taskforge/internal/observability"
	"github.com/aminkbi/taskforge/internal/tasks"
	"go.opentelemetry.io/otel/attribute"
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
	RecoveryHealth    *healthcheck.Reporter
	GlobalTaskLimiter *TaskTypeLimiter
	PoolTaskLimiter   *TaskTypeLimiter
	BudgetManager     BudgetManager
	TaskBudgets       map[string]TaskBudget
	QueueMetrics      QueueMetricsProvider
	Adaptive          AdaptiveConfig
	AdaptiveStore     AdaptiveStateWriter
	runtimeState      *workerState
}

type workerState struct {
	mu                   sync.Mutex
	pending              []*pendingDelivery
	running              int
	effectiveConcurrency int
	window               adaptiveWindow
	healthyWindows       int
	lastAdjustmentAction string
	lastAdjustmentReason string
	lastAdjustedAt       time.Time
}

type adaptiveWindow struct {
	executions    int
	failed        int
	totalLatency  time.Duration
	budgetBlocked int64
}

type pendingDelivery struct {
	delivery    broker.Delivery
	brokerLease *leaseHandle
	mu          sync.Mutex
	execCancel  context.CancelFunc
}

type dispatchCandidate struct {
	entry       *pendingDelivery
	release     func()
	budgetLease *TaskBudget
}

func (w *Worker) Run(ctx context.Context) error {
	return w.run(ctx, nil)
}

func (w *Worker) run(ctx context.Context, stop <-chan struct{}) error {
	if w.Concurrency < 1 {
		return fmt.Errorf("worker concurrency must be >= 1")
	}
	if w.Prefetch == 0 {
		w.Prefetch = w.defaultPrefetch()
	}
	if w.Prefetch < 1 {
		return fmt.Errorf("worker prefetch must be >= 1")
	}
	if w.Adaptive.Enabled && w.QueueMetrics == nil {
		return fmt.Errorf("adaptive concurrency requires queue metrics provider")
	}
	if w.Clock == nil {
		w.Clock = clock.RealClock{}
	}

	w.Logger.Info(
		"worker runtime started",
		"pool", w.PoolName,
		"queue", w.Queue,
		"concurrency", w.Concurrency,
		"prefetch", w.Prefetch,
		"adaptive_enabled", w.Adaptive.Enabled,
	)

	state := &workerState{
		effectiveConcurrency: w.Concurrency,
		pending:              make([]*pendingDelivery, 0, w.Prefetch),
	}
	w.runtimeState = state
	w.publishAdaptiveState(ctx, state, observability.AdaptivePoolSnapshot{
		Pool:                  w.PoolName,
		Queue:                 w.Queue,
		AdaptiveEnabled:       w.Adaptive.Enabled,
		ConfiguredConcurrency: float64(w.Concurrency),
		EffectiveConcurrency:  float64(w.Concurrency),
		MinConcurrency:        float64(w.minConcurrency()),
		MaxConcurrency:        float64(w.maxConcurrency()),
	})
	w.Metrics.SetWorkerEffectiveConcurrency(w.PoolName, w.Queue, float64(w.Concurrency))

	controlCtx, cancelControl := context.WithCancel(ctx)
	defer cancelControl()
	if stop != nil {
		go func() {
			select {
			case <-stop:
				cancelControl()
			case <-controlCtx.Done():
			}
		}()
	}

	reserveWake := make(chan struct{}, 1)
	dispatchWake := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	var loops sync.WaitGroup
	var executions sync.WaitGroup

	loops.Add(1)
	go func() {
		defer loops.Done()
		if err := w.reserveLoop(controlCtx, state, reserveWake, dispatchWake); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	loops.Add(1)
	go func() {
		defer loops.Done()
		if err := w.dispatchLoop(ctx, controlCtx, state, reserveWake, dispatchWake, errCh, &executions); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	if w.Adaptive.Enabled {
		loops.Add(1)
		go func() {
			defer loops.Done()
			if err := w.adaptiveLoop(controlCtx, state, reserveWake, dispatchWake); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		loops.Wait()
		executions.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		<-done
		return nil
	case <-done:
		return nil
	case err := <-errCh:
		cancelControl()
		w.stopPendingReservations(state)
		notify(reserveWake)
		notify(dispatchWake)
		<-done
		return err
	}
}

func (w *Worker) reserveLoop(ctx context.Context, state *workerState, reserveWake, dispatchWake chan struct{}) error {
	if w.RecoveryHealth != nil {
		w.RecoveryHealth.MarkReady("worker reserve and reclaim loop healthy")
	}
	defer func() {
		if w.RecoveryHealth != nil {
			w.RecoveryHealth.MarkNotReady("worker shutting down")
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if !w.canReserve(state) {
			select {
			case <-ctx.Done():
				return nil
			case <-reserveWake:
			case <-time.After(25 * time.Millisecond):
			}
			continue
		}

		delivery, err := w.Broker.Reserve(ctx, w.Queue, w.consumerKey())
		if err != nil {
			switch {
			case errors.Is(err, broker.ErrNoTask):
				continue
			case errors.Is(err, context.Canceled):
				return nil
			default:
				if w.RecoveryHealth != nil {
					w.RecoveryHealth.MarkFailed(err.Error())
				}
				return fmt.Errorf("worker reserve task: %w", err)
			}
		}

		entry := &pendingDelivery{
			delivery:    delivery,
			brokerLease: startLeaseExtender(ctx, w.Logger, w.Broker, delivery, w.deliveryLeaseTTL(delivery)),
		}

		w.Metrics.IncReserved(tasks.EffectiveQueue(delivery.Message))
		state.mu.Lock()
		state.pending = append(state.pending, entry)
		state.mu.Unlock()
		go w.watchLeaseLoss(ctx, state, reserveWake, dispatchWake, entry)
		notify(dispatchWake)
	}
}

func (w *Worker) dispatchLoop(execCtx, controlCtx context.Context, state *workerState, reserveWake, dispatchWake chan struct{}, errCh chan<- error, executions *sync.WaitGroup) error {
	for {
		candidate, ok, err := w.nextDispatchable(controlCtx, state, reserveWake, dispatchWake)
		if err != nil {
			return err
		}
		if !ok {
			select {
			case <-controlCtx.Done():
				return nil
			case <-dispatchWake:
			case <-time.After(25 * time.Millisecond):
			}
			continue
		}

		executions.Add(1)
		go func() {
			defer executions.Done()
			w.executeCandidate(execCtx, state, reserveWake, dispatchWake, errCh, candidate)
		}()
	}
}

func (w *Worker) adaptiveLoop(ctx context.Context, state *workerState, reserveWake, dispatchWake chan struct{}) error {
	ticker := time.NewTicker(w.Adaptive.ControlPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			snapshot, oldConcurrency, action, reason, changed, err := w.evaluateAdaptiveWindow(ctx, state)
			if err != nil {
				return err
			}
			if changed {
				w.Metrics.IncWorkerConcurrencyAdjustment(w.PoolName, reason, action)
				w.Logger.Info(
					"worker concurrency adjusted",
					"pool", w.PoolName,
					"queue", w.Queue,
					"action", action,
					"reason", reason,
					"old_concurrency", oldConcurrency,
					"new_concurrency", int(snapshot.EffectiveConcurrency),
					"avg_latency_seconds", snapshot.AvgLatencySeconds,
					"error_rate", snapshot.ErrorRate,
					"budget_blocked", snapshot.BudgetBlocked,
					"backlog", snapshot.Backlog,
				)
				notify(reserveWake)
				notify(dispatchWake)
			}
			w.publishAdaptiveState(ctx, state, snapshot)
		}
	}
}

func (w *Worker) evaluateAdaptiveWindow(ctx context.Context, state *workerState) (observability.AdaptivePoolSnapshot, int, string, string, bool, error) {
	now := w.Clock.Now().UTC()
	queueSnapshot, err := w.QueueMetrics.QueueMetricsSnapshot(ctx, w.Queue)
	if err != nil {
		return observability.AdaptivePoolSnapshot{}, 0, "", "", false, fmt.Errorf("adaptive queue metrics %q: %w", w.Queue, err)
	}
	backlog := queueSnapshot.Depth + queueSnapshot.Reserved

	state.mu.Lock()
	window := state.window
	state.window = adaptiveWindow{}
	avgLatency := 0.0
	if window.executions > 0 {
		avgLatency = window.totalLatency.Seconds() / float64(window.executions)
	}
	errorRate := 0.0
	if window.executions > 0 {
		errorRate = float64(window.failed) / float64(window.executions)
	}

	reason := ""
	action := ""
	changed := false
	current := state.effectiveConcurrency
	old := current
	switch {
	case avgLatency > w.Adaptive.LatencyThreshold.Seconds():
		reason = "latency"
		action = "scale_down"
		current = maxInt(w.Adaptive.MinConcurrency, current-w.Adaptive.ScaleDownStep)
		state.healthyWindows = 0
	case errorRate > w.Adaptive.ErrorRateThreshold:
		reason = "error_rate"
		action = "scale_down"
		current = maxInt(w.Adaptive.MinConcurrency, current-w.Adaptive.ScaleDownStep)
		state.healthyWindows = 0
	case window.budgetBlocked > 0 && backlog >= float64(w.Adaptive.BacklogThreshold):
		reason = "budget_exhaustion"
		action = "scale_down"
		current = maxInt(w.Adaptive.MinConcurrency, current-w.Adaptive.ScaleDownStep)
		state.healthyWindows = 0
	default:
		if window.executions > 0 &&
			avgLatency <= w.Adaptive.LatencyThreshold.Seconds() &&
			errorRate <= w.Adaptive.ErrorRateThreshold &&
			window.budgetBlocked == 0 {
			state.healthyWindows++
		} else {
			state.healthyWindows = 0
		}
		if backlog >= float64(w.Adaptive.BacklogThreshold) &&
			state.healthyWindows >= w.Adaptive.HealthyWindowsRequired &&
			now.Sub(state.lastAdjustedAt) >= w.Adaptive.Cooldown {
			reason = "healthy_backlog"
			action = "scale_up"
			current = minInt(w.Adaptive.MaxConcurrency, current+w.Adaptive.ScaleUpStep)
		}
	}

	if current != old {
		changed = true
		state.effectiveConcurrency = current
		state.lastAdjustedAt = now
		state.lastAdjustmentAction = action
		state.lastAdjustmentReason = reason
		w.Metrics.SetWorkerEffectiveConcurrency(w.PoolName, w.Queue, float64(current))
	} else if state.lastAdjustmentAction == "" && !w.Adaptive.Enabled {
		state.lastAdjustmentAction = "static"
		state.lastAdjustmentReason = "disabled"
	}

	snapshot := observability.AdaptivePoolSnapshot{
		Pool:                  w.PoolName,
		Queue:                 w.Queue,
		AdaptiveEnabled:       w.Adaptive.Enabled,
		ConfiguredConcurrency: float64(w.Concurrency),
		EffectiveConcurrency:  float64(state.effectiveConcurrency),
		MinConcurrency:        float64(w.minConcurrency()),
		MaxConcurrency:        float64(w.maxConcurrency()),
		AvgLatencySeconds:     avgLatency,
		ErrorRate:             errorRate,
		BudgetBlocked:         float64(window.budgetBlocked),
		Backlog:               backlog,
		HealthyWindows:        float64(state.healthyWindows),
		LastAdjustmentAction:  state.lastAdjustmentAction,
		LastAdjustmentReason:  state.lastAdjustmentReason,
		LastAdjustedAt:        state.lastAdjustedAt,
	}
	state.mu.Unlock()
	return snapshot, old, action, reason, changed, nil
}

func (w *Worker) nextDispatchable(ctx context.Context, state *workerState, reserveWake, dispatchWake chan struct{}) (dispatchCandidate, bool, error) {
	state.mu.Lock()
	if state.running >= state.effectiveConcurrency || len(state.pending) == 0 {
		state.mu.Unlock()
		return dispatchCandidate{}, false, nil
	}
	pending := append([]*pendingDelivery(nil), state.pending...)
	state.mu.Unlock()

	for _, entry := range pending {
		if w.dropPendingIfLeaseLost(state, reserveWake, dispatchWake, entry) {
			continue
		}

		delivery := entry.delivery
		releaseGlobal, ok := tryAcquireTaskSlot(w.GlobalTaskLimiter, delivery.Message.Name)
		if !ok {
			continue
		}

		releasePool, ok := tryAcquireTaskSlot(w.PoolTaskLimiter, delivery.Message.Name)
		if !ok {
			releaseGlobal()
			continue
		}

		budgetLease, ok, err := w.tryAcquireBudget(ctx, delivery)
		if err != nil {
			releasePool()
			releaseGlobal()
			return dispatchCandidate{}, false, err
		}
		if !ok {
			releasePool()
			releaseGlobal()
			continue
		}

		if entry.brokerLease != nil && entry.brokerLease.IsLost() {
			w.releaseBudget(ctx, budgetLease, delivery)
			releasePool()
			releaseGlobal()
			w.dropPendingIfLeaseLost(state, reserveWake, dispatchWake, entry)
			continue
		}

		state.mu.Lock()
		if state.running >= state.effectiveConcurrency {
			state.mu.Unlock()
			w.releaseBudget(ctx, budgetLease, delivery)
			releasePool()
			releaseGlobal()
			return dispatchCandidate{}, false, nil
		}
		index := indexPendingEntry(state.pending, delivery.Execution.DeliveryID)
		if index < 0 {
			state.mu.Unlock()
			w.releaseBudget(ctx, budgetLease, delivery)
			releasePool()
			releaseGlobal()
			continue
		}
		if state.pending[index].brokerLease != nil && state.pending[index].brokerLease.IsLost() {
			stale := state.pending[index]
			state.pending = append(state.pending[:index], state.pending[index+1:]...)
			state.mu.Unlock()
			w.releaseBudget(ctx, budgetLease, delivery)
			releasePool()
			releaseGlobal()
			w.logLeaseLoss(delivery, "pending dispatch", stale.brokerLease)
			notify(reserveWake)
			notify(dispatchWake)
			continue
		}
		selected := state.pending[index]
		state.pending = append(state.pending[:index], state.pending[index+1:]...)
		state.running++
		state.mu.Unlock()

		return dispatchCandidate{
			entry:       selected,
			budgetLease: budgetLease,
			release: func() {
				releasePool()
				releaseGlobal()
			},
		}, true, nil
	}

	return dispatchCandidate{}, false, nil
}

func (w *Worker) executeCandidate(ctx context.Context, state *workerState, reserveWake, dispatchWake chan struct{}, errCh chan<- error, candidate dispatchCandidate) {
	defer func() {
		candidate.release()
		state.mu.Lock()
		state.running--
		state.mu.Unlock()
		notify(reserveWake)
		notify(dispatchWake)
	}()

	entry := candidate.entry
	delivery := entry.delivery
	ttl := w.deliveryLeaseTTL(delivery)
	execCtx, cancelExec := context.WithCancel(ctx)
	entry.setExecutionCancel(cancelExec)
	defer func() {
		entry.clearExecutionCancel()
		cancelExec()
	}()
	if entry.brokerLease != nil && entry.brokerLease.IsLost() {
		cancelExec()
	}

	var budgetLease *leaseHandle
	if candidate.budgetLease != nil {
		budgetLease = startBudgetExtender(execCtx, w.Logger, w.BudgetManager, delivery, *candidate.budgetLease, ttl)
		if budgetLease != nil {
			go w.watchBudgetLeaseLoss(execCtx, cancelExec, budgetLease, *candidate.budgetLease)
		}
	}
	defer func() {
		if budgetLease != nil {
			budgetLease.Stop()
		}
		if entry.brokerLease != nil {
			entry.brokerLease.Stop()
		}
		w.releaseBudget(ctx, candidate.budgetLease, delivery)
	}()

	if err := w.processTask(execCtx, delivery, entry.brokerLease); err != nil {
		select {
		case errCh <- err:
		default:
		}
	}
}

func (w *Worker) processTask(ctx context.Context, delivery broker.Delivery, brokerLease *leaseHandle) error {
	msg := delivery.Message
	execCtx := ctx
	cancelExec := func() {}
	if msg.Timeout != nil && *msg.Timeout > 0 {
		execCtx, cancelExec = context.WithTimeout(ctx, *msg.Timeout)
	}
	defer cancelExec()
	execCtx = observability.ExtractTraceContext(execCtx, msg.Headers)

	runningDelivery, err := transitionDelivery(delivery, tasks.StateRunning)
	if err != nil {
		return fmt.Errorf("worker mark delivery running: %w", err)
	}
	execCtx, span := observability.StartQueueSpan(
		execCtx,
		"taskforge.worker",
		"taskforge.execute",
		runningDelivery.Message,
		attribute.String("taskforge.delivery_id", runningDelivery.Execution.DeliveryID),
		attribute.String("taskforge.worker_identity", runningDelivery.Execution.LeaseOwner),
		attribute.Int("taskforge.delivery_count", runningDelivery.Execution.DeliveryCount),
	)
	defer span.End()

	queue := tasks.EffectiveQueue(msg)
	w.Metrics.IncActiveTask(queue, msg.Name)
	started := time.Now()
	err = w.Handler.HandleTask(execCtx, msg)
	duration := time.Since(started)
	w.Metrics.DecActiveTask(queue, msg.Name)
	if w.abandonIfLeaseLost(delivery, brokerLease, "post_handle") {
		return nil
	}
	w.recordExecution(duration, err != nil)

	if err == nil {
		w.Metrics.IncCompleted(queue)
		w.Metrics.ObserveExecution(queue, msg.Name, "succeeded", duration.Seconds())
		succeededDelivery, transitionErr := transitionDelivery(runningDelivery, tasks.StateSucceeded)
		if transitionErr != nil {
			observability.MarkSpanError(span, transitionErr)
			return fmt.Errorf("worker mark delivery succeeded: %w", transitionErr)
		}
		if w.abandonIfLeaseLost(succeededDelivery, brokerLease, "ack_succeeded") {
			return nil
		}
		if ackErr := w.Broker.Ack(execCtx, succeededDelivery); ackErr != nil {
			if w.leaseOwnershipLost(ackErr) {
				w.logLeaseLoss(succeededDelivery, "ack_succeeded", brokerLease)
				return nil
			}
			observability.MarkSpanError(span, ackErr)
			return ackErr
		}
		return nil
	}

	observability.MarkSpanError(span, err)
	logging.WithDelivery(w.Logger, runningDelivery).Error(
		"task execution failed",
		"task_name", msg.Name,
		"attempt", msg.Attempt,
		"error", err,
	)
	w.Metrics.IncFailed(queue)
	w.Metrics.ObserveExecution(queue, msg.Name, "failed", duration.Seconds())

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
		observability.MarkSpanError(span, policyErr)
		return fmt.Errorf("worker decide outcome: %w", policyErr)
	}
	switch action {
	case outcomeRetry:
		retryDelivery, transitionErr := transitionDelivery(failedDelivery, tasks.StateRetryScheduled)
		if transitionErr != nil {
			observability.MarkSpanError(span, transitionErr)
			return fmt.Errorf("worker mark delivery retry_scheduled: %w", transitionErr)
		}
		if w.abandonIfLeaseLost(retryDelivery, brokerLease, "publish_retry") {
			return nil
		}
		if _, publishErr := w.Broker.Publish(execCtx, next, broker.PublishOptions{
			Source:           broker.PublishSourceRetry,
			DeduplicationKey: fmt.Sprintf("retry:%s", failedDelivery.Execution.DeliveryID),
		}); publishErr != nil {
			observability.MarkSpanError(span, publishErr)
			var admissionErr *broker.AdmissionError
			if errors.As(publishErr, &admissionErr) {
				overloadedEnvelope := dlq.NewEnvelope(failedDelivery, dlq.FailureClassOverloaded, publishErr.Error(), w.Clock.Now())
				deadLetterDelivery, transitionErr := transitionDelivery(failedDelivery, tasks.StateDeadLettered)
				if transitionErr != nil {
					observability.MarkSpanError(span, transitionErr)
					return fmt.Errorf("worker mark delivery dead_lettered after retry rejection: %w", transitionErr)
				}
				if w.DeadLetter != nil {
					if dlqErr := w.DeadLetter.PublishDeadLetter(execCtx, overloadedEnvelope); dlqErr != nil {
						observability.MarkSpanError(span, dlqErr)
						if nackErr := w.Broker.Nack(execCtx, failedDelivery, true); nackErr != nil {
							if w.leaseOwnershipLost(nackErr) {
								w.logLeaseLoss(failedDelivery, "nack_after_dead_letter_failure", brokerLease)
								return fmt.Errorf("publish dead-letter task: %w", dlqErr)
							}
							observability.MarkSpanError(span, nackErr)
							return errors.Join(fmt.Errorf("publish dead-letter task: %w", dlqErr), fmt.Errorf("nack original task: %w", nackErr))
						}
						return fmt.Errorf("publish dead-letter task: %w", dlqErr)
					}
					w.Metrics.IncDeadLetterResult(queue, msg.Name, string(dlq.FailureClassOverloaded))
				}
				if w.abandonIfLeaseLost(deadLetterDelivery, brokerLease, "ack_dead_lettered_retry_rejected") {
					return nil
				}
				if ackErr := w.Broker.Ack(execCtx, deadLetterDelivery); ackErr != nil {
					if w.leaseOwnershipLost(ackErr) {
						w.logLeaseLoss(deadLetterDelivery, "ack_dead_lettered_retry_rejected", brokerLease)
						return nil
					}
					observability.MarkSpanError(span, ackErr)
					return ackErr
				}
				return nil
			}
			if nackErr := w.Broker.Nack(execCtx, failedDelivery, true); nackErr != nil {
				if w.leaseOwnershipLost(nackErr) {
					w.logLeaseLoss(failedDelivery, "nack_retry_publish_failed", brokerLease)
					return fmt.Errorf("publish retry task: %w", publishErr)
				}
				observability.MarkSpanError(span, nackErr)
				return errors.Join(fmt.Errorf("publish retry task: %w", publishErr), fmt.Errorf("nack original task: %w", nackErr))
			}
			return fmt.Errorf("publish retry task: %w", publishErr)
		}
		w.Metrics.IncRetryScheduled(queue, msg.Name, string(failureClass))
		if w.abandonIfLeaseLost(retryDelivery, brokerLease, "ack_retry_scheduled") {
			return nil
		}
		if ackErr := w.Broker.Ack(execCtx, retryDelivery); ackErr != nil {
			if w.leaseOwnershipLost(ackErr) {
				w.logLeaseLoss(retryDelivery, "ack_retry_scheduled", brokerLease)
				return nil
			}
			observability.MarkSpanError(span, ackErr)
			return ackErr
		}
		return nil
	case outcomeDeadLetter:
		deadLetterDelivery, transitionErr := transitionDelivery(failedDelivery, tasks.StateDeadLettered)
		if transitionErr != nil {
			observability.MarkSpanError(span, transitionErr)
			return fmt.Errorf("worker mark delivery dead_lettered: %w", transitionErr)
		}
		if w.abandonIfLeaseLost(deadLetterDelivery, brokerLease, "publish_dead_letter") {
			return nil
		}
		if w.DeadLetter != nil {
			if dlqErr := w.DeadLetter.PublishDeadLetter(execCtx, envelope); dlqErr != nil {
				observability.MarkSpanError(span, dlqErr)
				if nackErr := w.Broker.Nack(execCtx, failedDelivery, true); nackErr != nil {
					if w.leaseOwnershipLost(nackErr) {
						w.logLeaseLoss(failedDelivery, "nack_dead_letter_publish_failed", brokerLease)
						return fmt.Errorf("publish dead-letter task: %w", dlqErr)
					}
					observability.MarkSpanError(span, nackErr)
					return errors.Join(fmt.Errorf("publish dead-letter task: %w", dlqErr), fmt.Errorf("nack original task: %w", nackErr))
				}
				return fmt.Errorf("publish dead-letter task: %w", dlqErr)
			}
			w.Metrics.IncDeadLetterResult(queue, msg.Name, string(failureClass))
		}
		if w.abandonIfLeaseLost(deadLetterDelivery, brokerLease, "ack_dead_lettered") {
			return nil
		}
		if ackErr := w.Broker.Ack(execCtx, deadLetterDelivery); ackErr != nil {
			if w.leaseOwnershipLost(ackErr) {
				w.logLeaseLoss(deadLetterDelivery, "ack_dead_lettered", brokerLease)
				return nil
			}
			observability.MarkSpanError(span, ackErr)
			return ackErr
		}
		return nil
	default:
		if w.abandonIfLeaseLost(failedDelivery, brokerLease, "ack_failed_delivery") {
			return nil
		}
		if ackErr := w.Broker.Ack(execCtx, failedDelivery); ackErr != nil {
			if w.leaseOwnershipLost(ackErr) {
				w.logLeaseLoss(failedDelivery, "ack_failed_delivery", brokerLease)
				return nil
			}
			observability.MarkSpanError(span, ackErr)
			return ackErr
		}
		return nil
	}
}

func (w *Worker) recordExecution(duration time.Duration, failed bool) {
	if !w.Adaptive.Enabled {
		return
	}
	if w.runtimeState == nil {
		return
	}
	w.runtimeState.mu.Lock()
	defer w.runtimeState.mu.Unlock()
	w.runtimeState.window.executions++
	w.runtimeState.window.totalLatency += duration
	if failed {
		w.runtimeState.window.failed++
	}
}

func (w *Worker) consumerKey() string {
	poolName := w.PoolName
	if poolName == "" {
		poolName = w.Queue
	}
	return fmt.Sprintf("%s-%s", w.ConsumerID, poolName)
}

func (w *Worker) defaultPrefetch() int {
	if w.Adaptive.Enabled && w.Adaptive.MaxConcurrency > 0 {
		return w.Adaptive.MaxConcurrency
	}
	return w.Concurrency
}

func (w *Worker) minConcurrency() int {
	if w.Adaptive.Enabled && w.Adaptive.MinConcurrency > 0 {
		return w.Adaptive.MinConcurrency
	}
	return w.Concurrency
}

func (w *Worker) maxConcurrency() int {
	if w.Adaptive.Enabled && w.Adaptive.MaxConcurrency > 0 {
		return w.Adaptive.MaxConcurrency
	}
	return w.Concurrency
}

func (w *Worker) canReserve(state *workerState) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.running+len(state.pending) < minInt(w.Prefetch, state.effectiveConcurrency)
}

func (w *Worker) deliveryLeaseTTL(delivery broker.Delivery) time.Duration {
	ttl := delivery.Message.VisibilityTimeout
	if ttl <= 0 {
		ttl = w.LeaseTTL
	}
	return ttl
}

func (w *Worker) tryAcquireBudget(ctx context.Context, delivery broker.Delivery) (*TaskBudget, bool, error) {
	if w.BudgetManager == nil || len(w.TaskBudgets) == 0 {
		return nil, true, nil
	}

	budget, ok := w.TaskBudgets[delivery.Message.Name]
	if !ok {
		return nil, true, nil
	}

	acquired, err := w.BudgetManager.AcquireLease(ctx, budget.Budget, delivery.Execution.DeliveryID, budget.Tokens, w.deliveryLeaseTTL(delivery))
	if err != nil {
		return nil, false, err
	}
	if !acquired {
		w.Metrics.IncDependencyBudgetBlocked(budget.Budget)
		w.recordBudgetBlocked()
		return nil, false, nil
	}
	return &budget, true, nil
}

func (w *Worker) releaseBudget(ctx context.Context, budget *TaskBudget, delivery broker.Delivery) {
	if budget == nil || w.BudgetManager == nil {
		return
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := w.BudgetManager.ReleaseLease(releaseCtx, budget.Budget, delivery.Execution.DeliveryID); err != nil {
		w.Logger.Error(
			"dependency budget release failed",
			"pool", w.PoolName,
			"queue", w.Queue,
			"budget", budget.Budget,
			"delivery_id", delivery.Execution.DeliveryID,
			"error", err,
		)
	}
}

func (w *Worker) watchLeaseLoss(ctx context.Context, state *workerState, reserveWake, dispatchWake chan struct{}, entry *pendingDelivery) {
	if entry == nil || entry.brokerLease == nil {
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-entry.brokerLease.Done():
		if !entry.brokerLease.IsLost() {
			return
		}
	case <-entry.brokerLease.Lost():
	}

	if w.dropPendingIfLeaseLost(state, reserveWake, dispatchWake, entry) {
		return
	}
	if entry.cancelExecutionOnLeaseLoss() {
		w.logLeaseLoss(entry.delivery, "running_execution", entry.brokerLease)
	}
}

func (w *Worker) watchBudgetLeaseLoss(ctx context.Context, cancelExec context.CancelFunc, budgetLease *leaseHandle, budget TaskBudget) {
	if budgetLease == nil {
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-budgetLease.Done():
	case <-budgetLease.Lost():
	}

	if !budgetLease.IsLost() {
		return
	}
	w.Metrics.IncDependencyBudgetLeaseRenewFailure(budget.Budget)
	cancelExec()
}

func (w *Worker) dropPendingIfLeaseLost(state *workerState, reserveWake, dispatchWake chan struct{}, entry *pendingDelivery) bool {
	if entry == nil || entry.brokerLease == nil || !entry.brokerLease.IsLost() {
		return false
	}

	state.mu.Lock()
	index := indexPendingEntry(state.pending, entry.delivery.Execution.DeliveryID)
	if index < 0 {
		state.mu.Unlock()
		return false
	}
	stale := state.pending[index]
	state.pending = append(state.pending[:index], state.pending[index+1:]...)
	state.mu.Unlock()

	w.logLeaseLoss(stale.delivery, "pending_queue", stale.brokerLease)
	notify(reserveWake)
	notify(dispatchWake)
	return true
}

func (w *Worker) stopPendingReservations(state *workerState) {
	state.mu.Lock()
	pending := append([]*pendingDelivery(nil), state.pending...)
	state.pending = nil
	state.mu.Unlock()

	for _, entry := range pending {
		if entry == nil || entry.brokerLease == nil {
			continue
		}
		entry.brokerLease.Stop()
	}
}

func (w *Worker) abandonIfLeaseLost(delivery broker.Delivery, brokerLease *leaseHandle, phase string) bool {
	if brokerLease == nil || !brokerLease.IsLost() {
		return false
	}
	w.logLeaseLoss(delivery, phase, brokerLease)
	return true
}

func (w *Worker) logLeaseLoss(delivery broker.Delivery, phase string, brokerLease *leaseHandle) {
	err := error(nil)
	if brokerLease != nil {
		err = brokerLease.Err()
	}
	logging.WithDelivery(w.Logger, delivery).Warn(
		"delivery lease lost",
		"phase", phase,
		"error", err,
	)
}

func (w *Worker) leaseOwnershipLost(err error) bool {
	return errors.Is(err, broker.ErrDeliveryExpired) || errors.Is(err, broker.ErrStaleDelivery)
}

func (w *Worker) recordBudgetBlocked() {
	if !w.Adaptive.Enabled {
		return
	}
	if w.runtimeState == nil {
		return
	}
	w.runtimeState.mu.Lock()
	w.runtimeState.window.budgetBlocked++
	w.runtimeState.mu.Unlock()
}

func (w *Worker) publishAdaptiveState(ctx context.Context, _ *workerState, snapshot observability.AdaptivePoolSnapshot) {
	if w.Metrics != nil {
		w.Metrics.SetWorkerEffectiveConcurrency(w.PoolName, w.Queue, snapshot.EffectiveConcurrency)
	}
	if w.AdaptiveStore == nil {
		return
	}
	storeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := w.AdaptiveStore.StoreAdaptiveStatus(storeCtx, snapshot); err != nil {
		w.Logger.Debug("adaptive status store failed", "pool", w.PoolName, "queue", w.Queue, "error", err)
	}
}

func indexPendingEntry(deliveries []*pendingDelivery, deliveryID string) int {
	for i, delivery := range deliveries {
		if delivery.delivery.Execution.DeliveryID == deliveryID {
			return i
		}
	}
	return -1
}

func (d *pendingDelivery) setExecutionCancel(cancel context.CancelFunc) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.execCancel = cancel
	d.mu.Unlock()
}

func (d *pendingDelivery) clearExecutionCancel() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.execCancel = nil
	d.mu.Unlock()
}

func (d *pendingDelivery) cancelExecutionOnLeaseLoss() bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	cancel := d.execCancel
	d.mu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func transitionDelivery(delivery broker.Delivery, next tasks.State) (broker.Delivery, error) {
	current := tasks.State(delivery.Execution.State)
	if err := tasks.ValidateTransition(current, next); err != nil {
		return broker.Delivery{}, err
	}
	delivery.Execution.State = string(next)
	return delivery, nil
}
