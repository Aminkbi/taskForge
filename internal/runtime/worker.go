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
	Broker       broker.Broker
	DeadLetter   dlq.Publisher
	Handler      Handler
	Logger       *slog.Logger
	Metrics      *observability.Metrics
	Clock        clock.Clock
	RetryPolicy  tasks.RetryPolicy
	Queue        string
	ConsumerID   string
	PollInterval time.Duration
	LeaseTTL     time.Duration
	Concurrency  int
}

func (w *Worker) Run(ctx context.Context) error {
	if w.Concurrency < 1 {
		return fmt.Errorf("worker concurrency must be >= 1")
	}

	w.Logger.Info("worker runtime started", "queue", w.Queue, "concurrency", w.Concurrency)

	var wg sync.WaitGroup
	errCh := make(chan error, w.Concurrency)
	for i := 0; i < w.Concurrency; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			if err := w.loop(ctx, workerIndex); err != nil {
				errCh <- err
			}
		}(i)
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

func (w *Worker) loop(ctx context.Context, workerIndex int) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		lease, msg, err := w.Broker.Reserve(ctx, w.Queue, fmt.Sprintf("%s-%d", w.ConsumerID, workerIndex))
		if err != nil {
			switch {
			case errors.Is(err, broker.ErrNoTask):
				timer := time.NewTimer(w.PollInterval)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil
				case <-timer.C:
					continue
				}
			case errors.Is(err, context.Canceled):
				return nil
			default:
				return fmt.Errorf("worker reserve task: %w", err)
			}
		}

		w.Metrics.TasksReservedTotal.Inc()
		if err := w.processTask(ctx, lease, msg); err != nil {
			return err
		}
	}
}

func (w *Worker) processTask(ctx context.Context, lease broker.Lease, msg broker.TaskMessage) error {
	execCtx := ctx
	cancelExec := func() {}
	if msg.Timeout != nil && *msg.Timeout > 0 {
		execCtx, cancelExec = context.WithTimeout(ctx, *msg.Timeout)
	}
	defer cancelExec()

	ttl := msg.VisibilityTimeout
	if ttl <= 0 {
		ttl = w.LeaseTTL
	}
	stopLease := startLeaseExtender(execCtx, w.Logger, w.Broker, lease, ttl)
	defer stopLease()

	w.Metrics.WorkerActiveTasks.Inc()
	started := time.Now()
	err := w.Handler.HandleTask(execCtx, msg)
	duration := time.Since(started).Seconds()
	w.Metrics.WorkerActiveTasks.Dec()

	if err == nil {
		w.Metrics.TasksCompletedTotal.Inc()
		w.Metrics.TaskExecutionDuration.WithLabelValues(msg.Name, "succeeded").Observe(duration)
		return w.Broker.Ack(ctx, lease)
	}

	w.Logger.Error("task execution failed", "task_id", msg.ID, "task_name", msg.Name, "attempt", msg.Attempt, "error", err)
	w.Metrics.TasksFailedTotal.Inc()
	w.Metrics.TaskExecutionDuration.WithLabelValues(msg.Name, "failed").Observe(duration)

	action, next := decideOutcome(msg, w.RetryPolicy, w.Clock)
	switch action {
	case outcomeRetry:
		if publishErr := w.Broker.Publish(ctx, next); publishErr != nil {
			if nackErr := w.Broker.Nack(ctx, lease, true); nackErr != nil {
				return errors.Join(fmt.Errorf("publish retry task: %w", publishErr), fmt.Errorf("nack original task: %w", nackErr))
			}
			return fmt.Errorf("publish retry task: %w", publishErr)
		}
		w.Metrics.TasksRetriedTotal.Inc()
		return w.Broker.Ack(ctx, lease)
	case outcomeDeadLetter:
		if w.DeadLetter != nil {
			if dlqErr := w.DeadLetter.PublishDeadLetter(ctx, msg, err.Error()); dlqErr != nil {
				if nackErr := w.Broker.Nack(ctx, lease, true); nackErr != nil {
					return errors.Join(fmt.Errorf("publish dead-letter task: %w", dlqErr), fmt.Errorf("nack original task: %w", nackErr))
				}
				return fmt.Errorf("publish dead-letter task: %w", dlqErr)
			}
			w.Metrics.TasksDeadLetteredTotal.Inc()
		}
		return w.Broker.Ack(ctx, lease)
	default:
		return w.Broker.Ack(ctx, lease)
	}
}
