package runtime

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/observability"
	"github.com/aminkbi/taskforge/internal/tasks"
)

func TestManagerRunsIsolatedQueueWorkers(t *testing.T) {
	t.Parallel()

	stub := newQueueBrokerStub(map[string][]broker.Delivery{
		"critical": {testDeliveryWithQueue("critical-1", "critical", "critical.task")},
		"bulk":     {testDeliveryWithQueue("bulk-1", "bulk", "bulk.task")},
	})

	var mu sync.Mutex
	processedByQueue := map[string][]string{}
	handler := HandlerFunc(func(_ context.Context, msg broker.TaskMessage) error {
		mu.Lock()
		processedByQueue[msg.Queue] = append(processedByQueue[msg.Queue], msg.ID)
		mu.Unlock()
		return nil
	})

	manager := &Manager{
		Workers: []*Worker{
			newQueueWorkerForTest(stub, "critical", handler),
			newQueueWorkerForTest(stub, "bulk", handler),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Run(ctx)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(processedByQueue["critical"]) == 1 && len(processedByQueue["bulk"]) == 1
		mu.Unlock()
		if done {
			cancel()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("manager.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager did not stop before timeout")
	}

	if got := processedByQueue["critical"]; len(got) != 1 || got[0] != "critical-1" {
		t.Fatalf("critical queue processed = %+v, want [critical-1]", got)
	}
	if got := processedByQueue["bulk"]; len(got) != 1 || got[0] != "bulk-1" {
		t.Fatalf("bulk queue processed = %+v, want [bulk-1]", got)
	}
}

func TestWorkerTaskTypeCapLeavesRoomForOtherTasks(t *testing.T) {
	t.Parallel()

	stub := newQueueBrokerStub(map[string][]broker.Delivery{
		"default": {
			testDeliveryWithQueue("shared-1", "default", "shared.task"),
			testDeliveryWithQueue("shared-2", "default", "shared.task"),
			testDeliveryWithQueue("other-1", "default", "other.task"),
		},
	})

	var currentShared int32
	var maxShared int32
	started := make(chan string, 3)
	releaseShared := make(chan struct{})
	handler := HandlerFunc(func(ctx context.Context, msg broker.TaskMessage) error {
		started <- msg.ID
		if msg.Name != "shared.task" {
			return nil
		}

		active := atomic.AddInt32(&currentShared, 1)
		for {
			observed := atomic.LoadInt32(&maxShared)
			if active <= observed || atomic.CompareAndSwapInt32(&maxShared, observed, active) {
				break
			}
		}
		defer atomic.AddInt32(&currentShared, -1)

		select {
		case <-releaseShared:
			return nil
		case <-ctx.Done():
			return nil
		}
	})

	worker := newQueueWorkerForTest(stub, "default", handler)
	worker.Concurrency = 2
	worker.Prefetch = 3
	worker.GlobalTaskLimiter = NewTaskTypeLimiter(map[string]int{"shared.task": 1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	first := waitForStartedTask(t, started)
	second := waitForStartedTask(t, started)
	startedIDs := map[string]bool{first: true, second: true}
	if !startedIDs["shared-1"] || !startedIDs["other-1"] {
		t.Fatalf("first two started tasks = %q, %q, want one shared task and one other task", first, second)
	}
	if got := atomic.LoadInt32(&maxShared); got != 1 {
		t.Fatalf("max shared concurrency = %d, want 1", got)
	}

	close(releaseShared)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("worker.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop before timeout")
	}
}

type queueBrokerStub struct {
	mu      sync.Mutex
	queues  map[string][]broker.Delivery
	acked   []broker.Delivery
	nacked  []broker.Delivery
	publish []broker.TaskMessage
}

func newQueueBrokerStub(queues map[string][]broker.Delivery) *queueBrokerStub {
	copied := make(map[string][]broker.Delivery, len(queues))
	for queue, deliveries := range queues {
		copied[queue] = append([]broker.Delivery(nil), deliveries...)
	}
	return &queueBrokerStub{queues: copied}
}

func (b *queueBrokerStub) Publish(_ context.Context, msg broker.TaskMessage, _ broker.PublishOptions) (broker.PublishResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.publish = append(b.publish, msg)
	return broker.PublishResult{Decision: broker.AdmissionDecisionAccepted, Queue: msg.Queue}, nil
}

func (b *queueBrokerStub) Reserve(_ context.Context, queue, _ string) (broker.Delivery, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	deliveries := b.queues[queue]
	if len(deliveries) == 0 {
		return broker.Delivery{}, broker.ErrNoTask
	}
	next := deliveries[0]
	b.queues[queue] = deliveries[1:]
	return next, nil
}

func (b *queueBrokerStub) Ack(_ context.Context, delivery broker.Delivery) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.acked = append(b.acked, delivery)
	return nil
}

func (b *queueBrokerStub) Nack(_ context.Context, delivery broker.Delivery, _ bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nacked = append(b.nacked, delivery)
	return nil
}

func (b *queueBrokerStub) ExtendLease(context.Context, broker.Delivery, time.Duration) error {
	return nil
}

func newQueueWorkerForTest(b broker.Broker, queue string, handler Handler) *Worker {
	return &Worker{
		Broker:      b,
		Handler:     handler,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:     observability.NewMetrics(),
		Clock:       clock.RealClock{},
		RetryPolicy: tasks.DefaultRetryPolicy(1),
		PoolName:    queue,
		Queue:       queue,
		ConsumerID:  "worker-test",
		LeaseTTL:    30 * time.Second,
		Concurrency: 1,
		Prefetch:    1,
	}
}

func testDeliveryWithQueue(id, queue, taskName string) broker.Delivery {
	now := time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC)
	return broker.Delivery{
		Message: broker.TaskMessage{
			ID:        id,
			Name:      taskName,
			Queue:     queue,
			CreatedAt: now.Add(-time.Minute),
		},
		Execution: broker.ExecutionMetadata{
			TaskID:          id,
			DeliveryID:      id + "-delivery",
			DeliveryCount:   1,
			FirstEnqueuedAt: now.Add(-time.Minute),
			LeasedAt:        now,
			LeaseExpiresAt:  now.Add(30 * time.Second),
			LeaseOwner:      "worker-test",
			State:           string(tasks.StateLeased),
		},
	}
}

func waitForStartedTask(t *testing.T, started <-chan string) string {
	t.Helper()

	select {
	case taskID := <-started:
		return taskID
	case <-time.After(time.Second):
		t.Fatal("task did not start before timeout")
		return ""
	}
}
