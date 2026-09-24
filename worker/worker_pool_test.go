package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/observability"
)

func TestManagerRunsIsolatedQueueWorkers(t *testing.T) {
	t.Parallel()

	stub := newQueueBrokerStub(map[string][]taskforge.Delivery{
		"critical": {testDeliveryWithQueue("critical-1", "critical", "critical.task")},
		"bulk":     {testDeliveryWithQueue("bulk-1", "bulk", "bulk.task")},
	})

	var mu sync.Mutex
	processedByQueue := map[string][]string{}
	handler := taskforge.HandlerFunc(func(_ context.Context, msg taskforge.Task) error {
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

func TestManagerDrainStopsReservingNewDeliveriesImmediately(t *testing.T) {
	t.Parallel()

	stub := newQueueBrokerStub(map[string][]taskforge.Delivery{
		"default": {
			testDeliveryWithQueue("task-1", "default", "default.task"),
			testDeliveryWithQueue("task-2", "default", "default.task"),
		},
	})

	started := make(chan string, 2)
	release := make(chan struct{})
	worker := newQueueWorkerForTest(stub, "default", taskforge.HandlerFunc(func(_ context.Context, msg taskforge.Task) error {
		started <- msg.ID
		<-release
		return nil
	}))

	manager := &Manager{
		Workers:         []*Worker{worker},
		ShutdownTimeout: 250 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Run(ctx)
	}()

	if first := waitForStartedTask(t, started); first != "task-1" {
		t.Fatalf("first started task = %q, want %q", first, "task-1")
	}

	cancel()
	time.Sleep(50 * time.Millisecond)

	snapshot, ok := worker.LifecycleSnapshot()
	if !ok {
		t.Fatal("LifecycleSnapshot() unavailable")
	}
	if snapshot.State != "draining" {
		t.Fatalf("worker lifecycle state = %q, want draining", snapshot.State)
	}

	select {
	case second := <-started:
		t.Fatalf("worker started a new delivery during drain: %q", second)
	case <-time.After(150 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("manager.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager did not stop before timeout")
	}
}

func TestManagerForcedShutdownReturnsAfterTimeout(t *testing.T) {
	t.Parallel()

	stub := newQueueBrokerStub(map[string][]taskforge.Delivery{
		"default": {
			testDeliveryWithQueue("task-1", "default", "default.task"),
		},
	})

	started := make(chan struct{}, 1)
	block := make(chan struct{})
	worker := newQueueWorkerForTest(stub, "default", taskforge.HandlerFunc(func(context.Context, taskforge.Task) error {
		started <- struct{}{}
		<-block
		return nil
	}))

	manager := &Manager{
		Workers:         []*Worker{worker},
		ShutdownTimeout: 50 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Run(ctx)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start before timeout")
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("manager.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager did not stop after shutdown timeout")
	}

	snapshot, ok := worker.LifecycleSnapshot()
	if !ok {
		t.Fatal("LifecycleSnapshot() unavailable")
	}
	if snapshot.State != "stopped" {
		t.Fatalf("worker lifecycle state = %q, want stopped", snapshot.State)
	}
	if snapshot.LastShutdownOutcome != "forced_timeout" {
		t.Fatalf("shutdown outcome = %q, want forced_timeout", snapshot.LastShutdownOutcome)
	}
	if snapshot.AbandonedDeliveries != 1 {
		t.Fatalf("abandoned deliveries = %v, want 1", snapshot.AbandonedDeliveries)
	}
}

func TestManagerForcedShutdownDoesNotDispatchPendingBufferedWork(t *testing.T) {
	t.Parallel()

	stub := newQueueBrokerStub(map[string][]taskforge.Delivery{
		"default": {
			testDeliveryWithQueue("task-1", "default", "shared.task"),
			testDeliveryWithQueue("task-2", "default", "shared.task"),
		},
	})

	started := make(chan string, 4)
	block := make(chan struct{})
	worker := newQueueWorkerForTest(stub, "default", taskforge.HandlerFunc(func(_ context.Context, msg taskforge.Task) error {
		started <- msg.ID
		if msg.ID == "task-1" {
			<-block
		}
		return nil
	}))
	worker.Concurrency = 2
	worker.Prefetch = 2
	worker.GlobalTaskLimiter = NewTaskTypeLimiter(map[string]int{"shared.task": 1})

	manager := &Manager{
		Workers:         []*Worker{worker},
		ShutdownTimeout: 50 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Run(ctx)
	}()

	if first := waitForStartedTask(t, started); first != "task-1" {
		t.Fatalf("first started task = %q, want %q", first, "task-1")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot, ok := worker.LifecycleSnapshot()
		if ok && snapshot.Pending == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	snapshot, ok := worker.LifecycleSnapshot()
	if !ok {
		t.Fatal("LifecycleSnapshot() unavailable")
	}
	if snapshot.Pending != 1 {
		t.Fatalf("pending deliveries before shutdown = %v, want 1", snapshot.Pending)
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("manager.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager did not stop after shutdown timeout")
	}

	select {
	case taskID := <-started:
		t.Fatalf("unexpected task started after forced shutdown: %q", taskID)
	case <-time.After(150 * time.Millisecond):
	}

	close(block)
}

func TestWorkerBudgetGatedTasksStayPendingUntilTokensFreeUp(t *testing.T) {
	t.Parallel()

	stub := newQueueBrokerStub(map[string][]taskforge.Delivery{
		"default": {
			testDeliveryWithQueue("shared-1", "default", "shared.task"),
			testDeliveryWithQueue("shared-2", "default", "shared.task"),
		},
	})

	budgets := &budgetManagerStub{
		capacity: map[string]int{"downstream": 1},
		held:     make(map[string]string),
	}
	started := make(chan string, 2)
	releaseShared := make(chan struct{})
	handler := taskforge.HandlerFunc(func(ctx context.Context, msg taskforge.Task) error {
		started <- msg.ID

		select {
		case <-releaseShared:
			return nil
		case <-ctx.Done():
			return nil
		}
	})

	worker := newQueueWorkerForTest(stub, "default", handler)
	worker.Concurrency = 2
	worker.Prefetch = 2
	worker.BudgetManager = budgets
	worker.TaskBudgets = map[string]TaskBudget{
		"shared.task": {
			Budget: "downstream",
			Tokens: 1,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	if first := waitForStartedTask(t, started); first != "shared-1" {
		t.Fatalf("first started task = %q, want %q", first, "shared-1")
	}
	select {
	case second := <-started:
		t.Fatalf("second task started before budget was released: %q", second)
	case <-time.After(150 * time.Millisecond):
	}

	close(releaseShared)

	if second := waitForStartedTask(t, started); second != "shared-2" {
		t.Fatalf("second started task = %q, want %q", second, "shared-2")
	}
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

func TestWorkerDropsPendingDeliveryWhenLeaseRenewalFails(t *testing.T) {
	t.Parallel()

	stub := newQueueBrokerStub(map[string][]taskforge.Delivery{
		"default": {
			testDeliveryWithQueue("shared-1", "default", "shared.task"),
			testDeliveryWithQueue("shared-2", "default", "shared.task"),
		},
	})
	stub.extendLeaseFunc = func(delivery taskforge.Delivery) error {
		if delivery.Execution.DeliveryID == "shared-2-delivery" {
			return taskforge.ErrDeliveryExpired
		}
		return nil
	}

	started := make(chan string, 2)
	releaseShared := make(chan struct{})
	handler := taskforge.HandlerFunc(func(ctx context.Context, msg taskforge.Task) error {
		started <- msg.ID
		select {
		case <-releaseShared:
			return nil
		case <-ctx.Done():
			return nil
		}
	})

	worker := newQueueWorkerForTest(stub, "default", handler)
	worker.Concurrency = 2
	worker.Prefetch = 2
	worker.LeaseTTL = 20 * time.Millisecond
	worker.GlobalTaskLimiter = NewTaskTypeLimiter(map[string]int{"shared.task": 1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	if first := waitForStartedTask(t, started); first != "shared-1" {
		t.Fatalf("first started task = %q, want %q", first, "shared-1")
	}

	time.Sleep(80 * time.Millisecond)
	close(releaseShared)

	select {
	case second := <-started:
		t.Fatalf("second task started after pending lease was lost: %q", second)
	case <-time.After(100 * time.Millisecond):
	}

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

func TestWorkerCancelsRunningTaskWhenLeaseRenewalFails(t *testing.T) {
	t.Parallel()

	stub := newQueueBrokerStub(map[string][]taskforge.Delivery{
		"default": {
			testDeliveryWithQueue("running-1", "default", "running.task"),
		},
	})
	stub.extendLeaseFunc = func(delivery taskforge.Delivery) error {
		if delivery.Execution.DeliveryID == "running-1-delivery" {
			return taskforge.ErrDeliveryExpired
		}
		return nil
	}

	canceled := make(chan struct{}, 1)
	handler := taskforge.HandlerFunc(func(ctx context.Context, msg taskforge.Task) error {
		<-ctx.Done()
		canceled <- struct{}{}
		return ctx.Err()
	})

	worker := newQueueWorkerForTest(stub, "default", handler)
	worker.LeaseTTL = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("handler was not canceled after lease loss")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("worker.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop before timeout")
	}

	if len(stub.acked) != 0 {
		t.Fatalf("Ack calls = %d, want 0", len(stub.acked))
	}
	if len(stub.nacked) != 0 {
		t.Fatalf("Nack calls = %d, want 0", len(stub.nacked))
	}
	if len(stub.publish) != 0 {
		t.Fatalf("Publish calls = %d, want 0", len(stub.publish))
	}
}

func TestWorkerResolvesTimedOutTaskAndContinuesProcessing(t *testing.T) {
	t.Parallel()

	first := testDeliveryWithQueue("timeout-1", "default", "timeout.task")
	second := testDeliveryWithQueue("after-timeout", "default", "success.task")
	broker := &contextAwareQueueBroker{
		queueBrokerStub: newQueueBrokerStub(map[string][]taskforge.Delivery{
			"default": {first, second},
		}),
		ackSignal: make(chan taskforge.Delivery, 2),
	}
	worker := newQueueWorkerForTest(broker, "default", taskforge.HandlerFunc(func(ctx context.Context, msg taskforge.Task) error {
		if msg.ID == first.Message.ID {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}))
	worker.LeaseTTL = 0
	worker.TaskTimeout = 20 * time.Millisecond
	worker.RetryPolicy = taskforge.DefaultRetryPolicy(3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- worker.Run(ctx)
	}()

	acknowledged := make([]taskforge.Delivery, 0, 2)
	for range 2 {
		select {
		case delivery := <-broker.ackSignal:
			acknowledged = append(acknowledged, delivery)
		case err := <-result:
			t.Fatalf("worker.Run() returned before resolving both tasks: %v", err)
		case <-time.After(time.Second):
			t.Fatal("worker did not acknowledge both the retry and following task")
		}
	}
	cancel()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("worker.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancellation")
	}

	if acknowledged[0].Execution.State != taskforge.StateRetryScheduled || acknowledged[1].Execution.State != taskforge.StateSucceeded {
		t.Fatalf("acknowledged states = [%s, %s], want retry_scheduled then succeeded", acknowledged[0].Execution.State, acknowledged[1].Execution.State)
	}
	broker.queueBrokerStub.mu.Lock()
	defer broker.queueBrokerStub.mu.Unlock()
	if len(broker.queueBrokerStub.publish) != 1 || broker.queueBrokerStub.publish[0].Attempt != 1 {
		t.Fatalf("published retries = %+v, want one retry at attempt 1", broker.queueBrokerStub.publish)
	}
	if len(broker.queueBrokerStub.nacked) != 0 {
		t.Fatalf("Nack calls = %d, want 0", len(broker.queueBrokerStub.nacked))
	}
}

func TestWorkerFatalErrorDrainsRunningTaskBeforeReturn(t *testing.T) {
	t.Parallel()

	stub := newQueueBrokerStub(map[string][]taskforge.Delivery{
		"default": {
			testDeliveryWithQueue("drain-1", "default", "drain.task"),
		},
	})
	stub.reserveFunc = func(queue string) (taskforge.Delivery, error) {
		stub.mu.Lock()
		defer stub.mu.Unlock()
		deliveries := stub.queues[queue]
		if len(deliveries) > 0 {
			next := deliveries[0]
			stub.queues[queue] = deliveries[1:]
			return next, nil
		}
		return taskforge.Delivery{}, errors.New("reserve boom")
	}

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	worker := newQueueWorkerForTest(stub, "default", taskforge.HandlerFunc(func(context.Context, taskforge.Task) error {
		started <- struct{}{}
		<-release
		return nil
	}))

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(context.Background())
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start before timeout")
	}

	select {
	case err := <-errCh:
		t.Fatalf("worker returned before running task drained: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-errCh:
		if err == nil || err.Error() != "worker reserve task: reserve boom" {
			t.Fatalf("worker.Run() error = %v, want reserve boom", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not return after running task drained")
	}
}

func TestManagerFatalErrorStopsSiblingWorkersBeforeReturn(t *testing.T) {
	t.Parallel()

	failingBroker := newQueueBrokerStub(map[string][]taskforge.Delivery{
		"critical": {
			testDeliveryWithQueue("critical-1", "critical", "critical.task"),
		},
	})
	failingBroker.reserveFunc = func(queue string) (taskforge.Delivery, error) {
		failingBroker.mu.Lock()
		defer failingBroker.mu.Unlock()
		deliveries := failingBroker.queues[queue]
		if len(deliveries) > 0 {
			next := deliveries[0]
			failingBroker.queues[queue] = deliveries[1:]
			return next, nil
		}
		return taskforge.Delivery{}, errors.New("reserve boom")
	}

	siblingBroker := newQueueBrokerStub(map[string][]taskforge.Delivery{
		"bulk": {
			testDeliveryWithQueue("bulk-1", "bulk", "bulk.task"),
		},
	})

	releaseCritical := make(chan struct{})
	releaseBulk := make(chan struct{})
	manager := &Manager{
		Workers: []*Worker{
			newQueueWorkerForTest(failingBroker, "critical", taskforge.HandlerFunc(func(context.Context, taskforge.Task) error {
				<-releaseCritical
				return nil
			})),
			newQueueWorkerForTest(siblingBroker, "bulk", taskforge.HandlerFunc(func(context.Context, taskforge.Task) error {
				<-releaseBulk
				return nil
			})),
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Run(context.Background())
	}()

	time.Sleep(150 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatalf("manager returned before sibling workers drained: %v", err)
	default:
	}

	close(releaseCritical)
	close(releaseBulk)

	select {
	case err := <-errCh:
		if err == nil || err.Error() != "worker reserve task: reserve boom" {
			t.Fatalf("manager.Run() error = %v, want reserve boom", err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager did not return after sibling workers drained")
	}
}

type budgetManagerStub struct {
	mu       sync.Mutex
	capacity map[string]int
	held     map[string]string
}

func (b *budgetManagerStub) AcquireLease(_ context.Context, budget, deliveryID string, tokens int, _ time.Duration) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if tokens != 1 {
		return false, nil
	}
	if _, ok := b.held[deliveryID]; ok {
		return true, nil
	}
	if len(b.held) >= b.capacity[budget] {
		return false, nil
	}
	b.held[deliveryID] = budget
	return true, nil
}

func (b *budgetManagerStub) RenewLease(context.Context, string, string, time.Duration) error {
	return nil
}

func (b *budgetManagerStub) ReleaseLease(_ context.Context, _ string, deliveryID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.held, deliveryID)
	return nil
}

type queueBrokerStub struct {
	mu              sync.Mutex
	queues          map[string][]taskforge.Delivery
	acked           []taskforge.Delivery
	nacked          []taskforge.Delivery
	publish         []taskforge.Task
	reserveFunc     func(queue string) (taskforge.Delivery, error)
	extendLeaseFunc func(taskforge.Delivery) error
}

type contextAwareQueueBroker struct {
	*queueBrokerStub
	ackSignal chan taskforge.Delivery
}

func (b *contextAwareQueueBroker) Publish(ctx context.Context, msg taskforge.Task, opts taskforge.PublishOptions) (taskforge.PublishResult, error) {
	if err := ctx.Err(); err != nil {
		return taskforge.PublishResult{}, err
	}
	return b.queueBrokerStub.Publish(ctx, msg, opts)
}

func (b *contextAwareQueueBroker) Ack(ctx context.Context, delivery taskforge.Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.queueBrokerStub.Ack(ctx, delivery); err != nil {
		return err
	}
	b.ackSignal <- delivery
	return nil
}

func (b *contextAwareQueueBroker) Nack(ctx context.Context, delivery taskforge.Delivery, requeue bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.queueBrokerStub.Nack(ctx, delivery, requeue)
}

func newQueueBrokerStub(queues map[string][]taskforge.Delivery) *queueBrokerStub {
	copied := make(map[string][]taskforge.Delivery, len(queues))
	for queue, deliveries := range queues {
		copied[queue] = append([]taskforge.Delivery(nil), deliveries...)
	}
	return &queueBrokerStub{queues: copied}
}

func (b *queueBrokerStub) Publish(_ context.Context, msg taskforge.Task, _ taskforge.PublishOptions) (taskforge.PublishResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.publish = append(b.publish, msg)
	return taskforge.PublishResult{Decision: taskforge.AdmissionDecisionAccepted, Queue: msg.Queue}, nil
}

func (b *queueBrokerStub) Reserve(_ context.Context, queue, _ string) (taskforge.Delivery, error) {
	if b.reserveFunc != nil {
		return b.reserveFunc(queue)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	deliveries := b.queues[queue]
	if len(deliveries) == 0 {
		return taskforge.Delivery{}, taskforge.ErrNoTask
	}
	next := deliveries[0]
	b.queues[queue] = deliveries[1:]
	return next, nil
}

func (b *queueBrokerStub) Ack(_ context.Context, delivery taskforge.Delivery) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.acked = append(b.acked, delivery)
	return nil
}

func (b *queueBrokerStub) Nack(_ context.Context, delivery taskforge.Delivery, _ bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nacked = append(b.nacked, delivery)
	return nil
}

func (b *queueBrokerStub) ExtendLease(_ context.Context, delivery taskforge.Delivery, _ time.Duration) error {
	if b.extendLeaseFunc != nil {
		return b.extendLeaseFunc(delivery)
	}
	return nil
}

func newQueueWorkerForTest(b taskforge.Broker, queue string, handler taskforge.Handler) *Worker {
	return &Worker{
		Broker:      b,
		Handler:     handler,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:     observability.NewMetrics(),
		Clock:       clock.RealClock{},
		RetryPolicy: taskforge.DefaultRetryPolicy(1),
		PoolName:    queue,
		Queue:       queue,
		ConsumerID:  "worker-test",
		LeaseTTL:    30 * time.Second,
		Concurrency: 1,
		Prefetch:    1,
	}
}

func testDeliveryWithQueue(id, queue, taskName string) taskforge.Delivery {
	now := time.Now().UTC()
	return taskforge.Delivery{
		Message: taskforge.Task{
			ID:        id,
			Name:      taskName,
			Queue:     queue,
			CreatedAt: now.Add(-time.Minute),
		},
		Execution: taskforge.ExecutionMetadata{
			TaskID:          id,
			DeliveryID:      id + "-delivery",
			DeliveryCount:   1,
			FirstEnqueuedAt: now.Add(-time.Minute),
			LeasedAt:        time.Time{},
			LeaseExpiresAt:  time.Time{},
			LeaseOwner:      "worker-test",
			State:           taskforge.StateLeased,
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
