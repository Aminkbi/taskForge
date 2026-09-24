package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
)

type batchLeaseBroker struct {
	*stubBroker
	calls chan []taskforge.Delivery
	err   error
}

func (b *batchLeaseBroker) ExtendLeases(ctx context.Context, renewals []taskforge.Delivery) ([]error, error) {
	select {
	case b.calls <- renewals:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	errs := make([]error, len(renewals))
	if len(errs) > 0 {
		errs[0] = b.err
	}
	return errs, nil
}

func TestLeaseCoordinatorBatchesAndReportsLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leaseErr := errors.New("renew failed")
	broker := &batchLeaseBroker{stubBroker: &stubBroker{}, calls: make(chan []taskforge.Delivery, 1), err: leaseErr}
	coordinator := newLeaseCoordinator(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), broker)
	first := testDelivery()
	second := testDelivery()
	second.Message.ID = "second-task"
	second.Execution.DeliveryID = "second-delivery"
	firstHandle, err := coordinator.register(first, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	secondHandle, err := coordinator.register(second, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { coordinator.run(); close(done) }()
	select {
	case renewals := <-broker.calls:
		if len(renewals) != 2 {
			t.Fatalf("batch size = %d, want 2", len(renewals))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batch renewal")
	}
	select {
	case <-firstHandle.Lost():
		if !errors.Is(firstHandle.Err(), leaseErr) {
			t.Fatalf("lease error = %v, want %v", firstHandle.Err(), leaseErr)
		}
	case <-secondHandle.Lost():
		if !errors.Is(secondHandle.Err(), leaseErr) {
			t.Fatalf("lease error = %v, want %v", secondHandle.Err(), leaseErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for failed lease")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not stop")
	}
}

func TestDeliveryLeaseTTLPrefersBrokerMetadata(t *testing.T) {
	now := time.Now().UTC()
	delivery := testDelivery()
	delivery.Execution.LeasedAt = now
	delivery.Execution.LeaseExpiresAt = now.Add(123 * time.Millisecond)
	worker := &Worker{LeaseTTL: time.Hour}
	if got := worker.deliveryLeaseTTL(delivery); got != 123*time.Millisecond {
		t.Fatalf("delivery lease TTL = %v, want 123ms", got)
	}
}

type reserveFailureBroker struct {
	*stubBroker
	delivery      taskforge.Delivery
	calls         atomic.Int32
	secondStarted chan struct{}
	allowFailure  chan struct{}
}

func (b *reserveFailureBroker) Reserve(ctx context.Context, _, _ string) (taskforge.Delivery, error) {
	if b.calls.Add(1) == 1 {
		return b.delivery, nil
	}
	select {
	case b.secondStarted <- struct{}{}:
	default:
	}
	select {
	case <-b.allowFailure:
		return taskforge.Delivery{}, errors.New("reserve failed")
	case <-ctx.Done():
		return taskforge.Delivery{}, ctx.Err()
	}
}

func TestWorkerReserveFailureDoesNotCancelActiveExecution(t *testing.T) {
	broker := &reserveFailureBroker{
		stubBroker:    &stubBroker{},
		delivery:      testDelivery(),
		secondStarted: make(chan struct{}, 1),
		allowFailure:  make(chan struct{}),
	}
	worker := newTestWorker(broker, &stubDeadLetter{}, taskforge.HandlerFunc(func(ctx context.Context, _ taskforge.Task) error {
		return nil
	}))
	worker.Prefetch = 2
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	worker.Handler = taskforge.HandlerFunc(func(ctx context.Context, _ taskforge.Task) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			close(canceled)
			return ctx.Err()
		}
	})
	ctx := t.Context()
	result := make(chan error, 1)
	go func() { result <- worker.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	select {
	case <-broker.secondStarted:
	case <-time.After(time.Second):
		t.Fatal("reserve failure was not reached")
	}
	close(broker.allowFailure)
	select {
	case <-canceled:
		t.Fatal("reserve failure canceled active execution")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "reserve failed") {
			t.Fatalf("Worker.Run() error = %v, want reserve failure", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after reserve failure")
	}
}

type expiringThenHealthyBroker struct {
	*stubBroker
	calls    atomic.Int32
	reserved chan struct{}
}

func (b *expiringThenHealthyBroker) Reserve(context.Context, string, string) (taskforge.Delivery, error) {
	call := b.calls.Add(1)
	select {
	case b.reserved <- struct{}{}:
	default:
	}
	if call == 1 {
		delivery := testDelivery()
		delivery.Execution.LeasedAt = time.Now().Add(-time.Second)
		delivery.Execution.LeaseExpiresAt = time.Now().Add(-time.Millisecond)
		return delivery, nil
	}
	delivery := testDelivery()
	delivery.Message.ID = "healthy-after-expired"
	delivery.Execution.TaskID = delivery.Message.ID
	delivery.Execution.DeliveryID = "healthy-after-expired-delivery"
	delivery.Execution.LeasedAt = time.Now()
	delivery.Execution.LeaseExpiresAt = time.Now().Add(time.Minute)
	return delivery, nil
}

func TestReserveLoopContinuesAfterExpiredRegistration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	broker := &expiringThenHealthyBroker{stubBroker: &stubBroker{}, reserved: make(chan struct{}, 4)}
	worker := newTestWorker(broker, &stubDeadLetter{}, taskforge.HandlerFunc(func(context.Context, taskforge.Task) error { return nil }))
	state := &workerState{effectiveConcurrency: 1, pending: make([]*pendingDelivery, 0, 1), workerID: "test", lifecycleState: "accepting"}
	coordinator := newLeaseCoordinator(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), broker)
	reserveWake := make(chan struct{}, 1)
	dispatchWake := make(chan struct{}, 1)
	loopDone := make(chan error, 1)
	go func() { loopDone <- worker.reserveLoop(ctx, ctx, coordinator, state, reserveWake, dispatchWake) }()
	go coordinator.run()
	for range 2 {
		select {
		case <-broker.reserved:
		case <-time.After(time.Second):
			t.Fatal("reserve loop did not continue after expired delivery")
		}
	}
	deadline := time.After(time.Second)
	for {
		state.mu.Lock()
		pending := len(state.pending)
		state.mu.Unlock()
		if pending == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("pending deliveries = %d, want 1", pending)
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-loopDone:
		if err != nil {
			t.Fatalf("reserveLoop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reserve loop did not stop")
	}
	select {
	case <-coordinator.done:
	case <-time.After(time.Second):
		t.Fatal("lease coordinator did not stop")
	}
}

func TestLeasePendingExpiryIsNonDestructiveUntilCoordinatorFences(t *testing.T) {
	delivery := testDelivery()
	delivery.Execution.LeasedAt = time.Now().Add(-time.Second)
	delivery.Execution.LeaseExpiresAt = time.Now().Add(-time.Millisecond)
	handle := &leaseHandle{doneCh: make(chan struct{}), lostCh: make(chan struct{})}
	entry := &pendingDelivery{delivery: delivery, brokerLease: handle}
	if !leasePendingExpired(entry, time.Now()) {
		t.Fatal("expired pending delivery was not detected")
	}
	if entry.brokerLease.IsLost() {
		t.Fatal("expiry check prematurely marked the lease lost")
	}
}

func TestLeaseCoordinatorRefreshUpdatesHandleExpiry(t *testing.T) {
	coordinator := newLeaseCoordinator(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), &stubBroker{})
	delivery := testDelivery()
	delivery.Execution.LeasedAt = time.Now()
	delivery.Execution.LeaseExpiresAt = time.Now().Add(10 * time.Millisecond)
	handle, err := coordinator.register(delivery, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	coordinator.refresh(handle, time.Second)
	if handle.leaseExpired(time.Now()) {
		t.Fatal("refreshed lease handle is still expired")
	}
}

func TestLeaseBatchTimeoutUsesLongestSafeLease(t *testing.T) {
	now := time.Now()
	timeout := leaseBatchTimeout([]leaseRenewal{
		{ttl: 20 * time.Millisecond, expiresAt: now.Add(20 * time.Millisecond)},
		{ttl: time.Second, expiresAt: now.Add(time.Second)},
	})
	if timeout < 500*time.Millisecond {
		t.Fatalf("lease batch timeout = %v, want time for the longer lease", timeout)
	}
}

func TestLeaseRenewalDelayUsesRemainingLifetime(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		ttl       time.Duration
		remaining time.Duration
		want      time.Duration
	}{
		{name: "full lifetime", ttl: 30 * time.Second, remaining: 30 * time.Second, want: 15 * time.Second},
		{name: "near expiry", ttl: 30 * time.Second, remaining: 4 * time.Millisecond, want: 2 * time.Millisecond},
		{name: "sub millisecond", ttl: 30 * time.Second, remaining: time.Millisecond, want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := leaseRenewalDelay(test.ttl, test.remaining); got != test.want {
				t.Fatalf("leaseRenewalDelay(%v, %v) = %v, want %v", test.ttl, test.remaining, got, test.want)
			}
		})
	}
}

func TestLeaseCoordinatorRejectsExpiredMetadata(t *testing.T) {
	delivery := testDelivery()
	delivery.Execution.LeasedAt = time.Now().Add(-time.Second)
	delivery.Execution.LeaseExpiresAt = time.Now().Add(-time.Millisecond)
	coordinator := newLeaseCoordinator(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), &stubBroker{})
	if _, err := coordinator.register(delivery, time.Second); !errors.Is(err, errLeaseAlreadyExpired) {
		t.Fatalf("register() error = %v, want %v", err, errLeaseAlreadyExpired)
	}
}

func TestLeaseCoordinatorRejectsRegistrationAfterStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	coordinator := newLeaseCoordinator(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), &stubBroker{})
	go coordinator.run()
	cancel()
	select {
	case <-coordinator.done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not stop")
	}
	if _, err := coordinator.register(testDelivery(), time.Second); !errors.Is(err, errLeaseCoordinatorStopped) {
		t.Fatalf("register() error = %v, want %v", err, errLeaseCoordinatorStopped)
	}
}

func TestLeaseCoordinatorBoundsRenewalBatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := &batchLeaseBroker{stubBroker: &stubBroker{}, calls: make(chan []taskforge.Delivery, 8)}
	coordinator := newLeaseCoordinator(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), broker)
	for index := range maxLeaseRenewalBatchSize + 5 {
		delivery := testDelivery()
		delivery.Message.ID = fmt.Sprintf("bounded-%d", index)
		delivery.Execution.DeliveryID = fmt.Sprintf("bounded-delivery-%d", index)
		if _, err := coordinator.register(delivery, 20*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	go coordinator.run()
	var first, second []taskforge.Delivery
	select {
	case first = <-broker.calls:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first renewal batch")
	}
	select {
	case second = <-broker.calls:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second renewal batch")
	}
	if len(first) != maxLeaseRenewalBatchSize || len(second) != 5 {
		t.Fatalf("batch sizes = %d,%d, want %d,5", len(first), len(second), maxLeaseRenewalBatchSize)
	}
	cancel()
	select {
	case <-coordinator.done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not stop")
	}
}

type concurrentScalarLeaseBroker struct {
	*stubBroker
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (b *concurrentScalarLeaseBroker) ExtendLease(ctx context.Context, _ taskforge.Delivery, _ time.Duration) error {
	b.calls.Add(1)
	select {
	case b.started <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestLeaseCoordinatorBoundsScalarFallbackConcurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := &concurrentScalarLeaseBroker{
		stubBroker: &stubBroker{},
		started:    make(chan struct{}, 2),
		release:    make(chan struct{}),
	}
	coordinator := newLeaseCoordinator(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), broker)
	for index := range 2 {
		delivery := testDelivery()
		delivery.Message.ID = fmt.Sprintf("scalar-%d", index)
		delivery.Execution.DeliveryID = fmt.Sprintf("scalar-delivery-%d", index)
		if _, err := coordinator.register(delivery, 500*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	go coordinator.run()
	for range 2 {
		select {
		case <-broker.started:
		case <-time.After(time.Second):
			t.Fatal("scalar fallback did not start concurrently")
		}
	}
	if broker.calls.Load() != 2 {
		t.Fatalf("scalar calls = %d, want 2", broker.calls.Load())
	}
	close(broker.release)
	cancel()
	select {
	case <-coordinator.done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not stop")
	}
}

type blockingBatchLeaseBroker struct {
	*stubBroker
	started  chan struct{}
	canceled chan struct{}
}

func (b *blockingBatchLeaseBroker) ExtendLeases(ctx context.Context, _ []taskforge.Delivery) ([]error, error) {
	close(b.started)
	<-ctx.Done()
	close(b.canceled)
	return nil, ctx.Err()
}

func TestLeaseCoordinatorCancelsInFlightBatchBeforeReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := &blockingBatchLeaseBroker{stubBroker: &stubBroker{}, started: make(chan struct{}), canceled: make(chan struct{})}
	coordinator := newLeaseCoordinator(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), broker)
	handle, err := coordinator.register(testDelivery(), 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	go coordinator.run()
	select {
	case <-broker.started:
	case <-time.After(time.Second):
		t.Fatal("batch renewal did not start")
	}
	cancel()
	select {
	case <-coordinator.done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not join after cancellation")
	}
	select {
	case <-broker.canceled:
	case <-time.After(time.Second):
		t.Fatal("in-flight batch did not observe cancellation")
	}
	if handle.IsLost() {
		t.Fatalf("shutdown unexpectedly marked lease lost: %v", handle.Err())
	}
}

type malformedBatchLeaseBroker struct {
	*stubBroker
	calls atomic.Int32
}

func (b *malformedBatchLeaseBroker) ExtendLeases(context.Context, []taskforge.Delivery) ([]error, error) {
	return nil, errors.New("malformed batch response")
}

func (b *malformedBatchLeaseBroker) ExtendLease(context.Context, taskforge.Delivery, time.Duration) error {
	b.calls.Add(1)
	return nil
}

func TestLeaseCoordinatorFallsBackAfterMalformedBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := &malformedBatchLeaseBroker{stubBroker: &stubBroker{}}
	coordinator := newLeaseCoordinator(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), broker)
	for index := range 2 {
		delivery := testDelivery()
		delivery.Message.ID = fmt.Sprintf("malformed-%d", index)
		delivery.Execution.DeliveryID = fmt.Sprintf("malformed-delivery-%d", index)
		if _, err := coordinator.register(delivery, 500*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	go coordinator.run()
	deadline := time.After(time.Second)
	for broker.calls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("scalar fallback was not used")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case <-coordinator.done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not stop")
	}
}

type workerBlockingBatchBroker struct {
	*stubBroker
	delivery taskforge.Delivery
	reserved atomic.Bool
	started  chan struct{}
	canceled chan struct{}
}

func (b *workerBlockingBatchBroker) Reserve(context.Context, string, string) (taskforge.Delivery, error) {
	if b.reserved.CompareAndSwap(false, true) {
		return b.delivery, nil
	}
	return taskforge.Delivery{}, taskforge.ErrNoTask
}

func (b *workerBlockingBatchBroker) ExtendLeases(ctx context.Context, _ []taskforge.Delivery) ([]error, error) {
	close(b.started)
	<-ctx.Done()
	close(b.canceled)
	return nil, ctx.Err()
}

func TestWorkerRunJoinsLeaseCoordinator(t *testing.T) {
	delivery := testDelivery()
	delivery.Message.ID = "worker-join"
	delivery.Execution.DeliveryID = "worker-join-delivery"
	broker := &workerBlockingBatchBroker{
		stubBroker: &stubBroker{},
		delivery:   delivery,
		started:    make(chan struct{}),
		canceled:   make(chan struct{}),
	}
	worker, err := New(Options{
		Broker:     broker,
		DeadLetter: &stubDeadLetter{},
		Handler: taskforge.HandlerFunc(func(ctx context.Context, _ taskforge.Task) error {
			<-ctx.Done()
			return ctx.Err()
		}),
		LeaseTTL:    50 * time.Millisecond,
		Concurrency: 1,
		Prefetch:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- worker.Run(ctx) }()
	select {
	case <-broker.started:
	case <-time.After(time.Second):
		t.Fatal("worker lease batch did not start")
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Worker.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Worker.Run() returned before lease coordinator shutdown")
	}
	select {
	case <-broker.canceled:
	case <-time.After(time.Second):
		t.Fatal("lease batch did not observe worker cancellation")
	}
}
