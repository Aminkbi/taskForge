package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestWorkerProcessTaskAcksSucceededDelivery(t *testing.T) {
	t.Parallel()

	b := &stubBroker{}
	w := newTestWorker(b, nil, taskforge.HandlerFunc(func(context.Context, taskforge.Task) error {
		return nil
	}))
	delivery := testDelivery()

	if err := w.processTask(context.Background(), delivery, nil); err != nil {
		t.Fatalf("processTask() error = %v", err)
	}
	if len(b.acked) != 1 {
		t.Fatalf("Ack calls = %d, want 1", len(b.acked))
	}
	if got := b.acked[0].Execution.State; got != taskforge.StateSucceeded {
		t.Fatalf("Ack state = %q, want %q", got, taskforge.StateSucceeded)
	}
}

func TestWorkerProcessTaskRecordsRunningAndTerminalState(t *testing.T) {
	t.Parallel()

	stateStore := &stubStateStore{}
	b := &stubBroker{}
	w := newTestWorker(b, nil, taskforge.HandlerFunc(func(context.Context, taskforge.Task) error {
		return nil
	}))
	w.StateStore = stateStore

	if err := w.processTask(context.Background(), testDelivery(), nil); err != nil {
		t.Fatalf("processTask() error = %v", err)
	}

	if len(stateStore.deliveryStates) != 2 {
		t.Fatalf("state writes = %d, want 2", len(stateStore.deliveryStates))
	}
	if stateStore.deliveryStates[0] != taskforge.StateRunning || stateStore.deliveryStates[1] != taskforge.StateSucceeded {
		t.Fatalf("state writes = %+v, want running then succeeded", stateStore.deliveryStates)
	}
}

func TestWorkerProcessTaskAppliesPoolTimeoutAndTaskOverride(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		poolTimeout time.Duration
		taskTimeout *time.Duration
		wantUpper   time.Duration
	}{
		{name: "pool default", poolTimeout: time.Minute, wantUpper: time.Minute},
		{name: "task override", poolTimeout: time.Minute, taskTimeout: durationPointer(5 * time.Second), wantUpper: 5 * time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var remaining time.Duration
			w := newTestWorker(&stubBroker{}, nil, taskforge.HandlerFunc(func(ctx context.Context, _ taskforge.Task) error {
				deadline, ok := ctx.Deadline()
				if !ok {
					t.Fatal("handler context has no deadline")
				}
				remaining = time.Until(deadline)
				return nil
			}))
			w.TaskTimeout = test.poolTimeout
			delivery := testDelivery()
			delivery.Message.Timeout = test.taskTimeout

			if err := w.processTask(context.Background(), delivery, nil); err != nil {
				t.Fatalf("processTask() error = %v", err)
			}
			if remaining <= 0 || remaining > test.wantUpper {
				t.Fatalf("handler deadline remaining = %v, want within (0, %v]", remaining, test.wantUpper)
			}
		})
	}
}

func durationPointer(value time.Duration) *time.Duration { return &value }

func TestWorkerProcessTaskRetriesFailedTask(t *testing.T) {
	t.Parallel()

	b := &stubBroker{}
	w := newTestWorker(b, nil, taskforge.HandlerFunc(func(context.Context, taskforge.Task) error {
		return taskforge.Retryable(errors.New("boom"))
	}))
	w.Clock = fixedClock{now: time.Date(2026, 4, 12, 12, 0, 0, 0, time.UTC)}
	w.RetryPolicy = taskforge.DefaultRetryPolicy(3)

	delivery := testDelivery()
	if err := w.processTask(context.Background(), delivery, nil); err != nil {
		t.Fatalf("processTask() error = %v", err)
	}
	if len(b.published) != 1 {
		t.Fatalf("Publish calls = %d, want 1", len(b.published))
	}
	if b.published[0].Attempt != 1 {
		t.Fatalf("Published attempt = %d, want 1", b.published[0].Attempt)
	}
	if b.published[0].ETA == nil {
		t.Fatalf("Published ETA is nil")
	}
	if got, want := b.publishOpts[0].DeduplicationKey, "retry:"+delivery.OwnershipKey(); got != want {
		t.Fatalf("Publish deduplication key = %q, want %q", got, want)
	}
	if got := b.published[0].Headers["last_error"]; got != "boom" {
		t.Fatalf("Published last_error = %q, want %q", got, "boom")
	}
	if len(b.acked) != 1 {
		t.Fatalf("Ack calls = %d, want 1", len(b.acked))
	}
	if got := b.acked[0].Execution.State; got != taskforge.StateRetryScheduled {
		t.Fatalf("Ack state = %q, want %q", got, taskforge.StateRetryScheduled)
	}
	if got := b.acked[0].Execution.LastError; got != "boom" {
		t.Fatalf("Ack last error = %q, want %q", got, "boom")
	}
}

func TestWorkerProcessTaskDeadLettersFailedTask(t *testing.T) {
	t.Parallel()

	b := &stubBroker{}
	deadLetters := &stubDeadLetter{}
	w := newTestWorker(b, deadLetters, taskforge.HandlerFunc(func(context.Context, taskforge.Task) error {
		return taskforge.Permanent(errors.New("boom"))
	}))
	w.Clock = fixedClock{now: time.Date(2026, 4, 12, 12, 0, 0, 0, time.UTC)}
	w.RetryPolicy = taskforge.DefaultRetryPolicy(1)

	delivery := testDelivery()
	if err := w.processTask(context.Background(), delivery, nil); err != nil {
		t.Fatalf("processTask() error = %v", err)
	}
	if len(deadLetters.envelopes) != 1 {
		t.Fatalf("dead-letter calls = %d, want 1", len(deadLetters.envelopes))
	}
	if got := deadLetters.envelopes[0].LastError; got != "boom" {
		t.Fatalf("dead-letter last_error = %q, want %q", got, "boom")
	}
	if got := deadLetters.envelopes[0].FailureClass; got != taskforge.FailureClassPermanent {
		t.Fatalf("dead-letter failure class = %q, want %q", got, taskforge.FailureClassPermanent)
	}
	if len(b.acked) != 1 {
		t.Fatalf("Ack calls = %d, want 1", len(b.acked))
	}
	if got := b.acked[0].Execution.State; got != taskforge.StateDeadLettered {
		t.Fatalf("Ack state = %q, want %q", got, taskforge.StateDeadLettered)
	}
}

func TestWorkerDoesNotAcknowledgeBeforeReplacementPublishSucceeds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		broker        *stubBroker
		deadLetters   taskforge.DeadLetterPublisher
		handlerError  error
		wantErrorPart string
	}{
		{
			name:          "dead letter",
			broker:        &stubBroker{},
			deadLetters:   failingDeadLetter{err: errors.New("dlq unavailable")},
			handlerError:  taskforge.Permanent(errors.New("boom")),
			wantErrorPart: "dlq unavailable",
		},
		{
			name:          "retry",
			broker:        &stubBroker{publishErr: errors.New("retry unavailable")},
			handlerError:  taskforge.Retryable(errors.New("boom")),
			wantErrorPart: "retry unavailable",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := newTestWorker(test.broker, test.deadLetters, taskforge.HandlerFunc(func(context.Context, taskforge.Task) error {
				return test.handlerError
			}))

			err := w.processTask(context.Background(), testDelivery(), nil)
			if err == nil || !strings.Contains(err.Error(), test.wantErrorPart) {
				t.Fatalf("processTask() error = %v, want %q", err, test.wantErrorPart)
			}
			if len(test.broker.acked) != 0 {
				t.Fatalf("Ack calls = %d, want no acknowledgement before replacement publish", len(test.broker.acked))
			}
			if len(test.broker.nacked) != 1 {
				t.Fatalf("Nack calls = %d, want source delivery requeued", len(test.broker.nacked))
			}
		})
	}
}

func TestWorkerProcessTaskDeadLettersRetryRejectedByAdmission(t *testing.T) {
	t.Parallel()

	b := &stubBroker{
		rejectRetry: true,
	}
	deadLetters := &stubDeadLetter{}
	w := newTestWorker(b, deadLetters, taskforge.HandlerFunc(func(context.Context, taskforge.Task) error {
		return taskforge.Retryable(errors.New("boom"))
	}))
	w.Clock = fixedClock{now: time.Date(2026, 4, 12, 12, 0, 0, 0, time.UTC)}
	w.RetryPolicy = taskforge.DefaultRetryPolicy(3)

	delivery := testDelivery()
	if err := w.processTask(context.Background(), delivery, nil); err != nil {
		t.Fatalf("processTask() error = %v", err)
	}
	if len(deadLetters.envelopes) != 1 {
		t.Fatalf("dead-letter calls = %d, want 1", len(deadLetters.envelopes))
	}
	if got := deadLetters.envelopes[0].FailureClass; got != taskforge.FailureClassOverloaded {
		t.Fatalf("dead-letter failure class = %q, want %q", got, taskforge.FailureClassOverloaded)
	}
	if len(b.acked) != 1 {
		t.Fatalf("Ack calls = %d, want 1", len(b.acked))
	}
	if got := b.acked[0].Execution.State; got != taskforge.StateDeadLettered {
		t.Fatalf("Ack state = %q, want %q", got, taskforge.StateDeadLettered)
	}
}

func TestWorkerProcessTaskPreservesTraceContext(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	rootCtx, rootSpan := provider.Tracer("test").Start(context.Background(), "publish")
	headers := observability.InjectTraceContext(rootCtx, nil)
	rootTraceID := rootSpan.SpanContext().TraceID()
	rootSpanID := rootSpan.SpanContext().SpanID()
	rootSpan.End()

	var got trace.SpanContext
	w := newTestWorker(&stubBroker{}, nil, taskforge.HandlerFunc(func(ctx context.Context, msg taskforge.Task) error {
		got = trace.SpanContextFromContext(ctx)
		return nil
	}))
	delivery := testDelivery()
	delivery.Message.Headers = headers

	if err := w.processTask(context.Background(), delivery, nil); err != nil {
		t.Fatalf("processTask() error = %v", err)
	}
	if !got.IsValid() {
		t.Fatalf("handler span context is invalid")
	}
	if got.TraceID() != rootTraceID {
		t.Fatalf("handler trace id = %s, want %s", got.TraceID(), rootTraceID)
	}
	if got.SpanID() == rootSpanID {
		t.Fatalf("handler span id = %s, want child span distinct from publisher span", got.SpanID())
	}
}

type stubBroker struct {
	acked       []taskforge.Delivery
	nacked      []taskforge.Delivery
	published   []taskforge.Task
	publishOpts []taskforge.PublishOptions
	rejectRetry bool
	publishErr  error
}

func (b *stubBroker) Publish(_ context.Context, msg taskforge.Task, opts taskforge.PublishOptions) (taskforge.PublishResult, error) {
	if b.publishErr != nil && opts.Source == taskforge.PublishSourceRetry {
		return taskforge.PublishResult{}, b.publishErr
	}
	if b.rejectRetry && opts.Source == taskforge.PublishSourceRetry {
		return taskforge.PublishResult{
			Decision: taskforge.AdmissionDecisionRejected,
			Queue:    msg.Queue,
			Reason:   "queue_pending_cap",
		}, &taskforge.AdmissionError{Queue: msg.Queue, Reason: "queue_pending_cap"}
	}
	b.published = append(b.published, msg)
	b.publishOpts = append(b.publishOpts, opts)
	return taskforge.PublishResult{Decision: taskforge.AdmissionDecisionAccepted, Queue: msg.Queue}, nil
}

func (b *stubBroker) Reserve(context.Context, string, string) (taskforge.Delivery, error) {
	return taskforge.Delivery{}, taskforge.ErrNoTask
}

func (b *stubBroker) Ack(_ context.Context, delivery taskforge.Delivery) error {
	b.acked = append(b.acked, delivery)
	return nil
}

func (b *stubBroker) Nack(_ context.Context, delivery taskforge.Delivery, _ bool) error {
	b.nacked = append(b.nacked, delivery)
	return nil
}

func (b *stubBroker) ExtendLease(context.Context, taskforge.Delivery, time.Duration) error {
	return nil
}

type stubDeadLetter struct {
	envelopes []taskforge.DeadLetterEnvelope
}

func (d *stubDeadLetter) PublishDeadLetter(_ context.Context, envelope taskforge.DeadLetterEnvelope) error {
	d.envelopes = append(d.envelopes, envelope)
	return nil
}

type failingDeadLetter struct{ err error }

func (d failingDeadLetter) PublishDeadLetter(context.Context, taskforge.DeadLetterEnvelope) error {
	return d.err
}

type stubStateStore struct {
	queued         []taskforge.Task
	deliveryStates []taskforge.State
}

func (s *stubStateStore) RecordQueued(_ context.Context, msg taskforge.Task) error {
	s.queued = append(s.queued, msg)
	return nil
}

func (s *stubStateStore) RecordDelivery(_ context.Context, _ taskforge.Delivery, state taskforge.State, _ []byte) error {
	s.deliveryStates = append(s.deliveryStates, state)
	return nil
}

func (s *stubStateStore) Get(context.Context, string) (taskforge.TaskRecord, error) {
	return taskforge.TaskRecord{}, taskforge.ErrTaskNotFound
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

func newTestWorker(b taskforge.Broker, deadLetterPublisher taskforge.DeadLetterPublisher, handler taskforge.Handler) *Worker {
	return &Worker{
		Broker:      b,
		DeadLetter:  deadLetterPublisher,
		Handler:     handler,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:     observability.NewMetrics(),
		Clock:       clock.RealClock{},
		RetryPolicy: taskforge.DefaultRetryPolicy(3),
		PoolName:    "default",
		Queue:       "default",
		ConsumerID:  "worker-1",
		LeaseTTL:    0,
		Concurrency: 1,
		Prefetch:    1,
	}
}

func testDelivery() taskforge.Delivery {
	now := time.Date(2026, 4, 12, 11, 0, 0, 0, time.UTC)
	return taskforge.Delivery{
		Message: taskforge.Task{
			ID:        "task-1",
			Name:      "demo.echo",
			Queue:     "default",
			CreatedAt: now.Add(-time.Minute),
		},
		Execution: taskforge.ExecutionMetadata{
			TaskID:          "task-1",
			DeliveryID:      "delivery-1",
			DeliveryCount:   1,
			FirstEnqueuedAt: now.Add(-time.Minute),
			LeasedAt:        now,
			LeaseExpiresAt:  now.Add(30 * time.Second),
			LeaseOwner:      "worker-1",
			State:           taskforge.StateLeased,
		},
	}
}
