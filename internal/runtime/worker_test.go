package runtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/dlq"
	"github.com/aminkbi/taskforge/internal/observability"
	"github.com/aminkbi/taskforge/internal/tasks"
)

func TestWorkerProcessTaskAcksSucceededDelivery(t *testing.T) {
	t.Parallel()

	b := &stubBroker{}
	w := newTestWorker(b, nil, HandlerFunc(func(context.Context, broker.TaskMessage) error {
		return nil
	}))
	delivery := testDelivery()

	if err := w.processTask(context.Background(), delivery); err != nil {
		t.Fatalf("processTask() error = %v", err)
	}
	if len(b.acked) != 1 {
		t.Fatalf("Ack calls = %d, want 1", len(b.acked))
	}
	if got := b.acked[0].Execution.State; got != string(tasks.StateSucceeded) {
		t.Fatalf("Ack state = %q, want %q", got, tasks.StateSucceeded)
	}
}

func TestWorkerProcessTaskRetriesFailedTask(t *testing.T) {
	t.Parallel()

	b := &stubBroker{}
	w := newTestWorker(b, nil, HandlerFunc(func(context.Context, broker.TaskMessage) error {
		return Retryable(errors.New("boom"))
	}))
	w.Clock = fixedClock{now: time.Date(2026, 4, 12, 12, 0, 0, 0, time.UTC)}
	w.RetryPolicy = tasks.DefaultRetryPolicy(3)

	delivery := testDelivery()
	if err := w.processTask(context.Background(), delivery); err != nil {
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
	if got := b.published[0].Headers["last_error"]; got != "boom" {
		t.Fatalf("Published last_error = %q, want %q", got, "boom")
	}
	if len(b.acked) != 1 {
		t.Fatalf("Ack calls = %d, want 1", len(b.acked))
	}
	if got := b.acked[0].Execution.State; got != string(tasks.StateRetryScheduled) {
		t.Fatalf("Ack state = %q, want %q", got, tasks.StateRetryScheduled)
	}
	if got := b.acked[0].Execution.LastError; got != "boom" {
		t.Fatalf("Ack last error = %q, want %q", got, "boom")
	}
}

func TestWorkerProcessTaskDeadLettersFailedTask(t *testing.T) {
	t.Parallel()

	b := &stubBroker{}
	deadLetters := &stubDeadLetter{}
	w := newTestWorker(b, deadLetters, HandlerFunc(func(context.Context, broker.TaskMessage) error {
		return Permanent(errors.New("boom"))
	}))
	w.Clock = fixedClock{now: time.Date(2026, 4, 12, 12, 0, 0, 0, time.UTC)}
	w.RetryPolicy = tasks.DefaultRetryPolicy(1)

	delivery := testDelivery()
	if err := w.processTask(context.Background(), delivery); err != nil {
		t.Fatalf("processTask() error = %v", err)
	}
	if len(deadLetters.envelopes) != 1 {
		t.Fatalf("dead-letter calls = %d, want 1", len(deadLetters.envelopes))
	}
	if got := deadLetters.envelopes[0].LastError; got != "boom" {
		t.Fatalf("dead-letter last_error = %q, want %q", got, "boom")
	}
	if got := deadLetters.envelopes[0].FailureClass; got != dlq.FailureClassPermanent {
		t.Fatalf("dead-letter failure class = %q, want %q", got, dlq.FailureClassPermanent)
	}
	if len(b.acked) != 1 {
		t.Fatalf("Ack calls = %d, want 1", len(b.acked))
	}
	if got := b.acked[0].Execution.State; got != string(tasks.StateDeadLettered) {
		t.Fatalf("Ack state = %q, want %q", got, tasks.StateDeadLettered)
	}
}

type stubBroker struct {
	acked     []broker.Delivery
	nacked    []broker.Delivery
	published []broker.TaskMessage
}

func (b *stubBroker) Publish(_ context.Context, msg broker.TaskMessage) error {
	b.published = append(b.published, msg)
	return nil
}

func (b *stubBroker) Reserve(context.Context, string, string) (broker.Delivery, error) {
	return broker.Delivery{}, broker.ErrNoTask
}

func (b *stubBroker) Ack(_ context.Context, delivery broker.Delivery) error {
	b.acked = append(b.acked, delivery)
	return nil
}

func (b *stubBroker) Nack(_ context.Context, delivery broker.Delivery, _ bool) error {
	b.nacked = append(b.nacked, delivery)
	return nil
}

func (b *stubBroker) ExtendLease(context.Context, broker.Delivery, time.Duration) error {
	return nil
}

type stubDeadLetter struct {
	envelopes []dlq.Envelope
}

func (d *stubDeadLetter) PublishDeadLetter(_ context.Context, envelope dlq.Envelope) error {
	d.envelopes = append(d.envelopes, envelope)
	return nil
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

func newTestWorker(b broker.Broker, deadLetterPublisher dlq.Publisher, handler Handler) *Worker {
	return &Worker{
		Broker:      b,
		DeadLetter:  deadLetterPublisher,
		Handler:     handler,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:     observability.NewMetrics(),
		Clock:       clock.RealClock{},
		RetryPolicy: tasks.DefaultRetryPolicy(3),
		PoolName:    "default",
		Queue:       "default",
		ConsumerID:  "worker-1",
		LeaseTTL:    0,
		Concurrency: 1,
		Prefetch:    1,
	}
}

func testDelivery() broker.Delivery {
	now := time.Date(2026, 4, 12, 11, 0, 0, 0, time.UTC)
	return broker.Delivery{
		Message: broker.TaskMessage{
			ID:        "task-1",
			Name:      "demo.echo",
			Queue:     "default",
			CreatedAt: now.Add(-time.Minute),
		},
		Execution: broker.ExecutionMetadata{
			TaskID:          "task-1",
			DeliveryID:      "delivery-1",
			DeliveryCount:   1,
			FirstEnqueuedAt: now.Add(-time.Minute),
			LeasedAt:        now,
			LeaseExpiresAt:  now.Add(30 * time.Second),
			LeaseOwner:      "worker-1",
			State:           string(tasks.StateLeased),
		},
	}
}
