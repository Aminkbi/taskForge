package brokerredis

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/tasks"
)

func TestNewDeliveryDefaults(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	now := createdAt.Add(5 * time.Second)
	delivery := newDelivery(broker.TaskMessage{
		ID:        "task-1",
		Name:      "demo.echo",
		Queue:     "default",
		CreatedAt: createdAt,
	}, "default", "worker-1", now, 30*time.Second)

	if delivery.Execution.TaskID != "task-1" {
		t.Fatalf("TaskID = %q, want %q", delivery.Execution.TaskID, "task-1")
	}
	if delivery.Execution.DeliveryID == "" {
		t.Fatalf("DeliveryID is empty")
	}
	if delivery.Execution.DeliveryCount != 1 {
		t.Fatalf("DeliveryCount = %d, want 1", delivery.Execution.DeliveryCount)
	}
	if !delivery.Execution.FirstEnqueuedAt.Equal(createdAt) {
		t.Fatalf("FirstEnqueuedAt = %v, want %v", delivery.Execution.FirstEnqueuedAt, createdAt)
	}
	if !delivery.Execution.LeasedAt.Equal(now) {
		t.Fatalf("LeasedAt = %v, want %v", delivery.Execution.LeasedAt, now)
	}
	if !delivery.Execution.LeaseExpiresAt.Equal(now.Add(30 * time.Second)) {
		t.Fatalf("LeaseExpiresAt = %v, want %v", delivery.Execution.LeaseExpiresAt, now.Add(30*time.Second))
	}
	if delivery.Execution.LeaseOwner != "worker-1" {
		t.Fatalf("LeaseOwner = %q, want %q", delivery.Execution.LeaseOwner, "worker-1")
	}
	if delivery.Execution.State != string(tasks.StateLeased) {
		t.Fatalf("State = %q, want %q", delivery.Execution.State, tasks.StateLeased)
	}
	if delivery.Execution.LastError != "" {
		t.Fatalf("LastError = %q, want empty", delivery.Execution.LastError)
	}
}

func TestAckRejectsExpiredDelivery(t *testing.T) {
	t.Parallel()

	b := New(nil, slog.Default(), 30*time.Second)
	delivery := broker.Delivery{
		Message: broker.TaskMessage{ID: "task-1"},
		Execution: broker.ExecutionMetadata{
			TaskID:         "task-1",
			DeliveryID:     "delivery-1",
			LeaseOwner:     "worker-1",
			LeaseExpiresAt: time.Now().UTC().Add(-time.Second),
			State:          string(tasks.StateSucceeded),
		},
	}
	b.active[delivery.Execution.DeliveryID] = delivery
	b.seen[delivery.Execution.DeliveryID] = delivery

	err := b.Ack(context.Background(), delivery)
	if !errors.Is(err, broker.ErrDeliveryExpired) {
		t.Fatalf("Ack() error = %v, want %v", err, broker.ErrDeliveryExpired)
	}
}

func TestAckRejectsStaleDelivery(t *testing.T) {
	t.Parallel()

	b := New(nil, slog.Default(), 30*time.Second)
	delivery := broker.Delivery{
		Message: broker.TaskMessage{ID: "task-1"},
		Execution: broker.ExecutionMetadata{
			TaskID:         "task-1",
			DeliveryID:     "delivery-1",
			LeaseOwner:     "worker-1",
			State:          string(tasks.StateSucceeded),
			LeasedAt:       time.Now().UTC(),
			LeaseExpiresAt: time.Now().UTC().Add(time.Second),
		},
	}
	b.seen[delivery.Execution.DeliveryID] = delivery

	err := b.Ack(context.Background(), delivery)
	if !errors.Is(err, broker.ErrStaleDelivery) {
		t.Fatalf("Ack() error = %v, want %v", err, broker.ErrStaleDelivery)
	}
}

func TestExtendLeaseRejectsExpiredDelivery(t *testing.T) {
	t.Parallel()

	b := New(nil, slog.Default(), 30*time.Second)
	delivery := broker.Delivery{
		Message: broker.TaskMessage{ID: "task-1"},
		Execution: broker.ExecutionMetadata{
			TaskID:         "task-1",
			DeliveryID:     "delivery-1",
			LeaseOwner:     "worker-1",
			LeaseExpiresAt: time.Now().UTC().Add(-time.Second),
			State:          string(tasks.StateLeased),
		},
	}
	b.active[delivery.Execution.DeliveryID] = delivery
	b.seen[delivery.Execution.DeliveryID] = delivery

	err := b.ExtendLease(context.Background(), delivery, 10*time.Second)
	if !errors.Is(err, broker.ErrDeliveryExpired) {
		t.Fatalf("ExtendLease() error = %v, want %v", err, broker.ErrDeliveryExpired)
	}
}
