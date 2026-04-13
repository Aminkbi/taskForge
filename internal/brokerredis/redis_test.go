package brokerredis

import (
	"testing"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/tasks"
)

func TestStreamNaming(t *testing.T) {
	t.Parallel()

	b := &RedisBroker{
		prefix:     defaultPrefix,
		hostname:   "host-1",
		instanceID: "42",
	}

	if got := b.streamKey("critical"); got != "taskforge:stream:critical" {
		t.Fatalf("streamKey() = %q, want %q", got, "taskforge:stream:critical")
	}
	if got := b.groupName("critical"); got != "taskforge:critical" {
		t.Fatalf("groupName() = %q, want %q", got, "taskforge:critical")
	}
	if got := b.consumerName("worker"); got != "worker:host-1:42" {
		t.Fatalf("consumerName() = %q, want %q", got, "worker:host-1:42")
	}
}

func TestNewDeliveryDefaults(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	now := createdAt.Add(5 * time.Second)
	delivery := newDelivery(broker.TaskMessage{
		ID:        "task-1",
		Name:      "demo.echo",
		Queue:     "default",
		CreatedAt: createdAt,
	}, "default", "worker-1:host-1:42", "1744538400000-0", now, 30*time.Second)

	if delivery.Execution.TaskID != "task-1" {
		t.Fatalf("TaskID = %q, want %q", delivery.Execution.TaskID, "task-1")
	}
	if delivery.Execution.DeliveryID != "1744538400000-0" {
		t.Fatalf("DeliveryID = %q, want %q", delivery.Execution.DeliveryID, "1744538400000-0")
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
	if delivery.Execution.LeaseOwner != "worker-1:host-1:42" {
		t.Fatalf("LeaseOwner = %q, want %q", delivery.Execution.LeaseOwner, "worker-1:host-1:42")
	}
	if delivery.Execution.State != string(tasks.StateLeased) {
		t.Fatalf("State = %q, want %q", delivery.Execution.State, tasks.StateLeased)
	}
}

func TestNormalizeQueue(t *testing.T) {
	t.Parallel()

	if got := normalizeQueue(""); got != "default" {
		t.Fatalf("normalizeQueue(\"\") = %q, want %q", got, "default")
	}
	if got := normalizeQueue("priority"); got != "priority" {
		t.Fatalf("normalizeQueue() = %q, want %q", got, "priority")
	}
}
