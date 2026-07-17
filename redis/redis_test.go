package redis

import (
	"context"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
)

type customStateStore struct{}

func (customStateStore) RecordQueued(context.Context, taskforge.Task) error { return nil }
func (customStateStore) RecordDelivery(context.Context, taskforge.Delivery, taskforge.State, []byte) error {
	return nil
}
func (customStateStore) Get(context.Context, string) (taskforge.TaskRecord, error) {
	return taskforge.TaskRecord{}, nil
}

func TestOwnsStateStoreOnlyForBuiltInStore(t *testing.T) {
	t.Parallel()

	builtIn := &Broker{stateStore: &stateStore{}}
	if !builtIn.OwnsStateStore(builtIn) {
		t.Fatal("built-in broker state store was not recognized")
	}
	custom := &Broker{stateStore: customStateStore{}}
	if custom.OwnsStateStore(custom) {
		t.Fatal("custom state store was incorrectly treated as atomically co-located")
	}
}

func TestStreamNaming(t *testing.T) {
	t.Parallel()

	b := &Broker{
		prefix:     defaultPrefix,
		hostname:   "host-1",
		instanceID: "42",
	}

	if got := b.streamKey("critical"); got != "taskforge:v2:stream:critical" {
		t.Fatalf("streamKey() = %q, want %q", got, "taskforge:v2:stream:critical")
	}
	if got := b.groupName("critical"); got != "taskforge:v2:critical" {
		t.Fatalf("groupName() = %q, want %q", got, "taskforge:v2:critical")
	}
	if got := b.consumerName("worker"); got != "worker:host-1:42" {
		t.Fatalf("consumerName() = %q, want %q", got, "worker:host-1:42")
	}
}

func TestNewDefaultsReserveTimeout(t *testing.T) {
	t.Parallel()

	b := New(Options{LeaseTTL: 30 * time.Second})
	if b.reserveTTL != defaultReserveTimeout {
		t.Fatalf("reserveTTL = %v, want %v", b.reserveTTL, defaultReserveTimeout)
	}
}

func TestNewUsesConfiguredReserveTimeout(t *testing.T) {
	t.Parallel()

	b := New(Options{LeaseTTL: 30 * time.Second, ReserveTimeout: 75 * time.Millisecond})
	if b.reserveTTL != 75*time.Millisecond {
		t.Fatalf("reserveTTL = %v, want %v", b.reserveTTL, 75*time.Millisecond)
	}
}

func TestNewDeliveryDefaults(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	now := createdAt.Add(5 * time.Second)
	delivery := newDelivery(taskforge.Task{
		ID:        "task-1",
		Name:      "demo.echo",
		Queue:     "default",
		CreatedAt: createdAt,
	}, "default", "worker-1:host-1:42", "1744538400000-0", now, 30*time.Second, 1)

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
	if delivery.Execution.State != taskforge.StateLeased {
		t.Fatalf("State = %q, want %q", delivery.Execution.State, taskforge.StateLeased)
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

func TestQueueStreamKeyUsesFairnessPartitionWhenConfigured(t *testing.T) {
	t.Parallel()

	policy, err := NewFairnessPolicy(FairnessRule{}, nil)
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}

	b := &Broker{
		prefix:           defaultPrefix,
		fairnessPolicies: map[string]*FairnessPolicy{"critical": policy},
	}

	if got := b.queueStreamKey("critical", "tenant-a"); got == b.streamKey("critical") {
		t.Fatalf("queueStreamKey() = %q, want fairness stream distinct from queue stream", got)
	}
	if got := b.queueStreamKey("default", "tenant-a"); got != b.streamKey("default") {
		t.Fatalf("queueStreamKey() = %q, want %q", got, b.streamKey("default"))
	}
}

func TestDeliveryCountUsesClaimFallback(t *testing.T) {
	t.Parallel()

	msg := taskforge.Task{Attempt: 0}
	if got := deliveryCount(msg, 2); got != 2 {
		t.Fatalf("deliveryCount() = %d, want 2", got)
	}

	msg.Attempt = 3
	if got := deliveryCount(msg, 2); got != 4 {
		t.Fatalf("deliveryCount() = %d, want 4", got)
	}
}

func TestRoutePublishedMessageAppliesOnlyToNewPublishes(t *testing.T) {
	t.Parallel()

	policy, err := ParseRoutingPolicyJSON([]byte(`{
		"rules":[
			{
				"name":"critical",
				"match":{"task_names":["demo.critical"]},
				"destination":{"queue":"critical","shard":"shard-a"}
			}
		]
	}`))
	if err != nil {
		t.Fatalf("ParseJSON() error = %v", err)
	}
	b := &Broker{routingPolicy: policy}
	msg := taskforge.Task{ID: "task-1", Name: "demo.critical", Queue: "ingress"}

	routed, placement := b.routePublishedMessage(msg, taskforge.PublishOptions{Source: taskforge.PublishSourceNew})
	if routed.Queue != "critical" || placement.Shard != "shard-a" || placement.Rule != "critical" {
		t.Fatalf("unexpected routed placement: msg=%+v placement=%+v", routed, placement)
	}

	preserved, placement := b.routePublishedMessage(routed, taskforge.PublishOptions{Source: taskforge.PublishSourceRetry})
	if preserved.Queue != "critical" || placement.Rule != "" || placement.Shard != "" {
		t.Fatalf("retry publish should preserve assigned placement: msg=%+v placement=%+v", preserved, placement)
	}
}
