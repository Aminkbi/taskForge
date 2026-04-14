package integration

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/brokerredis"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/dlq"
	"github.com/aminkbi/taskforge/internal/observability"
	runtimepkg "github.com/aminkbi/taskforge/internal/runtime"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
	"github.com/aminkbi/taskforge/internal/tasks"
)

const (
	ciLeaseTTL          = time.Second
	ciWaitForExpiry     = 1200 * time.Millisecond
	ciRenewBeforeExpiry = 400 * time.Millisecond
	ciPostRenewWindow   = 700 * time.Millisecond
	ciReserveTimeout    = 50 * time.Millisecond
)

func TestRedisBrokerPublishReserveAndAck(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)

	message := broker.TaskMessage{
		ID:          "integration-task-1",
		Name:        "integration.echo",
		Queue:       "default",
		Payload:     []byte(`{"hello":"world"}`),
		MaxAttempts: 3,
		CreatedAt:   time.Now().UTC(),
	}

	if err := brokerInstance.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery, err := brokerInstance.Reserve(ctx, "default", "integration-worker")
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if delivery.Message.ID != message.ID {
		t.Fatalf("Reserve() task id = %q, want %q", delivery.Message.ID, message.ID)
	}
	if delivery.Execution.DeliveryID == "" {
		t.Fatalf("Reserve() delivery id is empty")
	}

	pendingBeforeAck, err := client.XPending(ctx, "taskforge:stream:default", "taskforge:default").Result()
	if err != nil {
		t.Fatalf("XPending() before ack error = %v", err)
	}
	if pendingBeforeAck.Count != 1 {
		t.Fatalf("pending count before ack = %d, want 1", pendingBeforeAck.Count)
	}

	if err := brokerInstance.Ack(ctx, delivery); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	pendingAfterAck, err := client.XPending(ctx, "taskforge:stream:default", "taskforge:default").Result()
	if err != nil {
		t.Fatalf("XPending() after ack error = %v", err)
	}
	if pendingAfterAck.Count != 0 {
		t.Fatalf("pending count after ack = %d, want 0", pendingAfterAck.Count)
	}
}

func TestRedisBrokerConsumersDoNotDuplicateGroupDelivery(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t, 30*time.Second)

	message := broker.TaskMessage{
		ID:        "integration-task-2",
		Name:      "integration.echo",
		Queue:     "default",
		Payload:   []byte(`{"hello":"stream"}`),
		CreatedAt: time.Now().UTC(),
	}

	if err := brokerInstance.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	firstDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() first consumer error = %v", err)
	}

	secondCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()

	_, err = brokerInstance.Reserve(secondCtx, "default", "consumer-b")
	if !errors.Is(err, broker.ErrNoTask) {
		t.Fatalf("Reserve() second consumer error = %v, want %v", err, broker.ErrNoTask)
	}

	if err := brokerInstance.Ack(ctx, firstDelivery); err != nil {
		t.Fatalf("Ack() first delivery error = %v", err)
	}
}

func TestRedisBrokerReclaimsExpiredDelivery(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t, ciLeaseTTL)

	message := broker.TaskMessage{
		ID:        "integration-task-reclaim",
		Name:      "integration.reclaim",
		Queue:     "default",
		Payload:   []byte(`{"hello":"reclaim"}`),
		CreatedAt: time.Now().UTC(),
	}

	if err := brokerInstance.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	firstDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() first consumer error = %v", err)
	}

	time.Sleep(ciWaitForExpiry)

	reclaimedDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-b")
	if err != nil {
		t.Fatalf("Reserve() reclaimed consumer error = %v", err)
	}

	if reclaimedDelivery.Message.ID != firstDelivery.Message.ID {
		t.Fatalf("reclaimed task id = %q, want %q", reclaimedDelivery.Message.ID, firstDelivery.Message.ID)
	}
	if reclaimedDelivery.Execution.LeaseOwner == firstDelivery.Execution.LeaseOwner {
		t.Fatalf("reclaimed lease owner = %q, want different owner", reclaimedDelivery.Execution.LeaseOwner)
	}
	if reclaimedDelivery.Execution.DeliveryCount < 2 {
		t.Fatalf("reclaimed delivery count = %d, want >= 2", reclaimedDelivery.Execution.DeliveryCount)
	}
}

func TestRedisBrokerRejectsStaleAckAfterReclaim(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t, ciLeaseTTL)

	message := broker.TaskMessage{
		ID:        "integration-task-stale-ack",
		Name:      "integration.stale_ack",
		Queue:     "default",
		Payload:   []byte(`{"hello":"stale"}`),
		CreatedAt: time.Now().UTC(),
	}

	if err := brokerInstance.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	firstDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() first consumer error = %v", err)
	}

	time.Sleep(ciWaitForExpiry)

	reclaimedDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-b")
	if err != nil {
		t.Fatalf("Reserve() reclaimed consumer error = %v", err)
	}

	if err := brokerInstance.Ack(ctx, firstDelivery); !errors.Is(err, broker.ErrStaleDelivery) {
		t.Fatalf("Ack() stale delivery error = %v, want %v", err, broker.ErrStaleDelivery)
	}

	if err := brokerInstance.Ack(ctx, reclaimedDelivery); err != nil {
		t.Fatalf("Ack() reclaimed delivery error = %v", err)
	}
}

func TestRedisBrokerRejectsStaleNackAfterReclaim(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t, ciLeaseTTL)

	message := broker.TaskMessage{
		ID:        "integration-task-stale-nack",
		Name:      "integration.stale_nack",
		Queue:     "default",
		Payload:   []byte(`{"hello":"stale-nack"}`),
		CreatedAt: time.Now().UTC(),
	}

	if err := brokerInstance.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	firstDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() first consumer error = %v", err)
	}

	time.Sleep(ciWaitForExpiry)

	reclaimedDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-b")
	if err != nil {
		t.Fatalf("Reserve() reclaimed consumer error = %v", err)
	}

	if err := brokerInstance.Nack(ctx, firstDelivery, false); !errors.Is(err, broker.ErrStaleDelivery) {
		t.Fatalf("Nack() stale delivery error = %v, want %v", err, broker.ErrStaleDelivery)
	}

	if err := brokerInstance.Ack(ctx, reclaimedDelivery); err != nil {
		t.Fatalf("Ack() reclaimed delivery error = %v", err)
	}
}

func TestRedisBrokerExpiresCurrentOwnerAck(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t, ciLeaseTTL)

	message := broker.TaskMessage{
		ID:        "integration-task-expired-ack",
		Name:      "integration.expired_ack",
		Queue:     "default",
		Payload:   []byte(`{"hello":"expired"}`),
		CreatedAt: time.Now().UTC(),
	}

	if err := brokerInstance.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery, err := brokerInstance.Reserve(ctx, "default", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}

	time.Sleep(ciWaitForExpiry)

	if err := brokerInstance.Ack(ctx, delivery); !errors.Is(err, broker.ErrDeliveryExpired) {
		t.Fatalf("Ack() expired delivery error = %v, want %v", err, broker.ErrDeliveryExpired)
	}
}

func TestRedisBrokerExtendLeasePreventsReclaim(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, ciLeaseTTL)

	message := broker.TaskMessage{
		ID:        "integration-task-extend",
		Name:      "integration.extend",
		Queue:     "default",
		Payload:   []byte(`{"hello":"extend"}`),
		CreatedAt: time.Now().UTC(),
	}

	if err := brokerInstance.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery, err := brokerInstance.Reserve(ctx, "default", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}

	time.Sleep(ciRenewBeforeExpiry)
	if err := brokerInstance.ExtendLease(ctx, delivery, ciLeaseTTL); err != nil {
		t.Fatalf("ExtendLease() error = %v", err)
	}

	time.Sleep(ciPostRenewWindow)

	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: "taskforge:stream:default",
		Group:  "taskforge:default",
		Start:  delivery.Execution.DeliveryID,
		End:    delivery.Execution.DeliveryID,
		Count:  1,
	}).Result()
	if err != nil {
		t.Fatalf("XPendingExt() error = %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending entry count = %d, want 1", len(pending))
	}
	if pending[0].Consumer != delivery.Execution.LeaseOwner {
		t.Fatalf("pending owner = %q, want %q", pending[0].Consumer, delivery.Execution.LeaseOwner)
	}
	if pending[0].Idle >= ciLeaseTTL {
		t.Fatalf("pending idle = %v, want less than %v", pending[0].Idle, ciLeaseTTL)
	}

	if err := brokerInstance.Ack(ctx, delivery); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
}

func TestRedisBrokerMoveDueReleasesIntoStreamQueueInETAOrder(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t, 30*time.Second)

	base := time.Now().UTC()
	messages := []broker.TaskMessage{
		{
			ID:        "integration-task-delayed-3",
			Name:      "integration.delayed",
			Queue:     "default",
			Payload:   []byte(`{"hello":"third"}`),
			CreatedAt: base,
		},
		{
			ID:        "integration-task-delayed-1",
			Name:      "integration.delayed",
			Queue:     "default",
			Payload:   []byte(`{"hello":"first"}`),
			CreatedAt: base,
		},
		{
			ID:        "integration-task-delayed-2",
			Name:      "integration.delayed",
			Queue:     "default",
			Payload:   []byte(`{"hello":"second"}`),
			CreatedAt: base,
		},
	}
	eta3 := base.Add(30 * time.Millisecond)
	eta1 := base.Add(10 * time.Millisecond)
	eta2 := base.Add(20 * time.Millisecond)
	messages[0].ETA = &eta3
	messages[1].ETA = &eta1
	messages[2].ETA = &eta2

	for _, message := range messages {
		if err := brokerInstance.Publish(ctx, message); err != nil {
			t.Fatalf("Publish() delayed error = %v", err)
		}
	}

	releasedAt := base.Add(time.Second)
	moved, err := brokerInstance.MoveDue(ctx, releasedAt, 10)
	if err != nil {
		t.Fatalf("MoveDue() error = %v", err)
	}
	if moved != 3 {
		t.Fatalf("MoveDue() moved = %d, want 3", moved)
	}

	expectedOrder := []struct {
		id  string
		eta time.Time
	}{
		{id: "integration-task-delayed-1", eta: eta1},
		{id: "integration-task-delayed-2", eta: eta2},
		{id: "integration-task-delayed-3", eta: eta3},
	}

	for _, expected := range expectedOrder {
		delivery, err := brokerInstance.Reserve(ctx, "default", "delayed-consumer")
		if err != nil {
			t.Fatalf("Reserve() moved task error = %v", err)
		}
		if delivery.Message.ID != expected.id {
			t.Fatalf("Reserve() moved task id = %q, want %q", delivery.Message.ID, expected.id)
		}
		if delivery.Message.Headers[schedulerpkg.HeaderScheduledFor] != expected.eta.Format(time.RFC3339Nano) {
			t.Fatalf("scheduled_for = %q, want %q", delivery.Message.Headers[schedulerpkg.HeaderScheduledFor], expected.eta.Format(time.RFC3339Nano))
		}
		if delivery.Message.Headers[schedulerpkg.HeaderReleasedAt] != releasedAt.Format(time.RFC3339Nano) {
			t.Fatalf("released_at = %q, want %q", delivery.Message.Headers[schedulerpkg.HeaderReleasedAt], releasedAt.Format(time.RFC3339Nano))
		}
		lag, err := strconv.ParseInt(delivery.Message.Headers[schedulerpkg.HeaderReleaseLagMS], 10, 64)
		if err != nil {
			t.Fatalf("parse release lag = %v", err)
		}
		if lag < 0 {
			t.Fatalf("release lag = %d, want >= 0", lag)
		}
		if err := brokerInstance.Ack(ctx, delivery); err != nil {
			t.Fatalf("Ack() error = %v", err)
		}
	}
}

func TestWorkerRetryableErrorSchedulesAnotherAttempt(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)
	deadLetters := dlq.NewService(client, brokerInstance, slog.Default())
	worker := newIntegrationWorker(brokerInstance, deadLetters, runtimepkg.HandlerFunc(func(context.Context, broker.TaskMessage) error {
		return runtimepkg.Retryable(errors.New("boom"))
	}), tasks.RetryPolicy{
		MaxDeliveries:  3,
		InitialBackoff: 2 * time.Second,
		MaxBackoff:     2 * time.Second,
		Multiplier:     1,
	})

	message := broker.TaskMessage{
		ID:        "integration-retry-worker",
		Name:      "integration.retryable",
		Queue:     "default",
		Payload:   []byte(`{"hello":"retry"}`),
		CreatedAt: time.Now().UTC(),
	}
	if err := brokerInstance.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	runWorkerUntil(t, worker, func() (bool, error) {
		values, err := client.ZRange(ctx, "taskforge:delayed", 0, -1).Result()
		if err != nil {
			return false, err
		}
		return len(values) == 1, nil
	})

	values, err := client.ZRange(ctx, "taskforge:delayed", 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRange() error = %v", err)
	}
	var retried struct {
		Message broker.TaskMessage `json:"message"`
	}
	if err := json.Unmarshal([]byte(values[0]), &retried); err != nil {
		t.Fatalf("unmarshal retried delayed entry: %v", err)
	}
	if retried.Message.Attempt != 1 {
		t.Fatalf("retried attempt = %d, want 1", retried.Message.Attempt)
	}
	if retried.Message.Headers[tasks.HeaderRetryFailureClass] != string(dlq.FailureClassTransientRetryable) {
		t.Fatalf("retry failure class = %q, want %q", retried.Message.Headers[tasks.HeaderRetryFailureClass], dlq.FailureClassTransientRetryable)
	}
}

func TestSchedulerLeaderElectionDispatchesRecurringOnce(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)

	startAt := time.Now().UTC().Add(-time.Second)
	schedules := []schedulerpkg.ScheduleDefinition{{
		ID:            "integration-recurring-once",
		Interval:      500 * time.Millisecond,
		Queue:         "default",
		TaskName:      "integration.recurring",
		Payload:       json.RawMessage(`{"hello":"recurring"}`),
		Enabled:       true,
		MisfirePolicy: schedulerpkg.MisfirePolicyCoalesce,
		StartAt:       &startAt,
	}}

	schedulerA := newIntegrationScheduler(t, client, "scheduler-a", schedules)
	schedulerB := newIntegrationScheduler(t, client, "scheduler-b", schedules)

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	errChA := runScheduler(ctxA, schedulerA)
	errChB := runScheduler(ctxB, schedulerB)

	waitForStreamLength(t, client, "taskforge:stream:default", 1)
	cancelA()
	cancelB()
	waitForSchedulerStop(t, errChA)
	waitForSchedulerStop(t, errChB)

	messages := loadStreamTaskMessages(t, ctx, client, "taskforge:stream:default")
	if len(messages) != 1 {
		t.Fatalf("stream messages = %d, want 1", len(messages))
	}
	if messages[0].Headers[schedulerpkg.HeaderScheduleID] != "integration-recurring-once" {
		t.Fatalf("schedule_id header = %q, want %q", messages[0].Headers[schedulerpkg.HeaderScheduleID], "integration-recurring-once")
	}

	delivery, err := brokerInstance.Reserve(ctx, "default", "recurring-consumer")
	if err != nil {
		t.Fatalf("Reserve() recurring task error = %v", err)
	}
	if delivery.Message.ID == "" {
		t.Fatal("recurring task id is empty")
	}
}

func TestSchedulerFailoverCoalescesMissedRecurringRuns(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	startAt := time.Now().UTC().Add(-time.Second)
	schedules := []schedulerpkg.ScheduleDefinition{{
		ID:            "integration-recurring-failover",
		Interval:      50 * time.Millisecond,
		Queue:         "default",
		TaskName:      "integration.recurring",
		Payload:       json.RawMessage(`{"hello":"failover"}`),
		Enabled:       true,
		MisfirePolicy: schedulerpkg.MisfirePolicyCoalesce,
		StartAt:       &startAt,
	}}

	schedulerA := newIntegrationScheduler(t, client, "scheduler-a", schedules)
	schedulerB := newIntegrationScheduler(t, client, "scheduler-b", schedules)

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	errChA := runScheduler(ctxA, schedulerA)
	errChB := runScheduler(ctxB, schedulerB)

	waitForStreamLength(t, client, "taskforge:stream:default", 1)
	cancelA()
	waitForSchedulerStop(t, errChA)

	waitForStreamLength(t, client, "taskforge:stream:default", 2)
	cancelB()
	waitForSchedulerStop(t, errChB)

	messages := loadStreamTaskMessages(t, ctx, client, "taskforge:stream:default")
	if len(messages) != 2 {
		t.Fatalf("stream messages = %d, want 2", len(messages))
	}
	if messages[0].ID == messages[1].ID {
		t.Fatalf("recurring task ids are identical: %q", messages[0].ID)
	}
	if messages[1].Headers[schedulerpkg.HeaderScheduleID] != "integration-recurring-failover" {
		t.Fatalf("schedule_id header = %q, want %q", messages[1].Headers[schedulerpkg.HeaderScheduleID], "integration-recurring-failover")
	}
	missedRuns, err := strconv.Atoi(messages[1].Headers[schedulerpkg.HeaderScheduleMissedRuns])
	if err != nil {
		t.Fatalf("parse missed runs = %v", err)
	}
	if missedRuns < 1 {
		t.Fatalf("missed runs = %d, want >= 1", missedRuns)
	}
}

func TestWorkerPermanentErrorGoesDirectlyToDeadLetter(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)
	deadLetters := dlq.NewService(client, brokerInstance, slog.Default())
	worker := newIntegrationWorker(brokerInstance, deadLetters, runtimepkg.HandlerFunc(func(context.Context, broker.TaskMessage) error {
		return runtimepkg.Permanent(errors.New("bad payload"))
	}), tasks.DefaultRetryPolicy(3))

	message := broker.TaskMessage{
		ID:        "integration-permanent-worker",
		Name:      "integration.permanent",
		Queue:     "default",
		Payload:   []byte(`{"hello":"permanent"}`),
		CreatedAt: time.Now().UTC(),
	}
	if err := brokerInstance.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	runWorkerUntil(t, worker, func() (bool, error) {
		entries, err := deadLetters.List(ctx, "default", 10)
		if err != nil {
			return false, err
		}
		return len(entries) == 1, nil
	})

	entries, err := deadLetters.List(ctx, "default", 10)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if entries[0].Envelope.FailureClass != dlq.FailureClassPermanent {
		t.Fatalf("failure class = %q, want %q", entries[0].Envelope.FailureClass, dlq.FailureClassPermanent)
	}
}

func TestWorkerMaxDeliveryExhaustionMovesTaskToDeadLetter(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)
	deadLetters := dlq.NewService(client, brokerInstance, slog.Default())
	worker := newIntegrationWorker(brokerInstance, deadLetters, runtimepkg.HandlerFunc(func(context.Context, broker.TaskMessage) error {
		return runtimepkg.Retryable(errors.New("retry exhausted"))
	}), tasks.DefaultRetryPolicy(3))

	message := broker.TaskMessage{
		ID:          "integration-retry-exhausted",
		Name:        "integration.exhausted",
		Queue:       "default",
		Payload:     []byte(`{"hello":"exhausted"}`),
		MaxAttempts: 1,
		CreatedAt:   time.Now().UTC(),
	}
	if err := brokerInstance.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	runWorkerUntil(t, worker, func() (bool, error) {
		entries, err := deadLetters.List(ctx, "default", 10)
		if err != nil {
			return false, err
		}
		return len(entries) == 1, nil
	})

	entries, err := deadLetters.List(ctx, "default", 10)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if entries[0].Envelope.OriginalTask.ID != message.ID {
		t.Fatalf("dead-letter task id = %q, want %q", entries[0].Envelope.OriginalTask.ID, message.ID)
	}
}

func TestDeadLetterServiceReplayOneEntry(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)
	deadLetters := dlq.NewService(client, brokerInstance, slog.Default())

	original := broker.TaskMessage{
		ID:        "integration-replay",
		Name:      "integration.replay",
		Queue:     "default",
		Payload:   []byte(`{"hello":"replay"}`),
		CreatedAt: time.Now().UTC(),
	}
	envelope := dlq.Envelope{
		OriginalTask:     original,
		FailureClass:     dlq.FailureClassPermanent,
		LastError:        "failed permanently",
		DeliveryCount:    1,
		FirstEnqueuedAt:  original.CreatedAt,
		LastFailureAt:    time.Now().UTC(),
		WorkerIdentity:   "worker-1",
		DeliveryID:       "delivery-1",
		OriginalQueue:    "default",
		OriginalTaskName: original.Name,
	}

	if err := deadLetters.PublishDeadLetter(ctx, envelope); err != nil {
		t.Fatalf("PublishDeadLetter() error = %v", err)
	}

	entries, err := deadLetters.List(ctx, "default", 10)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("dead-letter entries = %d, want 1", len(entries))
	}

	if err := deadLetters.Replay(ctx, "default", entries[0].ID); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	delivery, err := brokerInstance.Reserve(ctx, "default", "replay-consumer")
	if err != nil {
		t.Fatalf("Reserve() replayed task error = %v", err)
	}
	if delivery.Message.ID != original.ID {
		t.Fatalf("replayed task id = %q, want %q", delivery.Message.ID, original.ID)
	}
}

func newIntegrationBroker(t *testing.T, leaseTTL time.Duration) (context.Context, *brokerredis.RedisBroker, *redis.Client) {
	t.Helper()

	if os.Getenv("TASKFORGE_RUN_INTEGRATION") != "1" {
		t.Skip("set TASKFORGE_RUN_INTEGRATION=1 to run Redis integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	t.Cleanup(func() {
		_ = client.Close()
	})

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB() error = %v", err)
	}

	return ctx, brokerredis.NewWithOptions(client, slog.Default(), leaseTTL, nil, brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	}), client
}

func newIntegrationWorker(b broker.Broker, deadLetters dlq.Publisher, handler runtimepkg.Handler, policy tasks.RetryPolicy) *runtimepkg.Worker {
	return &runtimepkg.Worker{
		Broker:       b,
		DeadLetter:   deadLetters,
		Handler:      handler,
		Logger:       slog.Default(),
		Metrics:      observability.NewMetrics(),
		Clock:        clock.RealClock{},
		RetryPolicy:  policy,
		Queue:        "default",
		ConsumerID:   "integration-worker",
		PollInterval: 10 * time.Millisecond,
		LeaseTTL:     30 * time.Second,
		Concurrency:  1,
	}
}

func newIntegrationScheduler(t *testing.T, client *redis.Client, owner string, schedules []schedulerpkg.ScheduleDefinition) *schedulerpkg.Scheduler {
	t.Helper()

	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, nil, brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	})
	elector := schedulerpkg.NewRedisLeaderElector(
		client,
		clock.RealClock{},
		slog.Default(),
		owner,
		100*time.Millisecond,
		25*time.Millisecond,
	)
	recurring := schedulerpkg.NewRecurringService(
		brokerInstance,
		schedulerpkg.NewRedisScheduleStateStore(client),
		schedules,
		slog.Default(),
	)
	return schedulerpkg.New(
		brokerInstance,
		recurring,
		elector,
		clock.RealClock{},
		slog.Default(),
		25*time.Millisecond,
		25*time.Millisecond,
	)
}

func runScheduler(ctx context.Context, scheduler *schedulerpkg.Scheduler) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- scheduler.Run(ctx)
	}()
	return errCh
}

func waitForSchedulerStop(t *testing.T, errCh <-chan error) {
	t.Helper()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("scheduler.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop before timeout")
	}
}

func waitForStreamLength(t *testing.T, client *redis.Client, streamKey string, expected int64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		count, err := client.XLen(context.Background(), streamKey).Result()
		if err != nil {
			t.Fatalf("XLen() error = %v", err)
		}
		if count >= expected {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("stream %s did not reach length %d before timeout", streamKey, expected)
}

func loadStreamTaskMessages(t *testing.T, ctx context.Context, client *redis.Client, streamKey string) []broker.TaskMessage {
	t.Helper()

	entries, err := client.XRange(ctx, streamKey, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange() error = %v", err)
	}

	messages := make([]broker.TaskMessage, 0, len(entries))
	for _, entry := range entries {
		raw, ok := entry.Values["message"]
		if !ok {
			t.Fatalf("stream entry missing message field: %+v", entry.Values)
		}
		payload, ok := raw.(string)
		if !ok {
			t.Fatalf("stream payload type = %T, want string", raw)
		}
		var msg broker.TaskMessage
		if err := json.Unmarshal([]byte(payload), &msg); err != nil {
			t.Fatalf("unmarshal stream task: %v", err)
		}
		messages = append(messages, msg)
	}
	return messages
}

func runWorkerUntil(t *testing.T, worker *runtimepkg.Worker, condition func() (bool, error)) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok, err := condition()
		if err != nil {
			t.Fatalf("condition error = %v", err)
		}
		if ok {
			cancel()
			select {
			case err := <-errCh:
				if err != nil {
					t.Fatalf("worker.Run() error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatalf("worker did not stop after cancel")
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("worker.Run() error = %v", err)
		}
	default:
	}
	t.Fatalf("condition was not met before timeout")
}
