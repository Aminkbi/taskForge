package integration

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/brokerredis"
)

func TestRedisBrokerPublishReserveAndAck(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t)

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
	ctx, brokerInstance, _ := newIntegrationBroker(t)

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

func TestRedisBrokerMoveDueReleasesIntoStreamQueue(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t)

	eta := time.Now().UTC().Add(50 * time.Millisecond)
	message := broker.TaskMessage{
		ID:        "integration-task-3",
		Name:      "integration.delayed",
		Queue:     "default",
		Payload:   []byte(`{"hello":"delayed"}`),
		ETA:       &eta,
		CreatedAt: time.Now().UTC(),
	}

	if err := brokerInstance.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() delayed error = %v", err)
	}

	moved, err := brokerInstance.MoveDue(ctx, eta.Add(time.Second), 10)
	if err != nil {
		t.Fatalf("MoveDue() error = %v", err)
	}
	if moved != 1 {
		t.Fatalf("MoveDue() moved = %d, want 1", moved)
	}

	delivery, err := brokerInstance.Reserve(ctx, "default", "delayed-consumer")
	if err != nil {
		t.Fatalf("Reserve() moved task error = %v", err)
	}
	if delivery.Message.ID != message.ID {
		t.Fatalf("Reserve() moved task id = %q, want %q", delivery.Message.ID, message.ID)
	}
}

func newIntegrationBroker(t *testing.T) (context.Context, *brokerredis.RedisBroker, *redis.Client) {
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

	return ctx, brokerredis.New(client, slog.Default(), 30*time.Second), client
}
