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

func TestRedisBrokerPublishReserve(t *testing.T) {
	if os.Getenv("TASKFORGE_RUN_INTEGRATION") != "1" {
		t.Skip("set TASKFORGE_RUN_INTEGRATION=1 to run Redis integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB() error = %v", err)
	}

	b := brokerredis.New(client, slog.Default(), 30*time.Second)
	message := broker.TaskMessage{
		ID:          "integration-task-1",
		Name:        "integration.echo",
		Queue:       "default",
		Payload:     []byte(`{"hello":"world"}`),
		MaxAttempts: 3,
		CreatedAt:   time.Now().UTC(),
	}

	if err := b.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	lease, got, err := b.Reserve(ctx, "default", "integration-worker")
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if got.ID != message.ID {
		t.Fatalf("Reserve() task id = %q, want %q", got.ID, message.ID)
	}
	if err := b.Ack(ctx, lease); err != nil && !errors.Is(err, broker.ErrUnknownLease) {
		t.Fatalf("Ack() error = %v", err)
	}
}
