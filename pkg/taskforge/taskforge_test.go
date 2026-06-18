package taskforge

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewTaskAppliesOptionsAndCopiesInputs(t *testing.T) {
	t.Parallel()

	headers := map[string]string{"source": "test"}
	payload := []byte(`{"ok":true}`)
	eta := time.Date(2026, 6, 18, 12, 0, 0, 0, time.FixedZone("test", 90*60))
	task := NewTask(
		"email.send",
		payload,
		WithID("task-1"),
		WithQueue("critical"),
		WithFairnessKey("tenant-a"),
		WithHeaders(headers),
		WithHeader("trace", "abc"),
		WithETA(eta),
		WithMaxAttempts(3),
		WithIdempotencyKey("email-1"),
	)

	headers["source"] = "mutated"
	payload[0] = '['

	if task.ID != "task-1" || task.Name != "email.send" || task.Queue != "critical" {
		t.Fatalf("unexpected task identity: %+v", task)
	}
	if task.Headers["source"] != "test" || task.Headers["trace"] != "abc" {
		t.Fatalf("headers were not copied/applied: %+v", task.Headers)
	}
	if string(task.Payload) != `{"ok":true}` {
		t.Fatalf("payload was not copied: %s", string(task.Payload))
	}
	if task.ETA == nil || !task.ETA.Equal(eta.UTC()) {
		t.Fatalf("ETA was not normalized to UTC: %v", task.ETA)
	}
	if task.MaxAttempts != 3 || task.IdempotencyKey != "email-1" || task.FairnessKey != "tenant-a" {
		t.Fatalf("options not applied: %+v", task)
	}
}

func TestRegistryDispatchesRegisteredHandler(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	var handled Task
	if err := registry.RegisterFunc("demo.echo", func(_ context.Context, task Task) error {
		handled = task
		return nil
	}); err != nil {
		t.Fatalf("RegisterFunc() error = %v", err)
	}

	task := NewTask("demo.echo", []byte("hello"), WithID("task-1"))
	if err := registry.HandleTask(context.Background(), task); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}
	if handled.ID != "task-1" {
		t.Fatalf("handler saw task %+v", handled)
	}
}

func TestRegistryReturnsValidationForUnknownTask(t *testing.T) {
	t.Parallel()

	err := NewRegistry().HandleTask(context.Background(), NewTask("missing", nil))
	if !errors.Is(err, ErrUnknownTask) {
		t.Fatalf("HandleTask() error = %v, want ErrUnknownTask", err)
	}
}
