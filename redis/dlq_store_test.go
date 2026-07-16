package redis

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
)

type stubBroker struct {
	messages []taskforge.Task
	options  []taskforge.PublishOptions
}

func (s *stubBroker) Publish(_ context.Context, msg taskforge.Task, opts taskforge.PublishOptions) (taskforge.PublishResult, error) {
	s.messages = append(s.messages, msg)
	s.options = append(s.options, opts)
	return taskforge.PublishResult{Decision: taskforge.AdmissionDecisionAccepted, Queue: msg.Queue}, nil
}

func (s *stubBroker) Reserve(context.Context, string, string) (taskforge.Delivery, error) {
	return taskforge.Delivery{}, nil
}

func (s *stubBroker) Ack(context.Context, taskforge.Delivery) error {
	return nil
}

func (s *stubBroker) Nack(context.Context, taskforge.Delivery, bool) error {
	return nil
}

func (s *stubBroker) ExtendLease(context.Context, taskforge.Delivery, time.Duration) error {
	return nil
}

func TestPublishDeadLetterUsesDeterministicIDAndDedupeKey(t *testing.T) {
	t.Parallel()

	b := &stubBroker{}
	service := newDeadLetterStore(nil, b, slog.New(slog.NewTextHandler(io.Discard, nil)))
	envelope := taskforge.DeadLetterEnvelope{
		OriginalTask: taskforge.Task{
			ID:    "task-1",
			Name:  "demo.echo",
			Queue: "default",
		},
		FailureClass: taskforge.FailureClassPermanent,
		LastError:    "boom",
		DeliveryID:   "delivery-1",
	}

	if err := service.PublishDeadLetter(context.Background(), envelope); err != nil {
		t.Fatalf("PublishDeadLetter() error = %v", err)
	}
	if len(b.messages) != 1 {
		t.Fatalf("published messages = %d, want 1", len(b.messages))
	}
	if got := b.messages[0].ID; got != "dlq:delivery-1" {
		t.Fatalf("dead-letter task id = %q, want %q", got, "dlq:delivery-1")
	}
	if got := b.options[0].DeduplicationKey; got != "dead_letter:delivery-1" {
		t.Fatalf("dead-letter deduplication key = %q, want %q", got, "dead_letter:delivery-1")
	}
}
