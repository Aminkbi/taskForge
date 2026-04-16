package dlq

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
)

type stubBroker struct {
	messages []broker.TaskMessage
	options  []broker.PublishOptions
}

func (s *stubBroker) Publish(_ context.Context, msg broker.TaskMessage, opts broker.PublishOptions) (broker.PublishResult, error) {
	s.messages = append(s.messages, msg)
	s.options = append(s.options, opts)
	return broker.PublishResult{Decision: broker.AdmissionDecisionAccepted, Queue: msg.Queue}, nil
}

func (s *stubBroker) Reserve(context.Context, string, string) (broker.Delivery, error) {
	return broker.Delivery{}, nil
}

func (s *stubBroker) Ack(context.Context, broker.Delivery) error {
	return nil
}

func (s *stubBroker) Nack(context.Context, broker.Delivery, bool) error {
	return nil
}

func (s *stubBroker) ExtendLease(context.Context, broker.Delivery, time.Duration) error {
	return nil
}

func TestPublishDeadLetterUsesDeterministicIDAndDedupeKey(t *testing.T) {
	t.Parallel()

	b := &stubBroker{}
	service := NewService(nil, b, slog.New(slog.NewTextHandler(io.Discard, nil)))
	envelope := Envelope{
		OriginalTask: broker.TaskMessage{
			ID:    "task-1",
			Name:  "demo.echo",
			Queue: "default",
		},
		FailureClass: FailureClassPermanent,
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
