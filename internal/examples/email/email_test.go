package email

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aminkbi/taskforge/internal/broker"
)

func TestHandlerSkipsDuplicateEmailSend(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(SendEmailPayload{
		Recipient: "ops@example.com",
		Subject:   "hello",
		Body:      "world",
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	store := NewIdempotencyStore()
	mailer := &CaptureMailer{}
	handler := Handler{Mailer: mailer, Store: store}
	message := broker.TaskMessage{
		ID:             "task-1",
		IdempotencyKey: "email:ops@example.com",
		Name:           TaskSendEmail,
		Payload:        payload,
	}

	if err := handler.HandleTask(context.Background(), message); err != nil {
		t.Fatalf("first HandleTask() error = %v", err)
	}
	if err := handler.HandleTask(context.Background(), message); err != nil {
		t.Fatalf("second HandleTask() error = %v", err)
	}
	if got := mailer.Count(); got != 1 {
		t.Fatalf("mailer.Count() = %d, want 1", got)
	}
	if got := store.Status("email:ops@example.com"); got != "sent" {
		t.Fatalf("store.Status() = %q, want sent", got)
	}
}

func TestHandlerReleasesIdempotencySlotOnFailure(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(SendEmailPayload{
		Recipient: "ops@example.com",
		Subject:   "hello",
		Body:      "world",
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	store := NewIdempotencyStore()
	handler := Handler{
		Mailer: MailerFunc(func(context.Context, SendEmailPayload) error {
			return errors.New("smtp unavailable")
		}),
		Store: store,
	}
	message := broker.TaskMessage{
		ID:             "task-1",
		IdempotencyKey: "email:ops@example.com",
		Name:           TaskSendEmail,
		Payload:        payload,
	}

	if err := handler.HandleTask(context.Background(), message); err == nil {
		t.Fatalf("HandleTask() error = nil, want failure")
	}
	if got := store.Status("email:ops@example.com"); got != "" {
		t.Fatalf("store.Status() = %q, want empty", got)
	}
	if !store.Begin("email:ops@example.com") {
		t.Fatalf("Begin() should allow retry after failed send")
	}
}

type MailerFunc func(context.Context, SendEmailPayload) error

func (f MailerFunc) Send(ctx context.Context, payload SendEmailPayload) error {
	return f(ctx, payload)
}
