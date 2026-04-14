package email

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/aminkbi/taskforge/internal/broker"
)

const TaskSendEmail = "example.email.send"

type SendEmailPayload struct {
	Recipient string `json:"recipient"`
	Subject   string `json:"subject"`
	Body      string `json:"body"`
}

type Mailer interface {
	Send(context.Context, SendEmailPayload) error
}

type Handler struct {
	Mailer Mailer
	Store  *IdempotencyStore
	Logger *slog.Logger
}

func (h Handler) HandleTask(ctx context.Context, msg broker.TaskMessage) error {
	if msg.Name != TaskSendEmail {
		return fmt.Errorf("email example: unsupported task %q", msg.Name)
	}
	if h.Mailer == nil {
		return fmt.Errorf("email example: missing mailer")
	}
	if h.Store == nil {
		return fmt.Errorf("email example: missing idempotency store")
	}

	var payload SendEmailPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("email example: decode payload: %w", err)
	}
	if payload.Recipient == "" {
		return fmt.Errorf("email example: recipient is required")
	}

	key := msg.IdempotencyKey
	if key == "" {
		key = msg.ID
	}
	if key == "" {
		return fmt.Errorf("email example: missing task id and idempotency key")
	}

	if !h.Store.Begin(key) {
		if h.Logger != nil {
			h.Logger.Info("skipped duplicate email task", "idempotency_key", key, "task_id", msg.ID)
		}
		return nil
	}

	if err := h.Mailer.Send(ctx, payload); err != nil {
		h.Store.Fail(key)
		return err
	}

	h.Store.Complete(key)
	if h.Logger != nil {
		h.Logger.Info("sent email", "idempotency_key", key, "recipient", payload.Recipient, "task_id", msg.ID)
	}
	return nil
}

type IdempotencyStore struct {
	mu      sync.Mutex
	entries map[string]string
}

func NewIdempotencyStore() *IdempotencyStore {
	return &IdempotencyStore{entries: map[string]string{}}
}

func (s *IdempotencyStore) Begin(key string) bool {
	if s == nil || key == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.entries[key]; exists {
		return false
	}
	s.entries[key] = "in_flight"
	return true
}

func (s *IdempotencyStore) Complete(key string) {
	if s == nil || key == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[key] = "sent"
}

func (s *IdempotencyStore) Fail(key string) {
	if s == nil || key == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
}

func (s *IdempotencyStore) Status(key string) string {
	if s == nil || key == "" {
		return ""
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[key]
}

type CaptureMailer struct {
	mu   sync.Mutex
	sent []SendEmailPayload
}

func (m *CaptureMailer) Send(_ context.Context, payload SendEmailPayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, payload)
	return nil
}

func (m *CaptureMailer) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sent)
}
