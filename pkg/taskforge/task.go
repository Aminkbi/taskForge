package taskforge

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/tasks"
)

type Task struct {
	ID                string            `json:"id"`
	Name              string            `json:"name"`
	Queue             string            `json:"queue"`
	FairnessKey       string            `json:"fairness_key,omitempty"`
	Payload           []byte            `json:"payload"`
	Headers           map[string]string `json:"headers,omitempty"`
	Attempt           int               `json:"attempt"`
	MaxAttempts       int               `json:"max_attempts"`
	VisibilityTimeout time.Duration     `json:"visibility_timeout"`
	ETA               *time.Time        `json:"eta,omitempty"`
	Timeout           *time.Duration    `json:"timeout,omitempty"`
	IdempotencyKey    string            `json:"idempotency_key,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
}

type TaskOption func(*Task)

func NewTask(name string, payload []byte, options ...TaskOption) Task {
	task := Task{
		ID:        uuid.NewString(),
		Name:      name,
		Queue:     "default",
		Payload:   append([]byte(nil), payload...),
		CreatedAt: time.Now().UTC(),
	}
	for _, option := range options {
		if option != nil {
			option(&task)
		}
	}
	return task
}

func JSONTask(name string, payload any, options ...TaskOption) (Task, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return Task{}, err
	}
	return NewTask(name, data, options...), nil
}

func WithID(id string) TaskOption {
	return func(task *Task) {
		task.ID = id
	}
}

func WithQueue(queue string) TaskOption {
	return func(task *Task) {
		task.Queue = queue
	}
}

func WithFairnessKey(key string) TaskOption {
	return func(task *Task) {
		task.FairnessKey = key
	}
}

func WithHeaders(headers map[string]string) TaskOption {
	return func(task *Task) {
		task.Headers = cloneHeaders(headers)
	}
}

func WithHeader(key, value string) TaskOption {
	return func(task *Task) {
		if task.Headers == nil {
			task.Headers = make(map[string]string, 1)
		}
		task.Headers[key] = value
	}
}

func WithETA(eta time.Time) TaskOption {
	return func(task *Task) {
		value := eta.UTC()
		task.ETA = &value
	}
}

func WithTimeout(timeout time.Duration) TaskOption {
	return func(task *Task) {
		task.Timeout = &timeout
	}
}

func WithVisibilityTimeout(timeout time.Duration) TaskOption {
	return func(task *Task) {
		task.VisibilityTimeout = timeout
	}
}

func WithMaxAttempts(maxAttempts int) TaskOption {
	return func(task *Task) {
		task.MaxAttempts = maxAttempts
	}
}

func WithIdempotencyKey(key string) TaskOption {
	return func(task *Task) {
		task.IdempotencyKey = key
	}
}

func EffectiveQueue(task Task) string {
	return tasks.EffectiveQueue(task.toBrokerMessage())
}

func (t Task) toBrokerMessage() broker.TaskMessage {
	return broker.TaskMessage{
		ID:                t.ID,
		Name:              t.Name,
		Queue:             t.Queue,
		FairnessKey:       t.FairnessKey,
		Payload:           append([]byte(nil), t.Payload...),
		Headers:           cloneHeaders(t.Headers),
		Attempt:           t.Attempt,
		MaxAttempts:       t.MaxAttempts,
		VisibilityTimeout: t.VisibilityTimeout,
		ETA:               cloneTimePtr(t.ETA),
		Timeout:           cloneDurationPtr(t.Timeout),
		IdempotencyKey:    t.IdempotencyKey,
		CreatedAt:         t.CreatedAt,
	}
}

func taskFromBrokerMessage(msg broker.TaskMessage) Task {
	return Task{
		ID:                msg.ID,
		Name:              msg.Name,
		Queue:             msg.Queue,
		FairnessKey:       msg.FairnessKey,
		Payload:           append([]byte(nil), msg.Payload...),
		Headers:           cloneHeaders(msg.Headers),
		Attempt:           msg.Attempt,
		MaxAttempts:       msg.MaxAttempts,
		VisibilityTimeout: msg.VisibilityTimeout,
		ETA:               cloneTimePtr(msg.ETA),
		Timeout:           cloneDurationPtr(msg.Timeout),
		IdempotencyKey:    msg.IdempotencyKey,
		CreatedAt:         msg.CreatedAt,
	}
}

func cloneHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(headers))
	for key, value := range headers {
		cloned[key] = value
	}
	return cloned
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := value.UTC()
	return &cloned
}

func cloneDurationPtr(value *time.Duration) *time.Duration {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
