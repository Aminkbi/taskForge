package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/observability"
)

const (
	dlqPrefix       = "dlq."
	redisKeyPrefix  = "taskforge"
	streamFieldName = "message"
	defaultListSize = 50
	auditQueue      = "dlq.audit"
)

type FailureClass string

const (
	FailureClassTransientRetryable FailureClass = "transient_retryable"
	FailureClassPermanent          FailureClass = "permanent"
	FailureClassLeaseLost          FailureClass = "lease_lost"
	FailureClassTimeout            FailureClass = "timeout"
	FailureClassDecodeValidation   FailureClass = "decode_or_validation"
	FailureClassOverloaded         FailureClass = "overloaded"
)

type Envelope struct {
	OriginalTask     broker.TaskMessage `json:"original_task"`
	FailureClass     FailureClass       `json:"failure_class"`
	LastError        string             `json:"last_error"`
	DeliveryCount    int                `json:"delivery_count"`
	FirstEnqueuedAt  time.Time          `json:"first_enqueued_at"`
	LastFailureAt    time.Time          `json:"last_failure_at"`
	WorkerIdentity   string             `json:"worker_identity"`
	DeliveryID       string             `json:"delivery_id"`
	TraceID          string             `json:"trace_id,omitempty"`
	OriginalQueue    string             `json:"original_queue"`
	OriginalTaskName string             `json:"original_task_name"`
}

type Entry struct {
	ID       string   `json:"id"`
	Queue    string   `json:"queue"`
	Envelope Envelope `json:"envelope"`
}

type AuditEvent struct {
	Action      string    `json:"action"`
	Queue       string    `json:"queue"`
	EntryID     string    `json:"entry_id"`
	TaskID      string    `json:"task_id"`
	OccurredAt  time.Time `json:"occurred_at"`
	Description string    `json:"description,omitempty"`
}

type Publisher interface {
	PublishDeadLetter(ctx context.Context, envelope Envelope) error
}

type Admin interface {
	List(ctx context.Context, queue string, limit int64) ([]Entry, error)
	Replay(ctx context.Context, queue, entryID string) error
	ReplayBatch(ctx context.Context, queue string, limit int64) (int, error)
	Discard(ctx context.Context, queue, entryID, reason string) error
}

type Service struct {
	Broker broker.Broker
	Client *redis.Client
	Logger *slog.Logger
}

func NewService(client *redis.Client, broker broker.Broker, logger *slog.Logger) *Service {
	return &Service{
		Broker: broker,
		Client: client,
		Logger: logger,
	}
}

func DeadLetterQueue(queue string) string {
	if queue == "" {
		queue = "default"
	}
	return dlqPrefix + queue
}

func NewEnvelope(delivery broker.Delivery, class FailureClass, lastError string, failedAt time.Time) Envelope {
	return Envelope{
		OriginalTask:     delivery.Message,
		FailureClass:     class,
		LastError:        lastError,
		DeliveryCount:    delivery.Execution.DeliveryCount,
		FirstEnqueuedAt:  delivery.Execution.FirstEnqueuedAt,
		LastFailureAt:    failedAt.UTC(),
		WorkerIdentity:   delivery.Execution.LeaseOwner,
		DeliveryID:       delivery.Execution.DeliveryID,
		TraceID:          observability.TraceIDFromHeaders(delivery.Message.Headers),
		OriginalQueue:    delivery.Message.Queue,
		OriginalTaskName: delivery.Message.Name,
	}
}

func (s *Service) PublishDeadLetter(ctx context.Context, envelope Envelope) error {
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.dlq",
		"taskforge.dead_letter_publish",
		envelope.OriginalTask,
		attribute.String("taskforge.result_class", string(envelope.FailureClass)),
	)
	defer span.End()

	payload, err := json.Marshal(envelope)
	if err != nil {
		observability.MarkSpanError(span, err)
		return fmt.Errorf("publish dead-letter envelope: marshal envelope: %w", err)
	}

	msg := broker.TaskMessage{
		ID:        fmt.Sprintf("dlq:%s", envelope.DeliveryID),
		Name:      "taskforge.dead_letter",
		Queue:     DeadLetterQueue(envelope.OriginalTask.Queue),
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
		Headers: map[string]string{
			"dead_letter_failure_class": string(envelope.FailureClass),
			"dead_letter_last_error":    envelope.LastError,
		},
	}
	if envelope.TraceID != "" {
		msg.Headers["dead_letter_trace_id"] = envelope.TraceID
	}

	if _, err := s.Broker.Publish(ctx, msg, broker.PublishOptions{
		Source:           broker.PublishSourceDeadLetter,
		DeduplicationKey: fmt.Sprintf("dead_letter:%s", envelope.DeliveryID),
	}); err != nil {
		observability.MarkSpanError(span, err)
		return err
	}
	return nil
}

func (s *Service) List(ctx context.Context, queue string, limit int64) ([]Entry, error) {
	if limit <= 0 {
		limit = defaultListSize
	}

	messages, err := s.Client.XRangeN(ctx, streamKey(DeadLetterQueue(queue)), "-", "+", limit).Result()
	if err != nil {
		return nil, fmt.Errorf("list dead-letter entries: %w", err)
	}

	entries := make([]Entry, 0, len(messages))
	for _, message := range messages {
		entry, err := decodeDeadLetterEntry(queue, message)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (s *Service) Replay(ctx context.Context, queue, entryID string) error {
	entry, err := s.loadEntry(ctx, queue, entryID)
	if err != nil {
		return err
	}
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.dlq",
		"taskforge.dead_letter_replay",
		entry.Envelope.OriginalTask,
		attribute.String("taskforge.delivery_id", entry.Envelope.DeliveryID),
	)
	defer span.End()

	replayed := entry.Envelope.OriginalTask
	replayed.ETA = nil
	if replayed.Headers == nil {
		replayed.Headers = map[string]string{}
	}
	replayed.Headers["dead_letter_replayed_from"] = entry.ID
	replayed.Headers["dead_letter_replayed_at"] = time.Now().UTC().Format(time.RFC3339Nano)

	if _, err := s.Broker.Publish(ctx, replayed, broker.PublishOptions{Source: broker.PublishSourceDLQReplay}); err != nil {
		observability.MarkSpanError(span, err)
		return fmt.Errorf("replay dead-letter entry: publish original task: %w", err)
	}
	if err := s.deleteEntry(ctx, queue, entry.ID); err != nil {
		observability.MarkSpanError(span, err)
		return err
	}

	return s.appendAudit(ctx, AuditEvent{
		Action:      "replay",
		Queue:       queue,
		EntryID:     entry.ID,
		TaskID:      entry.Envelope.OriginalTask.ID,
		OccurredAt:  time.Now().UTC(),
		Description: "replayed dead-letter entry",
	})
}

func (s *Service) ReplayBatch(ctx context.Context, queue string, limit int64) (int, error) {
	entries, err := s.List(ctx, queue, limit)
	if err != nil {
		return 0, err
	}

	replayed := 0
	for _, entry := range entries {
		if err := s.Replay(ctx, queue, entry.ID); err != nil {
			return replayed, err
		}
		replayed++
	}

	return replayed, nil
}

func (s *Service) Discard(ctx context.Context, queue, entryID, reason string) error {
	entry, err := s.loadEntry(ctx, queue, entryID)
	if err != nil {
		return err
	}
	if err := s.deleteEntry(ctx, queue, entry.ID); err != nil {
		return err
	}

	return s.appendAudit(ctx, AuditEvent{
		Action:      "discard",
		Queue:       queue,
		EntryID:     entry.ID,
		TaskID:      entry.Envelope.OriginalTask.ID,
		OccurredAt:  time.Now().UTC(),
		Description: reason,
	})
}

func (s *Service) loadEntry(ctx context.Context, queue, entryID string) (Entry, error) {
	messages, err := s.Client.XRangeN(ctx, streamKey(DeadLetterQueue(queue)), entryID, entryID, 1).Result()
	if err != nil {
		return Entry{}, fmt.Errorf("load dead-letter entry %s: %w", entryID, err)
	}
	if len(messages) == 0 {
		return Entry{}, broker.ErrUnknownDelivery
	}

	return decodeDeadLetterEntry(queue, messages[0])
}

func (s *Service) deleteEntry(ctx context.Context, queue, entryID string) error {
	if err := s.Client.XDel(ctx, streamKey(DeadLetterQueue(queue)), entryID).Err(); err != nil {
		return fmt.Errorf("delete dead-letter entry %s: %w", entryID, err)
	}
	return nil
}

func (s *Service) appendAudit(ctx context.Context, event AuditEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal dead-letter audit event: %w", err)
	}

	if _, err := s.Client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey(auditQueue),
		Values: map[string]interface{}{streamFieldName: string(payload)},
	}).Result(); err != nil {
		return fmt.Errorf("append dead-letter audit event: %w", err)
	}
	return nil
}

func decodeDeadLetterEntry(queue string, message redis.XMessage) (Entry, error) {
	raw, ok := message.Values[streamFieldName]
	if !ok {
		return Entry{}, fmt.Errorf("dead-letter entry %s missing %q field", message.ID, streamFieldName)
	}

	payload, err := messagePayload(raw)
	if err != nil {
		return Entry{}, fmt.Errorf("dead-letter entry %s payload: %w", message.ID, err)
	}

	var msg broker.TaskMessage
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		return Entry{}, fmt.Errorf("dead-letter entry %s unmarshal broker message: %w", message.ID, err)
	}

	var envelope Envelope
	if err := json.Unmarshal(msg.Payload, &envelope); err != nil {
		return Entry{}, fmt.Errorf("dead-letter entry %s unmarshal envelope: %w", message.ID, err)
	}

	return Entry{
		ID:       message.ID,
		Queue:    queue,
		Envelope: envelope,
	}, nil
}

func streamKey(queue string) string {
	return fmt.Sprintf("%s:stream:%s", redisKeyPrefix, queue)
}

func messagePayload(raw interface{}) (string, error) {
	switch value := raw.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	default:
		return "", fmt.Errorf("unexpected payload type %T", raw)
	}
}
