package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/observability"
)

const (
	dlqPrefix       = "dlq."
	streamFieldName = "message"
	defaultListSize = 50
	auditQueue      = "dlq.audit"
)

type auditEvent struct {
	Action      string    `json:"action"`
	Queue       string    `json:"queue"`
	EntryID     string    `json:"entry_id"`
	TaskID      string    `json:"task_id"`
	OccurredAt  time.Time `json:"occurred_at"`
	Description string    `json:"description,omitempty"`
}

type deadLetterStore struct {
	Broker taskforge.Broker
	Client *redis.Client
	Logger *slog.Logger
}

func newDeadLetterStore(client *redis.Client, broker taskforge.Broker, logger *slog.Logger) *deadLetterStore {
	return &deadLetterStore{
		Broker: broker,
		Client: client,
		Logger: logger,
	}
}

func deadLetterQueue(queue string) string {
	if queue == "" {
		queue = "default"
	}
	return dlqPrefix + queue
}

func (s *deadLetterStore) PublishDeadLetter(ctx context.Context, envelope taskforge.DeadLetterEnvelope) error {
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

	msg := taskforge.Task{
		ID:        fmt.Sprintf("dlq:%s", envelope.DeliveryID),
		Name:      "taskforge.dead_letter",
		Queue:     deadLetterQueue(envelope.OriginalTask.Queue),
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

	if _, err := s.Broker.Publish(ctx, msg, taskforge.PublishOptions{
		Source:           taskforge.PublishSourceDeadLetter,
		DeduplicationKey: fmt.Sprintf("dead_letter:%s", envelope.DeliveryID),
	}); err != nil {
		observability.MarkSpanError(span, err)
		return err
	}
	return nil
}

func (s *deadLetterStore) List(ctx context.Context, queue string, limit int64) ([]taskforge.DeadLetterEntry, error) {
	if limit <= 0 {
		limit = defaultListSize
	}

	messages, err := s.Client.XRangeN(ctx, streamKey(deadLetterQueue(queue)), "-", "+", limit).Result()
	if err != nil {
		return nil, fmt.Errorf("list dead-letter entries: %w", err)
	}

	entries := make([]taskforge.DeadLetterEntry, 0, len(messages))
	for _, message := range messages {
		entry, err := decodeDeadLetterEntry(queue, message)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (s *deadLetterStore) Replay(ctx context.Context, queue, entryID string) error {
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

	if _, err := s.Broker.Publish(ctx, replayed, taskforge.PublishOptions{Source: taskforge.PublishSourceDLQReplay}); err != nil {
		observability.MarkSpanError(span, err)
		return fmt.Errorf("replay dead-letter entry: publish original task: %w", err)
	}
	if err := s.deleteEntry(ctx, queue, entry.ID); err != nil {
		observability.MarkSpanError(span, err)
		return err
	}

	return s.appendAudit(ctx, auditEvent{
		Action:      "replay",
		Queue:       queue,
		EntryID:     entry.ID,
		TaskID:      entry.Envelope.OriginalTask.ID,
		OccurredAt:  time.Now().UTC(),
		Description: "replayed dead-letter entry",
	})
}

func (s *deadLetterStore) ReplayBatch(ctx context.Context, queue string, limit int64) (int, error) {
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

func (s *deadLetterStore) Discard(ctx context.Context, queue, entryID, reason string) error {
	entry, err := s.loadEntry(ctx, queue, entryID)
	if err != nil {
		return err
	}
	if err := s.deleteEntry(ctx, queue, entry.ID); err != nil {
		return err
	}

	return s.appendAudit(ctx, auditEvent{
		Action:      "discard",
		Queue:       queue,
		EntryID:     entry.ID,
		TaskID:      entry.Envelope.OriginalTask.ID,
		OccurredAt:  time.Now().UTC(),
		Description: reason,
	})
}

func (s *deadLetterStore) loadEntry(ctx context.Context, queue, entryID string) (taskforge.DeadLetterEntry, error) {
	messages, err := s.Client.XRangeN(ctx, streamKey(deadLetterQueue(queue)), entryID, entryID, 1).Result()
	if err != nil {
		return taskforge.DeadLetterEntry{}, fmt.Errorf("load dead-letter entry %s: %w", entryID, err)
	}
	if len(messages) == 0 {
		return taskforge.DeadLetterEntry{}, taskforge.ErrUnknownDelivery
	}

	return decodeDeadLetterEntry(queue, messages[0])
}

func (s *deadLetterStore) deleteEntry(ctx context.Context, queue, entryID string) error {
	if err := s.Client.XDel(ctx, streamKey(deadLetterQueue(queue)), entryID).Err(); err != nil {
		return fmt.Errorf("delete dead-letter entry %s: %w", entryID, err)
	}
	return nil
}

func (s *deadLetterStore) appendAudit(ctx context.Context, event auditEvent) error {
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

func decodeDeadLetterEntry(queue string, message redis.XMessage) (taskforge.DeadLetterEntry, error) {
	raw, ok := message.Values[streamFieldName]
	if !ok {
		return taskforge.DeadLetterEntry{}, fmt.Errorf("dead-letter entry %s missing %q field", message.ID, streamFieldName)
	}

	payload, err := dlqMessagePayload(raw)
	if err != nil {
		return taskforge.DeadLetterEntry{}, fmt.Errorf("dead-letter entry %s payload: %w", message.ID, err)
	}

	var msg taskforge.Task
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		return taskforge.DeadLetterEntry{}, fmt.Errorf("dead-letter entry %s unmarshal broker message: %w", message.ID, err)
	}

	var envelope taskforge.DeadLetterEnvelope
	if err := json.Unmarshal(msg.Payload, &envelope); err != nil {
		return taskforge.DeadLetterEntry{}, fmt.Errorf("dead-letter entry %s unmarshal envelope: %w", message.ID, err)
	}

	return taskforge.DeadLetterEntry{
		ID:       message.ID,
		Queue:    queue,
		Envelope: envelope,
	}, nil
}

func streamKey(queue string) string {
	return fmt.Sprintf("%s:stream:%s", defaultPrefix, queue)
}

func dlqMessagePayload(raw interface{}) (string, error) {
	switch value := raw.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	default:
		return "", fmt.Errorf("unexpected payload type %T", raw)
	}
}
