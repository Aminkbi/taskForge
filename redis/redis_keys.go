package redis

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"

	"github.com/aminkbi/taskforge"
)

func (b *Broker) effectiveLeaseTTL(msg taskforge.Task) time.Duration {
	if msg.VisibilityTimeout > 0 {
		return normalizeLeaseTTL(msg.VisibilityTimeout)
	}
	return normalizeLeaseTTL(b.leaseTTL)
}

func (b *Broker) streamKey(queue string) string {
	return b.prefix + ":stream:" + normalizeQueue(queue)
}

func (b *Broker) groupName(queue string) string {
	return b.prefix + ":" + normalizeQueue(queue)
}

func (b *Broker) consumerName(consumerID string) string {
	base := consumerID
	if base == "" {
		base = "worker"
	}
	return base + ":" + b.hostname + ":" + b.instanceID
}

func (b *Broker) delayedQueueKey(queue string) string {
	return b.prefix + ":delayed:queue:" + normalizeQueue(queue)
}

func (b *Broker) delayedQueueIndexKey() string {
	return b.prefix + ":delayed:queues"
}

func (b *Broker) delayedRetryIndexKey(queue string) string {
	return b.prefix + ":delayed:retry:" + normalizeQueue(queue)
}

func (b *Broker) schedulerLeadershipKey() string {
	return b.prefix + ":scheduler:leader"
}

func (b *Broker) publishReceiptKey(deduplicationKey string) string {
	sum := sha256Sum(deduplicationKey)
	return b.prefix + ":publish:receipt:" + hex.EncodeToString(sum[:])
}

func (b *Broker) publishReceiptTTL() time.Duration {
	return defaultPublishReceiptTTL
}

func (b *Broker) publishReceiptExists(ctx context.Context, deduplicationKey string) (bool, error) {
	exists, err := b.client.Exists(ctx, b.publishReceiptKey(deduplicationKey)).Result()
	if err != nil {
		return false, fmt.Errorf("publish task: inspect receipt: %w", err)
	}
	return exists > 0, nil
}

func (b *Broker) refreshDelayedQueueIndex(ctx context.Context, queue string) error {
	queue = normalizeQueue(queue)
	values, err := b.client.ZRangeWithScores(ctx, b.delayedQueueKey(queue), 0, 0).Result()
	if err != nil {
		return fmt.Errorf("refresh delayed queue index %q: %w", queue, err)
	}
	if len(values) == 0 {
		if err := b.client.ZRem(ctx, b.delayedQueueIndexKey(), queue).Err(); err != nil {
			return fmt.Errorf("refresh delayed queue index %q: remove queue: %w", queue, err)
		}
		return nil
	}
	if err := b.client.ZAdd(ctx, b.delayedQueueIndexKey(), redis.Z{
		Score:  values[0].Score,
		Member: queue,
	}).Err(); err != nil {
		return fmt.Errorf("refresh delayed queue index %q: update queue: %w", queue, err)
	}
	return nil
}

func normalizeQueue(queue string) string {
	if queue == "" {
		return "default"
	}
	return queue
}

func normalizePublishedMessage(msg taskforge.Task, now time.Time) taskforge.Task {
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	if msg.Queue == "" {
		msg.Queue = "default"
	}
	msg.FairnessKey = strings.TrimSpace(msg.FairnessKey)
	return msg
}

func decodeTask(entry redis.XMessage) (taskforge.Task, error) {
	raw, ok := entry.Values[streamPayloadField]
	if !ok {
		return taskforge.Task{}, fmt.Errorf("missing %q field", streamPayloadField)
	}

	payload, err := messagePayload(raw)
	if err != nil {
		return taskforge.Task{}, err
	}

	var msg taskforge.Task
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		return taskforge.Task{}, fmt.Errorf("unmarshal message: %w", err)
	}
	return msg, nil
}

func messagePayload(raw interface{}) (string, error) {
	switch value := raw.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	default:
		return "", fmt.Errorf("unexpected stream payload type %T", raw)
	}
}

type delayedEntry struct {
	EntryID      string         `json:"entry_id"`
	ScheduledFor time.Time      `json:"scheduled_for"`
	Message      taskforge.Task `json:"message"`
}

func decodeDelayedEntry(raw string) (delayedEntry, error) {
	var entry delayedEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return delayedEntry{}, err
	}
	if entry.EntryID == "" {
		return delayedEntry{}, fmt.Errorf("missing delayed entry id")
	}
	return entry, nil
}

func retryIndexFlag(msg taskforge.Task) string {
	if msg.Headers != nil && msg.Headers[taskforge.HeaderRetryScheduledAt] != "" {
		return "1"
	}
	return "0"
}

func deliveryCount(msg taskforge.Task, fallback int64) int {
	count := msg.Attempt + 1
	if fallback > int64(count) {
		count = int(fallback)
	}
	if count < 1 {
		return 1
	}
	return count
}

func redisScriptInt(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected redis script integer type %T", value)
	}
}

func newDelivery(msg taskforge.Task, queue, consumerID, deliveryID string, now time.Time, ttl time.Duration, count int) taskforge.Delivery {
	firstEnqueuedAt := msg.CreatedAt
	if firstEnqueuedAt.IsZero() {
		firstEnqueuedAt = now
	}

	return taskforge.Delivery{
		Message: msg,
		Execution: taskforge.ExecutionMetadata{
			TaskID:          msg.ID,
			DeliveryID:      deliveryID,
			DeliveryCount:   count,
			FirstEnqueuedAt: firstEnqueuedAt,
			LeasedAt:        now,
			LeaseExpiresAt:  now.Add(ttl),
			LeaseOwner:      consumerID,
			LastError:       messageLastError(msg),
			State:           taskforge.StateLeased,
		},
	}
}

func messageLastError(msg taskforge.Task) string {
	if msg.Headers == nil {
		return ""
	}
	return msg.Headers["last_error"]
}

func deliverySpanAttributes(delivery taskforge.Delivery) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("taskforge.delivery_id", delivery.Execution.DeliveryID),
		attribute.String("taskforge.worker_identity", delivery.Execution.LeaseOwner),
		attribute.Int("taskforge.delivery_count", delivery.Execution.DeliveryCount),
	}
}
