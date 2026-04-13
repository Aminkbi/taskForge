package brokerredis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/tasks"
)

const (
	defaultPrefix         = "taskforge"
	defaultReserveTimeout = time.Second
)

type RedisBroker struct {
	client   *redis.Client
	logger   *slog.Logger
	leaseTTL time.Duration
	prefix   string

	mu     sync.Mutex
	active map[string]broker.Delivery
	seen   map[string]broker.Delivery
}

func New(client *redis.Client, logger *slog.Logger, leaseTTL time.Duration) *RedisBroker {
	return &RedisBroker{
		client:   client,
		logger:   logger,
		leaseTTL: leaseTTL,
		prefix:   defaultPrefix,
		active:   make(map[string]broker.Delivery),
		seen:     make(map[string]broker.Delivery),
	}
}

func (b *RedisBroker) Ping(ctx context.Context) error {
	return b.client.Ping(ctx).Err()
}

func (b *RedisBroker) Publish(ctx context.Context, msg broker.TaskMessage) error {
	if msg.ID == "" {
		return fmt.Errorf("publish task: missing id")
	}
	msg = normalizePublishedMessage(msg, time.Now().UTC())

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("publish task: marshal message: %w", err)
	}

	if msg.ETA != nil && msg.ETA.After(time.Now().UTC()) {
		return b.client.ZAdd(ctx, b.delayedKey(), redis.Z{
			Score:  float64(msg.ETA.UnixMilli()),
			Member: payload,
		}).Err()
	}

	return b.client.RPush(ctx, b.queueKey(tasks.EffectiveQueue(msg)), payload).Err()
}

func (b *RedisBroker) Reserve(ctx context.Context, queue, consumerID string) (broker.Delivery, error) {
	result, err := b.client.BLPop(ctx, defaultReserveTimeout, b.queueKey(queue)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return broker.Delivery{}, broker.ErrNoTask
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return broker.Delivery{}, err
		}
		return broker.Delivery{}, fmt.Errorf("reserve task: %w", err)
	}
	if len(result) != 2 {
		return broker.Delivery{}, fmt.Errorf("reserve task: unexpected redis response length %d", len(result))
	}

	var msg broker.TaskMessage
	if err := json.Unmarshal([]byte(result[1]), &msg); err != nil {
		return broker.Delivery{}, fmt.Errorf("reserve task: unmarshal message: %w", err)
	}

	ttl := msg.VisibilityTimeout
	if ttl <= 0 {
		ttl = b.leaseTTL
	}
	now := time.Now().UTC()
	delivery := newDelivery(msg, queue, consumerID, now, ttl)

	b.mu.Lock()
	b.active[delivery.Execution.DeliveryID] = delivery
	b.seen[delivery.Execution.DeliveryID] = delivery
	b.mu.Unlock()

	return delivery, nil
}

func (b *RedisBroker) Ack(_ context.Context, delivery broker.Delivery) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	entry, err := b.validateActiveDeliveryLocked(delivery, time.Now().UTC())
	if err != nil {
		return err
	}

	delete(b.active, entry.Execution.DeliveryID)
	entry.Execution.State = delivery.Execution.State
	entry.Execution.LastError = delivery.Execution.LastError
	b.seen[entry.Execution.DeliveryID] = entry
	return nil
}

func (b *RedisBroker) Nack(ctx context.Context, delivery broker.Delivery, requeue bool) error {
	b.mu.Lock()
	entry, err := b.validateActiveDeliveryLocked(delivery, time.Now().UTC())
	if err == nil {
		delete(b.active, entry.Execution.DeliveryID)
		entry.Execution.State = delivery.Execution.State
		entry.Execution.LastError = delivery.Execution.LastError
		b.seen[entry.Execution.DeliveryID] = entry
	}
	b.mu.Unlock()

	if err != nil {
		return err
	}
	if !requeue {
		return nil
	}

	// TODO: Replace this local requeue path with a durable visibility-timeout implementation.
	entry.Message.ETA = nil
	return b.Publish(ctx, entry.Message)
}

func (b *RedisBroker) ExtendLease(_ context.Context, delivery broker.Delivery, ttl time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	entry, err := b.validateActiveDeliveryLocked(delivery, time.Now().UTC())
	if err != nil {
		return err
	}

	entry.Execution.LeaseExpiresAt = time.Now().UTC().Add(ttl)
	b.active[entry.Execution.DeliveryID] = entry
	b.seen[entry.Execution.DeliveryID] = entry
	return nil
}

func (b *RedisBroker) MoveDue(ctx context.Context, now time.Time, limit int64) (int, error) {
	if limit <= 0 {
		limit = 100
	}

	values, err := b.client.ZRangeByScore(ctx, b.delayedKey(), &redis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%d", now.UTC().UnixMilli()),
		Offset: 0,
		Count:  limit,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("move due tasks: query delayed set: %w", err)
	}

	moved := 0
	for _, raw := range values {
		var msg broker.TaskMessage
		if err := json.Unmarshal([]byte(raw), &msg); err != nil {
			return moved, fmt.Errorf("move due tasks: unmarshal delayed message: %w", err)
		}

		msg.ETA = nil
		payload, err := json.Marshal(msg)
		if err != nil {
			return moved, fmt.Errorf("move due tasks: marshal ready message: %w", err)
		}

		pipe := b.client.TxPipeline()
		pipe.ZRem(ctx, b.delayedKey(), raw)
		pipe.RPush(ctx, b.queueKey(tasks.EffectiveQueue(msg)), payload)

		if _, err := pipe.Exec(ctx); err != nil {
			return moved, fmt.Errorf("move due tasks: release delayed message: %w", err)
		}
		moved++
	}

	return moved, nil
}

func (b *RedisBroker) queueKey(queue string) string {
	return fmt.Sprintf("%s:queue:%s", b.prefix, queue)
}

func (b *RedisBroker) delayedKey() string {
	return fmt.Sprintf("%s:delayed", b.prefix)
}

func normalizePublishedMessage(msg broker.TaskMessage, now time.Time) broker.TaskMessage {
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	if msg.Queue == "" {
		msg.Queue = "default"
	}
	return msg
}

func newDelivery(msg broker.TaskMessage, queue, consumerID string, now time.Time, ttl time.Duration) broker.Delivery {
	firstEnqueuedAt := msg.CreatedAt
	if firstEnqueuedAt.IsZero() {
		firstEnqueuedAt = now
	}

	deliveryCount := msg.Attempt + 1
	if deliveryCount < 1 {
		deliveryCount = 1
	}

	return broker.Delivery{
		Message: msg,
		Execution: broker.ExecutionMetadata{
			TaskID:          msg.ID,
			DeliveryID:      fmt.Sprintf("%s:%s:%d", msg.ID, queue, now.UnixNano()),
			DeliveryCount:   deliveryCount,
			FirstEnqueuedAt: firstEnqueuedAt,
			LeasedAt:        now,
			LeaseExpiresAt:  now.Add(ttl),
			LeaseOwner:      consumerID,
			LastError:       messageLastError(msg),
			State:           string(tasks.StateLeased),
		},
	}
}

func messageLastError(msg broker.TaskMessage) string {
	if msg.Headers == nil {
		return ""
	}
	return msg.Headers["last_error"]
}

func (b *RedisBroker) validateActiveDeliveryLocked(delivery broker.Delivery, now time.Time) (broker.Delivery, error) {
	if delivery.Execution.DeliveryID == "" {
		return broker.Delivery{}, broker.ErrUnknownDelivery
	}

	entry, ok := b.active[delivery.Execution.DeliveryID]
	if !ok {
		if _, seen := b.seen[delivery.Execution.DeliveryID]; seen {
			return broker.Delivery{}, broker.ErrStaleDelivery
		}
		return broker.Delivery{}, broker.ErrUnknownDelivery
	}

	if delivery.Execution.TaskID != "" && delivery.Execution.TaskID != entry.Execution.TaskID {
		return broker.Delivery{}, broker.ErrStaleDelivery
	}
	if delivery.Execution.LeaseOwner != "" && delivery.Execution.LeaseOwner != entry.Execution.LeaseOwner {
		return broker.Delivery{}, broker.ErrStaleDelivery
	}
	if !entry.Execution.LeaseExpiresAt.IsZero() && now.After(entry.Execution.LeaseExpiresAt) {
		return broker.Delivery{}, broker.ErrDeliveryExpired
	}

	return entry, nil
}
