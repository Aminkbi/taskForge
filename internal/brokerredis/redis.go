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
	leased map[string]leasedMessage
}

type leasedMessage struct {
	message broker.TaskMessage
	lease   broker.Lease
}

func New(client *redis.Client, logger *slog.Logger, leaseTTL time.Duration) *RedisBroker {
	return &RedisBroker{
		client:   client,
		logger:   logger,
		leaseTTL: leaseTTL,
		prefix:   defaultPrefix,
		leased:   make(map[string]leasedMessage),
	}
}

func (b *RedisBroker) Ping(ctx context.Context) error {
	return b.client.Ping(ctx).Err()
}

func (b *RedisBroker) Publish(ctx context.Context, msg broker.TaskMessage) error {
	if msg.ID == "" {
		return fmt.Errorf("publish task: missing id")
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now().UTC()
	}
	if msg.Queue == "" {
		msg.Queue = "default"
	}

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

func (b *RedisBroker) Reserve(ctx context.Context, queue, consumerID string) (broker.Lease, broker.TaskMessage, error) {
	result, err := b.client.BLPop(ctx, defaultReserveTimeout, b.queueKey(queue)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return broker.Lease{}, broker.TaskMessage{}, broker.ErrNoTask
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return broker.Lease{}, broker.TaskMessage{}, err
		}
		return broker.Lease{}, broker.TaskMessage{}, fmt.Errorf("reserve task: %w", err)
	}
	if len(result) != 2 {
		return broker.Lease{}, broker.TaskMessage{}, fmt.Errorf("reserve task: unexpected redis response length %d", len(result))
	}

	var msg broker.TaskMessage
	if err := json.Unmarshal([]byte(result[1]), &msg); err != nil {
		return broker.Lease{}, broker.TaskMessage{}, fmt.Errorf("reserve task: unmarshal message: %w", err)
	}

	ttl := msg.VisibilityTimeout
	if ttl <= 0 {
		ttl = b.leaseTTL
	}
	now := time.Now().UTC()
	lease := broker.Lease{
		Token:      fmt.Sprintf("%s:%d", msg.ID, now.UnixNano()),
		Queue:      queue,
		TaskID:     msg.ID,
		ConsumerID: consumerID,
		ExpiresAt:  now.Add(ttl),
	}

	b.mu.Lock()
	b.leased[lease.Token] = leasedMessage{message: msg, lease: lease}
	b.mu.Unlock()

	return lease, msg, nil
}

func (b *RedisBroker) Ack(_ context.Context, lease broker.Lease) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.leased[lease.Token]; !ok {
		return broker.ErrUnknownLease
	}
	delete(b.leased, lease.Token)
	return nil
}

func (b *RedisBroker) Nack(ctx context.Context, lease broker.Lease, requeue bool) error {
	b.mu.Lock()
	entry, ok := b.leased[lease.Token]
	if ok {
		delete(b.leased, lease.Token)
	}
	b.mu.Unlock()

	if !ok {
		return broker.ErrUnknownLease
	}
	if !entry.lease.ExpiresAt.IsZero() && time.Now().UTC().After(entry.lease.ExpiresAt) {
		return broker.ErrLeaseExpired
	}
	if !requeue {
		return nil
	}

	// TODO: Replace this local requeue path with a durable visibility-timeout implementation.
	entry.message.ETA = nil
	return b.Publish(ctx, entry.message)
}

func (b *RedisBroker) ExtendLease(_ context.Context, lease broker.Lease, ttl time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	entry, ok := b.leased[lease.Token]
	if !ok {
		return broker.ErrUnknownLease
	}
	if time.Now().UTC().After(entry.lease.ExpiresAt) {
		return broker.ErrLeaseExpired
	}

	entry.lease.ExpiresAt = time.Now().UTC().Add(ttl)
	b.leased[lease.Token] = entry
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
