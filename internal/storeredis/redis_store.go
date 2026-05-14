package storeredis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/store"
	"github.com/aminkbi/taskforge/internal/tasks"
)

const defaultPrefix = "taskforge"

type RedisStore struct {
	client    *redis.Client
	prefix    string
	retention store.RetentionPolicy
}

func New(client *redis.Client, retention store.RetentionPolicy) *RedisStore {
	return &RedisStore{
		client:    client,
		prefix:    defaultPrefix,
		retention: retention,
	}
}

func (s *RedisStore) Save(ctx context.Context, taskID string, state tasks.State, payload []byte) error {
	return s.RecordDelivery(ctx, broker.Delivery{
		Message: broker.TaskMessage{ID: taskID},
		Execution: broker.ExecutionMetadata{
			TaskID: taskID,
			State:  string(state),
		},
	}, state, payload)
}

func (s *RedisStore) RecordQueued(ctx context.Context, msg broker.TaskMessage) error {
	now := time.Now().UTC()
	taskID := msg.ID
	if taskID == "" {
		return fmt.Errorf("record queued task: missing id")
	}
	createdAt := msg.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}

	fields := map[string]any{
		"task_id":    taskID,
		"name":       msg.Name,
		"queue":      tasks.EffectiveQueue(msg),
		"state":      string(tasks.StateQueued),
		"created_at": formatTime(createdAt),
		"updated_at": formatTime(now),
	}
	if err := s.client.HSet(ctx, s.taskKey(taskID), fields).Err(); err != nil {
		return fmt.Errorf("record queued task %s: %w", taskID, err)
	}
	return s.applyRecordTTL(ctx, taskID, tasks.StateQueued)
}

func (s *RedisStore) RecordDelivery(ctx context.Context, delivery broker.Delivery, state tasks.State, resultPayload []byte) error {
	now := time.Now().UTC()
	msg := delivery.Message
	taskID := delivery.Execution.TaskID
	if taskID == "" {
		taskID = msg.ID
	}
	if taskID == "" {
		return fmt.Errorf("record task delivery: missing task id")
	}
	createdAt := msg.CreatedAt
	if createdAt.IsZero() {
		createdAt = delivery.Execution.FirstEnqueuedAt
	}
	if createdAt.IsZero() {
		createdAt = now
	}

	fields := map[string]any{
		"task_id":          taskID,
		"name":             msg.Name,
		"queue":            tasks.EffectiveQueue(msg),
		"state":            string(state),
		"last_error":       delivery.Execution.LastError,
		"created_at":       formatTime(createdAt),
		"updated_at":       formatTime(now),
		"delivery_count":   delivery.Execution.DeliveryCount,
		"last_delivery_id": delivery.Execution.DeliveryID,
		"last_lease_owner": delivery.Execution.LeaseOwner,
	}
	if state == tasks.StateRunning {
		fields["started_at"] = formatTime(now)
	}
	if tasks.IsTerminal(state) {
		fields["completed_at"] = formatTime(now)
	}

	key := s.taskKey(taskID)
	if err := s.client.HSet(ctx, key, fields).Err(); err != nil {
		return fmt.Errorf("record task %s state %s: %w", taskID, state, err)
	}
	if len(resultPayload) > 0 {
		if err := s.storePayload(ctx, taskID, resultPayload); err != nil {
			return err
		}
	}
	return s.applyRecordTTL(ctx, taskID, state)
}

func (s *RedisStore) Get(ctx context.Context, taskID string) (store.TaskRecord, error) {
	values, err := s.client.HGetAll(ctx, s.taskKey(taskID)).Result()
	if err != nil {
		return store.TaskRecord{}, fmt.Errorf("get task %s: %w", taskID, err)
	}
	if len(values) == 0 {
		return store.TaskRecord{}, store.ErrTaskNotFound
	}

	record := store.TaskRecord{
		TaskID:         values["task_id"],
		Name:           values["name"],
		Queue:          values["queue"],
		State:          tasks.State(values["state"]),
		LastError:      values["last_error"],
		CreatedAt:      parseTime(values["created_at"]),
		StartedAt:      parseTime(values["started_at"]),
		CompletedAt:    parseTime(values["completed_at"]),
		UpdatedAt:      parseTime(values["updated_at"]),
		DeliveryCount:  parseInt(values["delivery_count"]),
		LastDeliveryID: values["last_delivery_id"],
		LastLeaseOwner: values["last_lease_owner"],
	}
	payload, err := s.client.Get(ctx, s.payloadKey(taskID)).Bytes()
	if err != nil && !errors.Is(err, redis.Nil) {
		return store.TaskRecord{}, fmt.Errorf("get task %s payload: %w", taskID, err)
	}
	if err == nil {
		record.ResultPayload = payload
	}
	return record, nil
}

func (s *RedisStore) storePayload(ctx context.Context, taskID string, payload []byte) error {
	key := s.payloadKey(taskID)
	if s.retention.ResultPayload > 0 {
		if err := s.client.Set(ctx, key, payload, s.retention.ResultPayload).Err(); err != nil {
			return fmt.Errorf("store task %s payload: %w", taskID, err)
		}
		return nil
	}
	if err := s.client.Set(ctx, key, payload, 0).Err(); err != nil {
		return fmt.Errorf("store task %s payload: %w", taskID, err)
	}
	return nil
}

func (s *RedisStore) applyRecordTTL(ctx context.Context, taskID string, state tasks.State) error {
	ttl := s.recordTTL(state)
	if ttl <= 0 {
		return s.client.Persist(ctx, s.taskKey(taskID)).Err()
	}
	return s.client.Expire(ctx, s.taskKey(taskID), ttl).Err()
}

func (s *RedisStore) recordTTL(state tasks.State) time.Duration {
	switch state {
	case tasks.StateSucceeded:
		return s.retention.SucceededState
	case tasks.StateRetryScheduled, tasks.StateDeadLettered:
		return s.retention.FailedState
	default:
		return 0
	}
}

func (s *RedisStore) taskKey(taskID string) string {
	return fmt.Sprintf("%s:task:%s", s.prefix, taskID)
}

func (s *RedisStore) payloadKey(taskID string) string {
	return fmt.Sprintf("%s:task:%s:payload", s.prefix, taskID)
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func parseInt(value string) int {
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}
