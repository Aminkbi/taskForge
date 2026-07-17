package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge"
)

var recordStatesScript = redis.NewScript(`
local position = 1
for _, key in ipairs(KEYS) do
  local ttl = tonumber(ARGV[position]) or -1
  local fieldCount = tonumber(ARGV[position + 1]) or 0
  position = position + 2
  local fields = {}
  for index = 1, fieldCount * 2 do
    fields[index] = ARGV[position]
    position = position + 1
  end
  if #fields > 0 then
    redis.call("HSET", key, unpack(fields))
  end
  if ttl > 0 then
    redis.call("PEXPIRE", key, ttl)
  else
    redis.call("PERSIST", key)
  end
end
return #KEYS
`)

type stateStore struct {
	client    *redis.Client
	prefix    string
	retention taskforge.RetentionPolicy
}

func newStateStore(client *redis.Client, retention taskforge.RetentionPolicy) *stateStore {
	return &stateStore{
		client:    client,
		prefix:    defaultPrefix,
		retention: retention,
	}
}

func (s *stateStore) RecordQueued(ctx context.Context, msg taskforge.Task) error {
	now := time.Now().UTC()
	record, err := s.queuedRecord(msg, now)
	if err != nil {
		return err
	}
	if err := s.recordStates(ctx, []stateRecord{record}); err != nil {
		return fmt.Errorf("record queued task %s: %w", record.taskID, err)
	}
	return nil
}

func (s *stateStore) queuedRecord(msg taskforge.Task, now time.Time) (stateRecord, error) {
	taskID := msg.ID
	if taskID == "" {
		return stateRecord{}, fmt.Errorf("record queued task: missing id")
	}
	createdAt := msg.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	return stateRecord{taskID: taskID, state: taskforge.StateQueued, fields: map[string]any{
		"task_id":    taskID,
		"name":       msg.Name,
		"queue":      taskforge.EffectiveQueue(msg),
		"state":      string(taskforge.StateQueued),
		"created_at": formatTime(createdAt),
		"updated_at": formatTime(now),
	}}, nil
}

func (s *stateStore) RecordDelivery(ctx context.Context, delivery taskforge.Delivery, state taskforge.State, resultPayload []byte) error {
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
		"queue":            taskforge.EffectiveQueue(msg),
		"state":            string(state),
		"last_error":       delivery.Execution.LastError,
		"created_at":       formatTime(createdAt),
		"updated_at":       formatTime(now),
		"delivery_count":   delivery.Execution.DeliveryCount,
		"last_delivery_id": delivery.Execution.DeliveryID,
		"last_lease_owner": delivery.Execution.LeaseOwner,
	}
	if state == taskforge.StateRunning {
		fields["started_at"] = formatTime(now)
	}
	if taskforge.CompletesTask(state) {
		fields["completed_at"] = formatTime(now)
	}

	if err := s.recordStates(ctx, []stateRecord{{taskID: taskID, state: state, fields: fields}}); err != nil {
		return fmt.Errorf("record task %s state %s: %w", taskID, state, err)
	}
	if len(resultPayload) > 0 {
		if err := s.storePayload(ctx, taskID, resultPayload); err != nil {
			return err
		}
	}
	return nil
}

func (s *stateStore) RecordDeliveryBatch(ctx context.Context, deliveries []taskforge.Delivery, state taskforge.State) error {
	records := make([]stateRecord, 0, len(deliveries))
	for _, delivery := range deliveries {
		record, err := s.deliveryRecord(delivery, state)
		if err != nil {
			return err
		}
		records = append(records, record)
	}
	if err := s.recordStates(ctx, records); err != nil {
		return fmt.Errorf("record %d task deliveries state %s: %w", len(deliveries), state, err)
	}
	return nil
}

func (s *stateStore) deliveryRecord(delivery taskforge.Delivery, state taskforge.State) (stateRecord, error) {
	now := time.Now().UTC()
	msg := delivery.Message
	taskID := delivery.Execution.TaskID
	if taskID == "" {
		taskID = msg.ID
	}
	if taskID == "" {
		return stateRecord{}, fmt.Errorf("record task delivery: missing task id")
	}
	createdAt := msg.CreatedAt
	if createdAt.IsZero() {
		createdAt = delivery.Execution.FirstEnqueuedAt
	}
	if createdAt.IsZero() {
		createdAt = now
	}
	return stateRecord{taskID: taskID, state: state, fields: deliveryStateFields(delivery, state, createdAt, now)}, nil
}

type stateRecord struct {
	taskID string
	state  taskforge.State
	fields map[string]any
}

func (s *stateStore) recordStates(ctx context.Context, records []stateRecord) error {
	if len(records) == 0 {
		return nil
	}
	keys := make([]string, 0, len(records))
	args := make([]any, 0, len(records)*22)
	for _, record := range records {
		keys = append(keys, s.taskKey(record.taskID))
		ttl := s.recordTTL(record.state)
		ttlMillis := int64(-1)
		if ttl > 0 {
			ttlMillis = ttl.Milliseconds()
		}
		args = append(args, ttlMillis, len(record.fields))
		for field, value := range record.fields {
			args = append(args, field, value)
		}
	}
	return recordStatesScript.Run(ctx, s.client, keys, args...).Err()
}

func deliveryStateFields(delivery taskforge.Delivery, state taskforge.State, createdAt, now time.Time) map[string]any {
	msg := delivery.Message
	taskID := delivery.Execution.TaskID
	if taskID == "" {
		taskID = msg.ID
	}
	fields := map[string]any{
		"task_id":          taskID,
		"name":             msg.Name,
		"queue":            taskforge.EffectiveQueue(msg),
		"state":            string(state),
		"last_error":       delivery.Execution.LastError,
		"created_at":       formatTime(createdAt),
		"updated_at":       formatTime(now),
		"delivery_count":   delivery.Execution.DeliveryCount,
		"last_delivery_id": delivery.Execution.DeliveryID,
		"last_lease_owner": delivery.Execution.LeaseOwner,
	}
	if state == taskforge.StateRunning {
		fields["started_at"] = formatTime(now)
	}
	if taskforge.CompletesTask(state) {
		fields["completed_at"] = formatTime(now)
	}
	return fields
}

func (s *stateStore) Get(ctx context.Context, taskID string) (taskforge.TaskRecord, error) {
	values, err := s.client.HGetAll(ctx, s.taskKey(taskID)).Result()
	if err != nil {
		return taskforge.TaskRecord{}, fmt.Errorf("get task %s: %w", taskID, err)
	}
	if len(values) == 0 {
		return taskforge.TaskRecord{}, taskforge.ErrTaskNotFound
	}

	record := taskforge.TaskRecord{
		TaskID:         values["task_id"],
		Name:           values["name"],
		Queue:          values["queue"],
		State:          taskforge.State(values["state"]),
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
		return taskforge.TaskRecord{}, fmt.Errorf("get task %s payload: %w", taskID, err)
	}
	if err == nil {
		record.ResultPayload = payload
	}
	return record, nil
}

func (s *stateStore) storePayload(ctx context.Context, taskID string, payload []byte) error {
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

func (s *stateStore) recordTTL(state taskforge.State) time.Duration {
	switch state {
	case taskforge.StateSucceeded:
		return s.retention.SucceededState
	case taskforge.StateRetryScheduled, taskforge.StateDeadLettered:
		return s.retention.FailedState
	default:
		return 0
	}
}

func (s *stateStore) taskKey(taskID string) string {
	return fmt.Sprintf("%s:task:%s", s.prefix, taskID)
}

func (s *stateStore) payloadKey(taskID string) string {
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
