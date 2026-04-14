package scheduler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisScheduleStateStore struct {
	client *redis.Client
	prefix string
}

func NewRedisScheduleStateStore(client *redis.Client) *RedisScheduleStateStore {
	return &RedisScheduleStateStore{
		client: client,
		prefix: defaultSchedulerPrefix,
	}
}

func (s *RedisScheduleStateStore) Load(ctx context.Context, scheduleID string) (ScheduleState, bool, error) {
	payload, err := s.client.Get(ctx, s.stateKey(scheduleID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return ScheduleState{}, false, nil
		}
		return ScheduleState{}, false, fmt.Errorf("load schedule state: %w", err)
	}

	var state ScheduleState
	if err := json.Unmarshal(payload, &state); err != nil {
		return ScheduleState{}, false, fmt.Errorf("unmarshal schedule state: %w", err)
	}
	return state, true, nil
}

func (s *RedisScheduleStateStore) Save(ctx context.Context, scheduleID string, state ScheduleState) error {
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal schedule state: %w", err)
	}
	if err := s.client.Set(ctx, s.stateKey(scheduleID), payload, 0).Err(); err != nil {
		return fmt.Errorf("save schedule state: %w", err)
	}
	return nil
}

func (s *RedisScheduleStateStore) stateKey(scheduleID string) string {
	return fmt.Sprintf("%s:schedule:state:%s", s.prefix, scheduleID)
}
