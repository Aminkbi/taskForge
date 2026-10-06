package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aminkbi/taskforge"
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

func (s *RedisScheduleStateStore) ReconcileConfigured(ctx context.Context, fence taskforge.LeadershipFence, schedules []ScheduleDefinition, now time.Time) error {
	configuredIDs := make([]string, 0, len(schedules))
	configuredSet := make(map[string]struct{}, len(schedules))
	configuredStateKeys := make([]string, 0, len(schedules))
	for _, schedule := range schedules {
		configuredIDs = append(configuredIDs, schedule.ID)
		configuredSet[schedule.ID] = struct{}{}
		configuredStateKeys = append(configuredStateKeys, s.stateKey(schedule.ID))
	}

	var persistedIDs []string
	err := s.execWithFence(ctx, fence, "reconcile_configured", func() ([]string, error) {
		var err error
		persistedIDs, err = s.client.SMembers(ctx, s.scheduleIDsKey()).Result()
		if err != nil {
			return nil, fmt.Errorf("load recurring schedule ids: %w", err)
		}

		watchKeys := []string{s.leadershipKey(), s.scheduleIDsKey(), s.dueIndexKey()}
		watchKeys = append(watchKeys, configuredStateKeys...)
		for _, scheduleID := range persistedIDs {
			watchKeys = append(watchKeys, s.stateKey(scheduleID))
		}
		return watchKeys, nil
	}, func(tx *redis.Tx) error {
		currentIDs, err := tx.SMembers(ctx, s.scheduleIDsKey()).Result()
		if err != nil {
			return fmt.Errorf("load recurring schedule ids: %w", err)
		}
		if !sameScheduleIDs(persistedIDs, currentIDs) {
			return redis.TxFailedErr
		}

		states := make(map[string]ScheduleState, len(configuredIDs))
		if len(configuredStateKeys) > 0 {
			values, err := tx.MGet(ctx, configuredStateKeys...).Result()
			if err != nil {
				return fmt.Errorf("load configured recurring schedule states: %w", err)
			}
			states, err = decodeScheduleStates(configuredIDs, values)
			if err != nil {
				return fmt.Errorf("load configured recurring schedule states: %w", err)
			}
		}

		removedIDs := make([]string, 0, len(persistedIDs))
		removedStateKeys := make([]string, 0, len(persistedIDs))
		for _, scheduleID := range persistedIDs {
			if _, exists := configuredSet[scheduleID]; exists {
				continue
			}
			removedIDs = append(removedIDs, scheduleID)
			removedStateKeys = append(removedStateKeys, s.stateKey(scheduleID))
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, schedule := range schedules {
				state, exists := states[schedule.ID]
				definitionHash := hashScheduleDefinition(schedule)
				if !exists || state.DefinitionHash != definitionHash || state.NextRunAt.IsZero() {
					state = initialScheduleState(schedule, now, definitionHash)
				}
				state.DefinitionHash = definitionHash
				state.MisfirePolicy = schedule.MisfirePolicy

				payload, err := json.Marshal(state)
				if err != nil {
					return fmt.Errorf("marshal recurring schedule state %s: %w", schedule.ID, err)
				}

				pipe.Set(ctx, s.stateKey(schedule.ID), payload, 0)
				pipe.SAdd(ctx, s.scheduleIDsKey(), schedule.ID)
				if schedule.Enabled {
					pipe.ZAdd(ctx, s.dueIndexKey(), redis.Z{
						Score:  float64(state.NextRunAt.UTC().UnixMilli()),
						Member: schedule.ID,
					})
					continue
				}
				pipe.ZRem(ctx, s.dueIndexKey(), schedule.ID)
			}
			if len(removedStateKeys) > 0 {
				pipe.Del(ctx, removedStateKeys...)
			}
			if len(removedIDs) > 0 {
				members := make([]interface{}, 0, len(removedIDs))
				for _, scheduleID := range removedIDs {
					members = append(members, scheduleID)
				}
				pipe.ZRem(ctx, s.dueIndexKey(), members...)
				pipe.SRem(ctx, s.scheduleIDsKey(), members...)
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, redis.TxFailedErr) {
				return err
			}
			return fmt.Errorf("reconcile recurring schedule state: %w", err)
		}
		return nil
	})
	return err
}

func sameScheduleIDs(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, id := range left {
		seen[id] = struct{}{}
	}
	for _, id := range right {
		if _, ok := seen[id]; !ok {
			return false
		}
	}
	return true
}

func (s *RedisScheduleStateStore) DueScheduleIDs(ctx context.Context, now time.Time, limit int64) ([]string, error) {
	ids, err := s.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     s.dueIndexKey(),
		Start:   "-inf",
		Stop:    strconv.FormatInt(now.UTC().UnixMilli(), 10),
		ByScore: true,
		Offset:  0,
		Count:   limit,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("query recurring due index: %w", err)
	}
	return ids, nil
}

func (s *RedisScheduleStateStore) LoadStates(ctx context.Context, scheduleIDs []string) (map[string]ScheduleState, error) {
	if len(scheduleIDs) == 0 {
		return map[string]ScheduleState{}, nil
	}

	keys := make([]string, 0, len(scheduleIDs))
	for _, scheduleID := range scheduleIDs {
		keys = append(keys, s.stateKey(scheduleID))
	}

	values, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("load recurring schedule states: %w", err)
	}

	states, err := decodeScheduleStates(scheduleIDs, values)
	if err != nil {
		return nil, fmt.Errorf("load recurring schedule states: %w", err)
	}
	return states, nil
}

func (s *RedisScheduleStateStore) SaveIndexed(ctx context.Context, fence taskforge.LeadershipFence, scheduleID string, state ScheduleState) error {
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal schedule state: %w", err)
	}

	return s.execWithFence(ctx, fence, "save_indexed", staticWatchKeys(s.leadershipKey()), func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, s.stateKey(scheduleID), payload, 0)
			pipe.SAdd(ctx, s.scheduleIDsKey(), scheduleID)
			pipe.ZAdd(ctx, s.dueIndexKey(), redis.Z{
				Score:  float64(state.NextRunAt.UTC().UnixMilli()),
				Member: scheduleID,
			})
			return nil
		})
		if err != nil {
			return fmt.Errorf("save schedule state: %w", err)
		}
		return nil
	})
}

func (s *RedisScheduleStateStore) AdvanceIfUnchanged(ctx context.Context, fence taskforge.LeadershipFence, scheduleID string, expected ScheduleState, next ScheduleState) (bool, error) {
	stateKey := s.stateKey(scheduleID)
	dueIndexKey := s.dueIndexKey()
	expectedNextRunAt := expected.NextRunAt.UTC()
	expectedDefinitionHash := expected.DefinitionHash

	advanced := false
	err := s.execWithFence(ctx, fence, "advance_if_unchanged", staticWatchKeys(s.leadershipKey(), stateKey), func(tx *redis.Tx) error {
		advanced = false
		payload, err := tx.Get(ctx, stateKey).Result()
		if err != nil {
			if err == redis.Nil {
				return nil
			}
			return fmt.Errorf("load schedule state: %w", err)
		}

		current, _, err := decodeScheduleState(payload)
		if err != nil {
			return fmt.Errorf("load schedule state: %w", err)
		}
		if !current.NextRunAt.UTC().Equal(expectedNextRunAt) || current.DefinitionHash != expectedDefinitionHash {
			return nil
		}

		nextPayload, err := json.Marshal(next)
		if err != nil {
			return fmt.Errorf("marshal schedule state: %w", err)
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, stateKey, nextPayload, 0)
			pipe.SAdd(ctx, s.scheduleIDsKey(), scheduleID)
			pipe.ZAdd(ctx, dueIndexKey, redis.Z{
				Score:  float64(next.NextRunAt.UTC().UnixMilli()),
				Member: scheduleID,
			})
			return nil
		})
		if err == nil {
			advanced = true
		}
		return err
	})
	if err != nil {
		return false, fmt.Errorf("advance schedule state: %w", err)
	}
	return advanced, nil
}

func (s *RedisScheduleStateStore) RemoveSchedule(ctx context.Context, fence taskforge.LeadershipFence, scheduleID string) error {
	return s.execWithFence(ctx, fence, "remove_schedule", staticWatchKeys(s.leadershipKey()), func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, s.stateKey(scheduleID))
			pipe.ZRem(ctx, s.dueIndexKey(), scheduleID)
			pipe.SRem(ctx, s.scheduleIDsKey(), scheduleID)
			return nil
		})
		if err != nil {
			return fmt.Errorf("remove recurring schedule: %w", err)
		}
		return nil
	})
}

func (s *RedisScheduleStateStore) RemoveFromDueIndex(ctx context.Context, fence taskforge.LeadershipFence, scheduleID string) error {
	return s.execWithFence(ctx, fence, "remove_due_index", staticWatchKeys(s.leadershipKey()), func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZRem(ctx, s.dueIndexKey(), scheduleID)
			return nil
		})
		if err != nil {
			return fmt.Errorf("remove recurring schedule from due index: %w", err)
		}
		return nil
	})
}

func (s *RedisScheduleStateStore) stateKey(scheduleID string) string {
	return fmt.Sprintf("%s:schedule:state:%s", s.prefix, scheduleID)
}

func (s *RedisScheduleStateStore) dueIndexKey() string {
	return fmt.Sprintf("%s:scheduler:recurring:due", s.prefix)
}

func (s *RedisScheduleStateStore) scheduleIDsKey() string {
	return fmt.Sprintf("%s:scheduler:recurring:ids", s.prefix)
}

func (s *RedisScheduleStateStore) leadershipKey() string {
	return fmt.Sprintf("%s:scheduler:leader", s.prefix)
}

func staticWatchKeys(keys ...string) func() ([]string, error) {
	return func() ([]string, error) {
		return keys, nil
	}
}

func (s *RedisScheduleStateStore) execWithFence(
	ctx context.Context,
	fence taskforge.LeadershipFence,
	operation string,
	watchKeys func() ([]string, error),
	fn func(tx *redis.Tx) error,
) error {
	for {
		keys, err := watchKeys()
		if err != nil {
			return err
		}
		err = s.client.Watch(ctx, func(tx *redis.Tx) error {
			if err := s.validateFence(ctx, tx, fence, operation); err != nil {
				return err
			}
			return fn(tx)
		}, keys...)
		if err == nil {
			return nil
		}
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return err
	}
}

func decodeScheduleState(value interface{}) (ScheduleState, bool, error) {
	if value == nil {
		return ScheduleState{}, false, nil
	}

	payload, ok := value.(string)
	if !ok {
		return ScheduleState{}, false, fmt.Errorf("unexpected type %T", value)
	}

	var state ScheduleState
	if err := json.Unmarshal([]byte(payload), &state); err != nil {
		return ScheduleState{}, false, fmt.Errorf("unmarshal: %w", err)
	}
	return state, true, nil
}

func decodeScheduleStates(scheduleIDs []string, values []interface{}) (map[string]ScheduleState, error) {
	states := make(map[string]ScheduleState, len(scheduleIDs))
	for index, value := range values {
		if index >= len(scheduleIDs) {
			return nil, fmt.Errorf("received %d schedule states for %d schedule ids", len(values), len(scheduleIDs))
		}
		state, exists, err := decodeScheduleState(value)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", scheduleIDs[index], err)
		}
		if exists {
			states[scheduleIDs[index]] = state
		}
	}
	return states, nil
}

func (s *RedisScheduleStateStore) validateFence(ctx context.Context, tx *redis.Tx, fence taskforge.LeadershipFence, operation string) error {
	if !fence.Valid() {
		return taskforge.NewStaleLeadershipError(operation)
	}
	value, err := tx.Get(ctx, s.leadershipKey()).Result()
	if err != nil {
		if err == redis.Nil {
			return taskforge.NewStaleLeadershipError(operation)
		}
		return fmt.Errorf("load scheduler leadership: %w", err)
	}
	if value != fence.Token {
		return taskforge.NewStaleLeadershipError(operation)
	}
	return nil
}
