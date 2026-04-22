package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

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

func (s *RedisScheduleStateStore) ReconcileConfigured(ctx context.Context, fence LeadershipFence, schedules []ScheduleDefinition, now time.Time) error {
	persistedIDs, err := s.client.SMembers(ctx, s.scheduleIDsKey()).Result()
	if err != nil {
		return fmt.Errorf("load recurring schedule ids: %w", err)
	}

	configuredIDs := make([]string, 0, len(schedules))
	configuredSet := make(map[string]struct{}, len(schedules))
	for _, schedule := range schedules {
		configuredIDs = append(configuredIDs, schedule.ID)
		configuredSet[schedule.ID] = struct{}{}
	}

	states, err := s.LoadStates(ctx, configuredIDs)
	if err != nil {
		return fmt.Errorf("load configured recurring schedule states: %w", err)
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

	return s.execWithFence(ctx, fence, "reconcile_configured", func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
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
			return fmt.Errorf("reconcile recurring schedule state: %w", err)
		}
		return nil
	})
}

func (s *RedisScheduleStateStore) DueScheduleIDs(ctx context.Context, now time.Time, limit int64) ([]string, error) {
	ids, err := s.client.ZRangeByScore(ctx, s.dueIndexKey(), &redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(now.UTC().UnixMilli(), 10),
		Offset: 0,
		Count:  limit,
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

	states := make(map[string]ScheduleState, len(scheduleIDs))
	for idx, value := range values {
		if value == nil {
			continue
		}

		payload, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("load recurring schedule state %s: unexpected type %T", scheduleIDs[idx], value)
		}

		var state ScheduleState
		if err := json.Unmarshal([]byte(payload), &state); err != nil {
			return nil, fmt.Errorf("unmarshal recurring schedule state %s: %w", scheduleIDs[idx], err)
		}
		states[scheduleIDs[idx]] = state
	}

	return states, nil
}

func (s *RedisScheduleStateStore) SaveIndexed(ctx context.Context, fence LeadershipFence, scheduleID string, state ScheduleState) error {
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal schedule state: %w", err)
	}

	return s.execWithFence(ctx, fence, "save_indexed", func(tx *redis.Tx) error {
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

func (s *RedisScheduleStateStore) AdvanceIfUnchanged(ctx context.Context, fence LeadershipFence, scheduleID string, expected ScheduleState, next ScheduleState) (bool, error) {
	stateKey := s.stateKey(scheduleID)
	dueIndexKey := s.dueIndexKey()
	expectedNextRunAt := expected.NextRunAt.UTC()
	expectedDefinitionHash := expected.DefinitionHash

	for {
		advanced := false
		err := s.client.Watch(ctx, func(tx *redis.Tx) error {
			if err := s.validateFence(ctx, tx, fence, "advance_if_unchanged"); err != nil {
				return err
			}
			payload, err := tx.Get(ctx, stateKey).Result()
			if err != nil {
				if err == redis.Nil {
					return nil
				}
				return fmt.Errorf("load schedule state: %w", err)
			}

			var current ScheduleState
			if err := json.Unmarshal([]byte(payload), &current); err != nil {
				return fmt.Errorf("unmarshal schedule state: %w", err)
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
		}, s.leadershipKey(), stateKey)
		if err == nil {
			return advanced, nil
		}
		if err == redis.TxFailedErr {
			continue
		}
		return false, fmt.Errorf("advance schedule state: %w", err)
	}
}

func (s *RedisScheduleStateStore) RemoveSchedule(ctx context.Context, fence LeadershipFence, scheduleID string) error {
	return s.execWithFence(ctx, fence, "remove_schedule", func(tx *redis.Tx) error {
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

func (s *RedisScheduleStateStore) RemoveFromDueIndex(ctx context.Context, fence LeadershipFence, scheduleID string) error {
	return s.execWithFence(ctx, fence, "remove_due_index", func(tx *redis.Tx) error {
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

func (s *RedisScheduleStateStore) execWithFence(ctx context.Context, fence LeadershipFence, operation string, fn func(tx *redis.Tx) error) error {
	for {
		err := s.client.Watch(ctx, func(tx *redis.Tx) error {
			if err := s.validateFence(ctx, tx, fence, operation); err != nil {
				return err
			}
			return fn(tx)
		}, s.leadershipKey())
		if err == nil {
			return nil
		}
		if err == redis.TxFailedErr {
			continue
		}
		return err
	}
}

func (s *RedisScheduleStateStore) validateFence(ctx context.Context, tx *redis.Tx, fence LeadershipFence, operation string) error {
	if !fence.Valid() {
		return NewStaleLeadershipError(operation)
	}
	value, err := tx.Get(ctx, s.leadershipKey()).Result()
	if err != nil {
		if err == redis.Nil {
			return NewStaleLeadershipError(operation)
		}
		return fmt.Errorf("load scheduler leadership: %w", err)
	}
	if value != fence.Token {
		return NewStaleLeadershipError(operation)
	}
	return nil
}
