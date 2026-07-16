package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aminkbi/taskforge"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const adaptiveSnapshotTTL = 2 * time.Minute

type adaptiveStateStore struct {
	client *redis.Client
	prefix string
}

func newAdaptiveStateStore(client *redis.Client, prefix string) *adaptiveStateStore {
	if client == nil {
		return nil
	}
	return &adaptiveStateStore{
		client: client,
		prefix: prefix,
	}
}

func (s *adaptiveStateStore) StoreAdaptiveStatus(ctx context.Context, snapshot taskforge.AdaptivePoolSnapshot) error {
	if s == nil {
		return nil
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal adaptive status for pool %q: %w", snapshot.Pool, err)
	}
	if err := s.client.Set(ctx, s.snapshotKey(snapshot.Pool), payload, adaptiveSnapshotTTL).Err(); err != nil {
		return fmt.Errorf("store adaptive status for pool %q: %w", snapshot.Pool, err)
	}
	return nil
}

func (s *adaptiveStateStore) AdaptiveStatusSnapshot(ctx context.Context, pool string) (taskforge.AdaptivePoolSnapshot, error) {
	if s == nil {
		return taskforge.AdaptivePoolSnapshot{}, nil
	}
	value, err := s.client.Get(ctx, s.snapshotKey(pool)).Result()
	if err != nil {
		if err == redis.Nil {
			return taskforge.AdaptivePoolSnapshot{
				Pool: strings.TrimSpace(pool),
			}, nil
		}
		return taskforge.AdaptivePoolSnapshot{}, fmt.Errorf("load adaptive status for pool %q: %w", pool, err)
	}

	var snapshot taskforge.AdaptivePoolSnapshot
	if err := json.Unmarshal([]byte(value), &snapshot); err != nil {
		return taskforge.AdaptivePoolSnapshot{}, fmt.Errorf("decode adaptive status for pool %q: %w", pool, err)
	}
	return snapshot, nil
}

func (s *adaptiveStateStore) snapshotKey(pool string) string {
	return fmt.Sprintf("%s:adaptive:%s", s.prefix, strings.TrimSpace(pool))
}
