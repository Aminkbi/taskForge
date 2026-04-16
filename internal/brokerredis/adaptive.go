package brokerredis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/observability"
)

const adaptiveSnapshotTTL = 2 * time.Minute

type AdaptiveStateStore struct {
	client *redis.Client
	prefix string
}

func NewAdaptiveStateStore(client *redis.Client, prefix string) *AdaptiveStateStore {
	if client == nil {
		return nil
	}
	return &AdaptiveStateStore{
		client: client,
		prefix: prefix,
	}
}

func (s *AdaptiveStateStore) StoreAdaptiveStatus(ctx context.Context, snapshot observability.AdaptivePoolSnapshot) error {
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

func (s *AdaptiveStateStore) AdaptiveStatusSnapshot(ctx context.Context, pool string) (observability.AdaptivePoolSnapshot, error) {
	if s == nil {
		return observability.AdaptivePoolSnapshot{}, nil
	}
	value, err := s.client.Get(ctx, s.snapshotKey(pool)).Result()
	if err != nil {
		if err == redis.Nil {
			return observability.AdaptivePoolSnapshot{
				Pool: strings.TrimSpace(pool),
			}, nil
		}
		return observability.AdaptivePoolSnapshot{}, fmt.Errorf("load adaptive status for pool %q: %w", pool, err)
	}

	var snapshot observability.AdaptivePoolSnapshot
	if err := json.Unmarshal([]byte(value), &snapshot); err != nil {
		return observability.AdaptivePoolSnapshot{}, fmt.Errorf("decode adaptive status for pool %q: %w", pool, err)
	}
	return snapshot, nil
}

func (s *AdaptiveStateStore) snapshotKey(pool string) string {
	return fmt.Sprintf("%s:adaptive:%s", s.prefix, strings.TrimSpace(pool))
}
