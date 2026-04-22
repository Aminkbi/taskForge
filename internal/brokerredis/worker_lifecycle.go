package brokerredis

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/observability"
)

const workerLifecycleSnapshotTTL = 2 * time.Minute

type WorkerLifecycleStore struct {
	client *redis.Client
	prefix string
}

func NewWorkerLifecycleStore(client *redis.Client, prefix string) *WorkerLifecycleStore {
	if client == nil {
		return nil
	}
	return &WorkerLifecycleStore{
		client: client,
		prefix: prefix,
	}
}

func (s *WorkerLifecycleStore) StoreWorkerLifecycleSnapshot(ctx context.Context, snapshot observability.WorkerLifecycleSnapshot) error {
	if s == nil {
		return nil
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal worker lifecycle for worker %q: %w", snapshot.WorkerID, err)
	}
	if err := s.client.Set(ctx, s.snapshotKey(snapshot.WorkerID), payload, workerLifecycleSnapshotTTL).Err(); err != nil {
		return fmt.Errorf("store worker lifecycle for worker %q: %w", snapshot.WorkerID, err)
	}
	return nil
}

func (s *WorkerLifecycleStore) WorkerLifecycleSnapshots(ctx context.Context) ([]observability.WorkerLifecycleSnapshot, error) {
	if s == nil {
		return nil, nil
	}

	pattern := fmt.Sprintf("%s:worker:lifecycle:*", s.prefix)
	keys := make([]string, 0)
	var cursor uint64
	for {
		batch, next, err := s.client.Scan(ctx, cursor, pattern, 64).Result()
		if err != nil {
			return nil, fmt.Errorf("scan worker lifecycle snapshots: %w", err)
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}

	values, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("load worker lifecycle snapshots: %w", err)
	}

	snapshots := make([]observability.WorkerLifecycleSnapshot, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		raw, ok := value.(string)
		if !ok || strings.TrimSpace(raw) == "" {
			continue
		}
		var snapshot observability.WorkerLifecycleSnapshot
		if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
			return nil, fmt.Errorf("decode worker lifecycle snapshot: %w", err)
		}
		snapshots = append(snapshots, snapshot)
	}

	slices.SortFunc(snapshots, func(a, b observability.WorkerLifecycleSnapshot) int {
		if cmp := strings.Compare(a.Queue, b.Queue); cmp != 0 {
			return cmp
		}
		if cmp := strings.Compare(a.Pool, b.Pool); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.WorkerID, b.WorkerID)
	})

	return snapshots, nil
}

func (s *WorkerLifecycleStore) snapshotKey(workerID string) string {
	return fmt.Sprintf("%s:worker:lifecycle:%s", s.prefix, strings.TrimSpace(workerID))
}
