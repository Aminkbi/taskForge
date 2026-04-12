package storeredis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/tasks"
)

type RedisStore struct {
	client *redis.Client
}

func New(client *redis.Client) *RedisStore {
	return &RedisStore{client: client}
}

func (s *RedisStore) Save(ctx context.Context, taskID string, state tasks.State, payload []byte) error {
	key := fmt.Sprintf("taskforge:result:%s", taskID)
	return s.client.HSet(ctx, key,
		"state", string(state),
		"payload", payload,
	).Err()
}
