package shared

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aminkbi/taskforge/internal/config"
)

func ApplyDefaultRedisDB(cfg *config.Config, fallback int) {
	if cfg == nil {
		return
	}
	if cfg.RedisDB == 0 && getenv("TASKFORGE_REDIS_DB") == "" {
		cfg.RedisDB = fallback
	}
}

func WaitFor(ctx context.Context, interval time.Duration, check func() (bool, error)) error {
	if interval <= 0 {
		interval = 25 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		ok, err := check()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for condition: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func getenv(key string) string {
	return os.Getenv(key)
}
