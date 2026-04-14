package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/clock"
)

const (
	defaultSchedulerPrefix = "taskforge"
)

var (
	acquireLeadershipScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if current then
  return {0, 0}
end
local fence = redis.call("INCR", KEYS[2])
local value = ARGV[1] .. "|" .. tostring(fence)
redis.call("PSETEX", KEYS[1], ARGV[2], value)
return {1, fence}
`)
	renewLeadershipScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call("PEXPIRE", KEYS[1], ARGV[2])
return 1
`)
	releaseLeadershipScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call("DEL", KEYS[1])
return 1
`)
)

type RedisLeaderElector struct {
	client        *redis.Client
	clock         clock.Clock
	logger        *slog.Logger
	owner         string
	ttl           time.Duration
	renewInterval time.Duration
	prefix        string
	mu            sync.RWMutex
	lastRenewedAt time.Time
	fenceToken    int64
	leader        bool
}

type LeadershipSnapshot struct {
	Leader        bool
	Owner         string
	FenceToken    int64
	LastRenewedAt time.Time
}

func NewRedisLeaderElector(
	client *redis.Client,
	clk clock.Clock,
	logger *slog.Logger,
	owner string,
	ttl time.Duration,
	renewInterval time.Duration,
) *RedisLeaderElector {
	return &RedisLeaderElector{
		client:        client,
		clock:         clk,
		logger:        logger,
		owner:         owner,
		ttl:           ttl,
		renewInterval: renewInterval,
		prefix:        defaultSchedulerPrefix,
	}
}

func (e *RedisLeaderElector) Ensure(ctx context.Context) (bool, error) {
	if e.client == nil {
		return false, fmt.Errorf("scheduler leadership: missing redis client")
	}

	now := e.clock.Now().UTC()
	e.mu.RLock()
	if e.leader && now.Sub(e.lastRenewedAt) < e.renewInterval {
		e.mu.RUnlock()
		return true, nil
	}
	leader := e.leader
	e.mu.RUnlock()

	if leader {
		ok, err := e.renew(ctx, now)
		if err != nil {
			return false, err
		}
		return ok, nil
	}

	return e.acquire(ctx, now)
}

func (e *RedisLeaderElector) Release(ctx context.Context) error {
	e.mu.RLock()
	if !e.leader {
		e.mu.RUnlock()
		return nil
	}
	lockValue := e.lockValue()
	e.mu.RUnlock()

	released, err := releaseLeadershipScript.Run(
		ctx,
		e.client,
		[]string{e.lockKey()},
		lockValue,
	).Int64()
	if err != nil {
		return fmt.Errorf("release scheduler leadership: %w", err)
	}

	snapshot := e.Snapshot()
	if released == 1 && e.logger != nil {
		e.logger.Info("scheduler leadership released", "owner", snapshot.Owner, "fence_token", snapshot.FenceToken)
	}

	e.mu.Lock()
	e.leader = false
	e.fenceToken = 0
	e.lastRenewedAt = time.Time{}
	e.mu.Unlock()
	return nil
}

func (e *RedisLeaderElector) acquire(ctx context.Context, now time.Time) (bool, error) {
	values, err := acquireLeadershipScript.Run(
		ctx,
		e.client,
		[]string{e.lockKey(), e.fenceKey()},
		e.owner,
		e.ttl.Milliseconds(),
	).Slice()
	if err != nil {
		return false, fmt.Errorf("acquire scheduler leadership: %w", err)
	}

	if len(values) != 2 {
		return false, fmt.Errorf("acquire scheduler leadership: unexpected response size %d", len(values))
	}

	acquired, err := toInt64(values[0])
	if err != nil {
		return false, fmt.Errorf("acquire scheduler leadership: parse acquired flag: %w", err)
	}
	if acquired == 0 {
		return false, nil
	}

	fenceToken, err := toInt64(values[1])
	if err != nil {
		return false, fmt.Errorf("acquire scheduler leadership: parse fence token: %w", err)
	}

	e.mu.Lock()
	e.leader = true
	e.fenceToken = fenceToken
	e.lastRenewedAt = now
	e.mu.Unlock()
	snapshot := e.Snapshot()
	if e.logger != nil {
		e.logger.Info("scheduler leadership acquired", "owner", snapshot.Owner, "fence_token", snapshot.FenceToken)
	}
	return true, nil
}

func (e *RedisLeaderElector) renew(ctx context.Context, now time.Time) (bool, error) {
	renewed, err := renewLeadershipScript.Run(
		ctx,
		e.client,
		[]string{e.lockKey()},
		e.lockValue(),
		e.ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("renew scheduler leadership: %w", err)
	}

	if renewed == 0 {
		snapshot := e.Snapshot()
		if e.logger != nil {
			e.logger.Warn("scheduler leadership lost", "owner", snapshot.Owner, "fence_token", snapshot.FenceToken)
		}
		e.mu.Lock()
		e.leader = false
		e.fenceToken = 0
		e.lastRenewedAt = time.Time{}
		e.mu.Unlock()
		return false, nil
	}

	e.mu.Lock()
	e.lastRenewedAt = now
	e.mu.Unlock()
	return true, nil
}

func (e *RedisLeaderElector) lockKey() string {
	return fmt.Sprintf("%s:scheduler:leader", e.prefix)
}

func (e *RedisLeaderElector) fenceKey() string {
	return fmt.Sprintf("%s:scheduler:fence", e.prefix)
}

func (e *RedisLeaderElector) lockValue() string {
	return fmt.Sprintf("%s|%d", e.owner, e.fenceToken)
}

func (e *RedisLeaderElector) Snapshot() LeadershipSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return LeadershipSnapshot{
		Leader:        e.leader,
		Owner:         e.owner,
		FenceToken:    e.fenceToken,
		LastRenewedAt: e.lastRenewedAt,
	}
}

func toInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		var parsed int64
		_, err := fmt.Sscanf(typed, "%d", &parsed)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unexpected type %T", value)
	}
}
