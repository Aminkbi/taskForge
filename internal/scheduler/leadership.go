package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/clock"
)

const (
	defaultSchedulerPrefix = "taskforge"
)

var (
	ErrLeadershipLost = errors.New("scheduler: leadership lost")

	acquireLeadershipScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if current then
  return {0, "", current}
end
local epoch = redis.call("INCR", KEYS[2])
local token = ARGV[1] .. "|" .. epoch
redis.call("PSETEX", KEYS[1], ARGV[2], token)
return {1, tostring(epoch), token}
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

type LeadershipFence struct {
	Owner string
	Epoch int64
	Token string
}

func (f LeadershipFence) Valid() bool {
	return f.Token != "" && f.Owner != "" && f.Epoch > 0
}

type LeadershipSnapshot struct {
	Leader         bool
	Owner          string
	Epoch          int64
	Fence          LeadershipFence
	AcquiredAt     time.Time
	LastRenewedAt  time.Time
	LastLostAt     time.Time
	LastLossReason string
}

type LeadershipRecord struct {
	Present      bool
	Owner        string
	Epoch        int64
	Token        string
	TTLRemaining time.Duration
	ObservedAt   time.Time
}

type StaleLeadershipError struct {
	Operation string
}

func (e *StaleLeadershipError) Error() string {
	if e == nil || e.Operation == "" {
		return ErrLeadershipLost.Error()
	}
	return fmt.Sprintf("%s during %s", ErrLeadershipLost, e.Operation)
}

func (e *StaleLeadershipError) Unwrap() error {
	return ErrLeadershipLost
}

func NewStaleLeadershipError(operation string) error {
	return &StaleLeadershipError{Operation: operation}
}

type RedisLeaderElector struct {
	client        *redis.Client
	clock         clock.Clock
	logger        *slog.Logger
	owner         string
	ttl           time.Duration
	renewInterval time.Duration
	prefix        string
	mu            sync.RWMutex
	snapshot      LeadershipSnapshot
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
		snapshot: LeadershipSnapshot{
			Owner: owner,
		},
	}
}

func (e *RedisLeaderElector) Ensure(ctx context.Context) (LeadershipSnapshot, error) {
	if e.client == nil {
		return LeadershipSnapshot{}, fmt.Errorf("scheduler leadership: missing redis client")
	}

	now := e.clock.Now().UTC()
	snapshot := e.Snapshot()
	if snapshot.Leader && now.Sub(snapshot.LastRenewedAt) < e.renewInterval {
		return snapshot, nil
	}

	if snapshot.Leader {
		renewed, err := e.renew(ctx, now)
		if err != nil {
			e.demote(now, "renew_error")
			return e.Snapshot(), err
		}
		if renewed {
			return e.Snapshot(), nil
		}
		return e.Snapshot(), nil
	}

	if err := e.acquire(ctx, now); err != nil {
		return e.Snapshot(), err
	}
	return e.Snapshot(), nil
}

func (e *RedisLeaderElector) Release(ctx context.Context) error {
	snapshot := e.Snapshot()
	if !snapshot.Leader || !snapshot.Fence.Valid() {
		return nil
	}

	released, err := releaseLeadershipScript.Run(
		ctx,
		e.client,
		[]string{e.lockKey()},
		snapshot.Fence.Token,
	).Int64()
	if err != nil {
		return fmt.Errorf("release scheduler leadership: %w", err)
	}

	if released == 1 && e.logger != nil {
		e.logger.Info("scheduler leadership released", "owner", snapshot.Owner, "epoch", snapshot.Epoch)
	}

	e.demote(e.clock.Now().UTC(), "released")
	return nil
}

func (e *RedisLeaderElector) Demote(reason string) {
	e.demote(e.clock.Now().UTC(), reason)
}

func (e *RedisLeaderElector) Observe(ctx context.Context) (LeadershipRecord, error) {
	now := e.clock.Now().UTC()
	token, err := e.client.Get(ctx, e.lockKey()).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return LeadershipRecord{ObservedAt: now}, nil
		}
		return LeadershipRecord{}, fmt.Errorf("observe scheduler leadership: %w", err)
	}
	ttl, err := e.client.PTTL(ctx, e.lockKey()).Result()
	if err != nil {
		return LeadershipRecord{}, fmt.Errorf("observe scheduler leadership ttl: %w", err)
	}
	fence, err := parseLeadershipFence(token)
	if err != nil {
		return LeadershipRecord{}, fmt.Errorf("observe scheduler leadership token: %w", err)
	}
	return LeadershipRecord{
		Present:      true,
		Owner:        fence.Owner,
		Epoch:        fence.Epoch,
		Token:        token,
		TTLRemaining: ttl,
		ObservedAt:   now,
	}, nil
}

func (e *RedisLeaderElector) Snapshot() LeadershipSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.snapshot
}

func (e *RedisLeaderElector) acquire(ctx context.Context, now time.Time) error {
	values, err := acquireLeadershipScript.Run(
		ctx,
		e.client,
		[]string{e.lockKey(), e.epochKey()},
		e.owner,
		e.ttl.Milliseconds(),
	).Result()
	if err != nil {
		return fmt.Errorf("acquire scheduler leadership: %w", err)
	}

	result, ok := values.([]interface{})
	if !ok || len(result) != 3 {
		return fmt.Errorf("acquire scheduler leadership: unexpected script response %T", values)
	}

	acquired, err := redisInt64(result[0])
	if err != nil {
		return fmt.Errorf("acquire scheduler leadership: parse acquired flag: %w", err)
	}
	if acquired == 0 {
		return nil
	}

	epoch, err := redisInt64(result[1])
	if err != nil {
		return fmt.Errorf("acquire scheduler leadership: parse epoch: %w", err)
	}
	token, err := redisString(result[2])
	if err != nil {
		return fmt.Errorf("acquire scheduler leadership: parse token: %w", err)
	}

	e.mu.Lock()
	e.snapshot = LeadershipSnapshot{
		Leader: true,
		Owner:  e.owner,
		Epoch:  epoch,
		Fence: LeadershipFence{
			Owner: e.owner,
			Epoch: epoch,
			Token: token,
		},
		AcquiredAt:     now,
		LastRenewedAt:  now,
		LastLostAt:     e.snapshot.LastLostAt,
		LastLossReason: e.snapshot.LastLossReason,
	}
	e.mu.Unlock()

	if e.logger != nil {
		e.logger.Info("scheduler leadership acquired", "owner", e.owner, "epoch", epoch)
	}
	return nil
}

func (e *RedisLeaderElector) renew(ctx context.Context, now time.Time) (bool, error) {
	snapshot := e.Snapshot()
	renewed, err := renewLeadershipScript.Run(
		ctx,
		e.client,
		[]string{e.lockKey()},
		snapshot.Fence.Token,
		e.ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("renew scheduler leadership: %w", err)
	}

	if renewed == 0 {
		if e.logger != nil {
			e.logger.Warn("scheduler leadership lost", "owner", snapshot.Owner, "epoch", snapshot.Epoch, "reason", "token_mismatch")
		}
		e.demote(now, "token_mismatch")
		return false, nil
	}

	e.mu.Lock()
	current := e.snapshot
	current.LastRenewedAt = now
	e.snapshot = current
	e.mu.Unlock()
	return true, nil
}

func (e *RedisLeaderElector) demote(now time.Time, reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.snapshot.Leader && e.snapshot.LastLossReason == reason {
		return
	}
	e.snapshot.Leader = false
	e.snapshot.Fence = LeadershipFence{}
	e.snapshot.LastRenewedAt = time.Time{}
	e.snapshot.AcquiredAt = time.Time{}
	e.snapshot.LastLostAt = now
	e.snapshot.LastLossReason = reason
}

func (e *RedisLeaderElector) lockKey() string {
	return fmt.Sprintf("%s:scheduler:leader", e.prefix)
}

func (e *RedisLeaderElector) epochKey() string {
	return fmt.Sprintf("%s:scheduler:leader:epoch", e.prefix)
}

func parseLeadershipFence(token string) (LeadershipFence, error) {
	idx := strings.LastIndex(token, "|")
	if idx <= 0 || idx == len(token)-1 {
		return LeadershipFence{}, fmt.Errorf("invalid leadership token")
	}
	epoch, err := strconv.ParseInt(token[idx+1:], 10, 64)
	if err != nil {
		return LeadershipFence{}, fmt.Errorf("parse leadership epoch: %w", err)
	}
	return LeadershipFence{
		Owner: token[:idx],
		Epoch: epoch,
		Token: token,
	}, nil
}

func redisInt64(v interface{}) (int64, error) {
	switch value := v.(type) {
	case int64:
		return value, nil
	case string:
		return strconv.ParseInt(value, 10, 64)
	case []byte:
		return strconv.ParseInt(string(value), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected type %T", v)
	}
}

func redisString(v interface{}) (string, error) {
	switch value := v.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	default:
		return "", fmt.Errorf("unexpected type %T", v)
	}
}
