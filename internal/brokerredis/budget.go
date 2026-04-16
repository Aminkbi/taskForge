package brokerredis

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/observability"
)

var (
	budgetAcquireScript = redis.NewScript(`
local usedKey = KEYS[1]
local leasesKey = KEYS[2]
local expiriesKey = KEYS[3]
local now = tonumber(ARGV[1])
local deliveryID = ARGV[2]
local tokens = tonumber(ARGV[3])
local capacity = tonumber(ARGV[4])
local expiresAt = tonumber(ARGV[5])

local expired = redis.call("ZRANGEBYSCORE", expiriesKey, "-inf", now)
for _, leaseID in ipairs(expired) do
  local leaseTokens = tonumber(redis.call("HGET", leasesKey, leaseID) or "0")
  if leaseTokens > 0 then
    redis.call("HDEL", leasesKey, leaseID)
    redis.call("ZREM", expiriesKey, leaseID)
    redis.call("DECRBY", usedKey, leaseTokens)
  else
    redis.call("ZREM", expiriesKey, leaseID)
  end
end

local current = tonumber(redis.call("GET", usedKey) or "0")
local existing = tonumber(redis.call("HGET", leasesKey, deliveryID) or "0")
if existing > 0 then
  redis.call("ZADD", expiriesKey, expiresAt, deliveryID)
  return {1, current}
end
if current + tokens > capacity then
  return {0, current}
end
redis.call("SET", usedKey, current + tokens)
redis.call("HSET", leasesKey, deliveryID, tokens)
redis.call("ZADD", expiriesKey, expiresAt, deliveryID)
return {1, current + tokens}
`)
	budgetRenewScript = redis.NewScript(`
local usedKey = KEYS[1]
local leasesKey = KEYS[2]
local expiriesKey = KEYS[3]
local now = tonumber(ARGV[1])
local deliveryID = ARGV[2]
local expiresAt = tonumber(ARGV[3])

local expired = redis.call("ZRANGEBYSCORE", expiriesKey, "-inf", now)
for _, leaseID in ipairs(expired) do
  local leaseTokens = tonumber(redis.call("HGET", leasesKey, leaseID) or "0")
  if leaseTokens > 0 then
    redis.call("HDEL", leasesKey, leaseID)
    redis.call("ZREM", expiriesKey, leaseID)
    redis.call("DECRBY", usedKey, leaseTokens)
  else
    redis.call("ZREM", expiriesKey, leaseID)
  end
end

local existing = tonumber(redis.call("HGET", leasesKey, deliveryID) or "0")
if existing == 0 then
  return 0
end
redis.call("ZADD", expiriesKey, expiresAt, deliveryID)
return 1
`)
	budgetReleaseScript = redis.NewScript(`
local usedKey = KEYS[1]
local leasesKey = KEYS[2]
local expiriesKey = KEYS[3]
local deliveryID = ARGV[1]

local leaseTokens = tonumber(redis.call("HGET", leasesKey, deliveryID) or "0")
if leaseTokens == 0 then
  redis.call("ZREM", expiriesKey, deliveryID)
  return 0
end
redis.call("HDEL", leasesKey, deliveryID)
redis.call("ZREM", expiriesKey, deliveryID)
redis.call("DECRBY", usedKey, leaseTokens)
return leaseTokens
`)
	budgetUsageScript = redis.NewScript(`
local usedKey = KEYS[1]
local leasesKey = KEYS[2]
local expiriesKey = KEYS[3]
local now = tonumber(ARGV[1])

local expired = redis.call("ZRANGEBYSCORE", expiriesKey, "-inf", now)
for _, leaseID in ipairs(expired) do
  local leaseTokens = tonumber(redis.call("HGET", leasesKey, leaseID) or "0")
  if leaseTokens > 0 then
    redis.call("HDEL", leasesKey, leaseID)
    redis.call("ZREM", expiriesKey, leaseID)
    redis.call("DECRBY", usedKey, leaseTokens)
  else
    redis.call("ZREM", expiriesKey, leaseID)
  end
end

local current = tonumber(redis.call("GET", usedKey) or "0")
if current < 0 then
  current = 0
  redis.call("SET", usedKey, 0)
end
return current
`)
)

type BudgetStore struct {
	client     *redis.Client
	metrics    *observability.Metrics
	prefix     string
	capacities map[string]int
}

func NewBudgetStore(client *redis.Client, metrics *observability.Metrics, prefix string, capacities map[string]int) *BudgetStore {
	if client == nil || len(capacities) == 0 {
		return nil
	}
	return &BudgetStore{
		client:     client,
		metrics:    metrics,
		prefix:     prefix,
		capacities: maps.Clone(capacities),
	}
}

func (s *BudgetStore) AcquireLease(ctx context.Context, budget, deliveryID string, tokens int, ttl time.Duration) (bool, error) {
	if s == nil {
		return true, nil
	}
	budget = strings.TrimSpace(budget)
	capacity, ok := s.capacities[budget]
	if !ok {
		return false, fmt.Errorf("unknown dependency budget %q", budget)
	}
	if tokens < 1 {
		return false, fmt.Errorf("dependency budget %q tokens must be >= 1", budget)
	}

	now := time.Now().UTC()
	keys := s.budgetKeys(budget)
	result, err := budgetAcquireScript.Run(ctx, s.client, []string{keys.used, keys.leases, keys.expiries},
		now.UnixMilli(),
		deliveryID,
		tokens,
		capacity,
		now.Add(ttl).UnixMilli(),
	).Result()
	if err != nil {
		return false, fmt.Errorf("acquire dependency budget %q: %w", budget, err)
	}
	values, ok := result.([]any)
	if !ok || len(values) != 2 {
		return false, fmt.Errorf("acquire dependency budget %q: unexpected result %T", budget, result)
	}
	acquired, ok := int64Value(values[0])
	if !ok {
		return false, fmt.Errorf("acquire dependency budget %q: parse result %T", budget, values[0])
	}
	return acquired == 1, nil
}

func (s *BudgetStore) RenewLease(ctx context.Context, budget, deliveryID string, ttl time.Duration) error {
	if s == nil {
		return nil
	}
	now := time.Now().UTC()
	keys := s.budgetKeys(budget)
	result, err := budgetRenewScript.Run(ctx, s.client, []string{keys.used, keys.leases, keys.expiries},
		now.UnixMilli(),
		deliveryID,
		now.Add(ttl).UnixMilli(),
	).Int64()
	if err != nil {
		return fmt.Errorf("renew dependency budget %q: %w", budget, err)
	}
	if result == 0 {
		return redis.Nil
	}
	return nil
}

func (s *BudgetStore) ReleaseLease(ctx context.Context, budget, deliveryID string) error {
	if s == nil {
		return nil
	}
	keys := s.budgetKeys(budget)
	if _, err := budgetReleaseScript.Run(ctx, s.client, []string{keys.used, keys.leases, keys.expiries}, deliveryID).Result(); err != nil {
		return fmt.Errorf("release dependency budget %q: %w", budget, err)
	}
	return nil
}

func (s *BudgetStore) DependencyBudgetUsageSnapshots(ctx context.Context) ([]observability.DependencyBudgetUsageSnapshot, error) {
	if s == nil || len(s.capacities) == 0 {
		return nil, nil
	}

	names := slices.Collect(maps.Keys(s.capacities))
	slices.Sort(names)
	snapshots := make([]observability.DependencyBudgetUsageSnapshot, 0, len(names))
	now := time.Now().UTC().UnixMilli()
	for _, budget := range names {
		keys := s.budgetKeys(budget)
		inUse, err := budgetUsageScript.Run(ctx, s.client, []string{keys.used, keys.leases, keys.expiries}, now).Int64()
		if err != nil {
			return nil, fmt.Errorf("dependency budget usage %q: %w", budget, err)
		}
		snapshots = append(snapshots, observability.DependencyBudgetUsageSnapshot{
			Budget:   budget,
			Capacity: float64(s.capacities[budget]),
			InUse:    float64(inUse),
		})
	}
	return snapshots, nil
}

type budgetKeys struct {
	used     string
	leases   string
	expiries string
}

func (s *BudgetStore) budgetKeys(budget string) budgetKeys {
	base := fmt.Sprintf("%s:budget:%s", s.prefix, budget)
	return budgetKeys{
		used:     base + ":used",
		leases:   base + ":leases",
		expiries: base + ":expiries",
	}
}

func int64Value(value any) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}
