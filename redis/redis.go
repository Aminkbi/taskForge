package redis

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"uuid"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/logging"
	"github.com/aminkbi/taskforge/internal/observability"
)

const (
	defaultPrefix               = "taskforge:v2"
	defaultAddr                 = "localhost:6379"
	defaultLeaseTTL             = 30 * time.Second
	defaultReserveTimeout       = time.Second
	defaultPublishReceiptTTL    = 7 * 24 * time.Hour
	streamPayloadField          = "message"
	oldestReadyScanCount        = 64
	defaultReclaimBatchSize     = 128
	maxReclaimBatchSize         = 256
	maxReclaimIndexAuditEntries = 16384
	maxReclaimScanSteps         = 1024
)

// StateMode selects which task lifecycle records the broker persists.
type StateMode string

const (
	StateModeFull         StateMode = "full"
	StateModeDeliveryOnly StateMode = "delivery_only"
)

func normalizeStateMode(mode StateMode) (StateMode, error) {
	switch mode {
	case "", StateModeFull:
		return StateModeFull, nil
	case StateModeDeliveryOnly:
		return StateModeDeliveryOnly, nil
	default:
		return StateModeFull, fmt.Errorf("state mode: expected full or delivery_only, got %q", mode)
	}
}

var (
	publishReadyTaskScript = redis.NewScript(`
if tonumber(ARGV[3]) > 0 and redis.call("EXISTS", KEYS[3]) == 1 then
  return 0
end
if ARGV[4] ~= "" then
  redis.call("SADD", KEYS[4], ARGV[4])
end
redis.call("XADD", KEYS[1], "*", ARGV[1], ARGV[2])
if ARGV[4] ~= "" then
  redis.call("LPUSH", KEYS[5], ARGV[5])
  redis.call("LTRIM", KEYS[5], 0, 0)
end
local fieldCount = tonumber(ARGV[6])
if fieldCount > 0 then
  local fields = {}
  for index = 1, fieldCount * 2 do
    fields[index] = ARGV[6 + index]
  end
  redis.call("HSET", KEYS[2], unpack(fields))
  redis.call("PERSIST", KEYS[2])
end
if tonumber(ARGV[3]) > 0 then
  redis.call("PSETEX", KEYS[3], ARGV[3], "1")
end
return 1
`)
	publishDelayedScript = redis.NewScript(`
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
local head = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
if head[1] then
  redis.call("ZADD", KEYS[2], head[2], ARGV[3])
else
  redis.call("ZREM", KEYS[2], ARGV[3])
end
if ARGV[5] == "1" then
  redis.call("ZADD", KEYS[3], ARGV[1], ARGV[4])
end
local fieldCount = tonumber(ARGV[6]) or 0
if fieldCount > 0 then
  local fields = {}
  local position = 7
  for index = 1, fieldCount * 2 do
    fields[index] = ARGV[position]
    position = position + 1
  end
  redis.call("HSET", KEYS[4], unpack(fields))
  redis.call("PERSIST", KEYS[4])
end
return 1
`)
	publishDelayedWithReceiptScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
local head = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
if head[1] then
  redis.call("ZADD", KEYS[3], head[2], ARGV[4])
else
  redis.call("ZREM", KEYS[3], ARGV[4])
end
if ARGV[6] == "1" then
  redis.call("ZADD", KEYS[4], ARGV[1], ARGV[5])
end
redis.call("PSETEX", KEYS[2], ARGV[3], "1")
local fieldCount = tonumber(ARGV[7]) or 0
if fieldCount > 0 then
  local fields = {}
  local position = 8
  for index = 1, fieldCount * 2 do
    fields[index] = ARGV[position]
    position = position + 1
  end
  redis.call("HSET", KEYS[5], unpack(fields))
  redis.call("PERSIST", KEYS[5])
end
return 1
`)
	finalizeDeliveryScript = redis.NewScript(`
local pending = redis.call("XPENDING", KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if not pending[1] or pending[1][1] ~= ARGV[2] then
  return {0, "", 0}
end
local owner = pending[1][2]
local idle = tonumber(pending[1][3]) or 0
if ARGV[3] ~= "" and owner ~= ARGV[3] then
  return {2, owner, idle}
end
local ttl = tonumber(ARGV[4]) or 0
if ttl > 0 and idle >= ttl then
  return {3, owner, idle}
end
local acked = redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
local deleted = redis.call("XDEL", KEYS[1], ARGV[2])
if acked ~= 1 or deleted ~= 1 then
  return {0, owner, idle}
end
pcall(function() redis.call("ZREM", KEYS[2], ARGV[2]) end)
if KEYS[3] then
  local stateTTL = tonumber(ARGV[5]) or -1
  local fieldCount = tonumber(ARGV[6]) or 0
  local fields = {}
  local position = 7
  for index = 1, fieldCount * 2 do
    fields[index] = ARGV[position]
    position = position + 1
  end
  if #fields > 0 then
    redis.call("HSET", KEYS[3], unpack(fields))
  end
  if stateTTL > 0 then
    redis.call("PEXPIRE", KEYS[3], stateTTL)
  else
    redis.call("PERSIST", KEYS[3])
  end
end
return {1, owner, idle}
`)
	fencedRenewLeasesScript = redis.NewScript(`
local results = {}
local redisTime = redis.call("TIME")
local nowMillis = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)
for index = 2, #ARGV, 3 do
  local id = ARGV[index]
  local expectedOwner = ARGV[index + 1]
  local ttl = tonumber(ARGV[index + 2]) or 0
  local deadline = nowMillis + ttl
  local pending = redis.call("XPENDING", KEYS[1], ARGV[1], id, id, 1)
  if not pending[1] or pending[1][1] ~= id then
    results[#results + 1] = 0
  elseif expectedOwner ~= "" and pending[1][2] ~= expectedOwner then
    results[#results + 1] = 2
  elseif ttl > 0 and tonumber(pending[1][3]) >= ttl then
    results[#results + 1] = 3
  else
    local claimed = redis.call("XCLAIM", KEYS[1], ARGV[1], expectedOwner, 0, id, "JUSTID")
    if #claimed == 1 then
      pcall(function() redis.call("ZADD", KEYS[2], deadline, id) end)
      results[#results + 1] = 1
    else
      results[#results + 1] = 0
    end
  end
end
return results
`)
	fencedReleaseReadyScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return {-1, 0}
end
local existed = redis.call("EXISTS", KEYS[4])
if existed == 0 then
  redis.call("XADD", KEYS[3], "*", ARGV[3], ARGV[4])
  redis.call("PSETEX", KEYS[4], ARGV[5], "1")
end
local removed = redis.call("ZREM", KEYS[2], ARGV[2])
redis.call("ZREM", KEYS[6], ARGV[7])
local head = redis.call("ZRANGE", KEYS[2], 0, 0, "WITHSCORES")
if head[1] then
  redis.call("ZADD", KEYS[5], head[2], ARGV[6])
else
  redis.call("ZREM", KEYS[5], ARGV[6])
end
return {existed, removed}
`)
	fencedReleaseFairReadyScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return {-1, 0}
end
local existed = redis.call("EXISTS", KEYS[6])
if existed == 0 then
  redis.call("SADD", KEYS[3], ARGV[3])
  redis.call("XADD", KEYS[4], "*", ARGV[4], ARGV[5])
  redis.call("LPUSH", KEYS[5], ARGV[6])
  redis.call("LTRIM", KEYS[5], 0, 0)
  redis.call("PSETEX", KEYS[6], ARGV[7], "1")
end
local removed = redis.call("ZREM", KEYS[2], ARGV[2])
redis.call("ZREM", KEYS[8], ARGV[9])
local head = redis.call("ZRANGE", KEYS[2], 0, 0, "WITHSCORES")
if head[1] then
  redis.call("ZADD", KEYS[7], head[2], ARGV[8])
else
  redis.call("ZREM", KEYS[7], ARGV[8])
end
return {existed, removed}
`)
	fencedReleaseDelayedScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return {-1, 0}
end
local existed = redis.call("EXISTS", KEYS[4])
if existed == 0 then
  redis.call("ZADD", KEYS[3], ARGV[3], ARGV[4])
  if ARGV[10] == "1" then
    redis.call("ZADD", KEYS[7], ARGV[3], ARGV[9])
  end
  redis.call("PSETEX", KEYS[4], ARGV[5], "1")
end
local removed = redis.call("ZREM", KEYS[2], ARGV[2])
redis.call("ZREM", KEYS[6], ARGV[8])
local oldHead = redis.call("ZRANGE", KEYS[2], 0, 0, "WITHSCORES")
if oldHead[1] then
  redis.call("ZADD", KEYS[5], oldHead[2], ARGV[6])
else
  redis.call("ZREM", KEYS[5], ARGV[6])
end
local newHead = redis.call("ZRANGE", KEYS[3], 0, 0, "WITHSCORES")
if newHead[1] then
  redis.call("ZADD", KEYS[5], newHead[2], ARGV[7])
else
  redis.call("ZREM", KEYS[5], ARGV[7])
end
return {existed, removed}
`)
)

type Broker struct {
	client            *redis.Client
	ownedClient       bool
	logger            *slog.Logger
	metrics           *observability.Metrics
	leaseTTL          time.Duration
	reserveTTL        time.Duration
	prefix            string
	hostname          string
	instanceID        string
	fairnessPolicies  map[string]*FairnessPolicy
	admissionPolicies map[string]AdmissionPolicy
	routingPolicy     *RoutingPolicy
	admissionStateMu  sync.RWMutex
	admissionStates   map[string]taskforge.AdmissionStatusSnapshot
	budgetStore       *budgetStore
	adaptiveStore     *adaptiveStateStore
	workerStore       *workerLifecycleStore
	stateStore        taskforge.StateStore
	stateMode         StateMode
	configErr         error
	reclaimBatchSize  int64
	deadLetters       *deadLetterStore
	consumerGroupsMu  sync.RWMutex
	consumerGroups    map[string]struct{}
	reclaimMu         sync.Mutex
	reclaimNext       map[string]time.Time
	reclaimIntervals  map[string]time.Duration
	reclaimCursors    map[string]string
	reclaimScanning   map[string]bool
	reclaimScanSteps  map[string]int
	reclaimAuditNext  map[string]time.Time
}

type Options struct {
	Addr              string
	Password          string
	DB                int
	TLSConfig         *tls.Config
	Client            *redis.Client
	Logger            *slog.Logger
	LeaseTTL          time.Duration
	ReserveTimeout    time.Duration
	FairnessPolicies  map[string]*FairnessPolicy
	AdmissionPolicies map[string]AdmissionPolicy
	RoutingPolicy     *RoutingPolicy
	DependencyBudgets map[string]int
	StateStore        taskforge.StateStore
	StateMode         StateMode
	ReclaimBatchSize  int
	Retention         taskforge.RetentionPolicy
}

func New(options Options) *Broker {
	stateMode, configErr := normalizeStateMode(options.StateMode)
	return newBroker(options, stateMode, configErr)
}

func NewChecked(options Options) (*Broker, error) {
	stateMode, err := normalizeStateMode(options.StateMode)
	if err != nil {
		return nil, err
	}
	return newBroker(options, stateMode, nil), nil
}

func newBroker(options Options, stateMode StateMode, configErr error) *Broker {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	reserveTimeout := options.ReserveTimeout
	if reserveTimeout <= 0 {
		reserveTimeout = defaultReserveTimeout
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	leaseTTL := options.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = defaultLeaseTTL
	}
	client := options.Client
	ownedClient := false
	if client == nil {
		client = NewClient(options)
		ownedClient = true
	}
	metrics := observability.NewMetrics()
	state := options.StateStore
	if state == nil {
		state = newStateStore(client, options.Retention)
	}
	reclaimBatchSize := options.ReclaimBatchSize
	if reclaimBatchSize <= 0 {
		reclaimBatchSize = defaultReclaimBatchSize
	}
	reclaimBatchSize = min(reclaimBatchSize, maxReclaimBatchSize)

	b := &Broker{
		client:            client,
		ownedClient:       ownedClient,
		logger:            logger,
		metrics:           metrics,
		leaseTTL:          leaseTTL,
		reserveTTL:        reserveTimeout,
		prefix:            defaultPrefix,
		hostname:          hostname,
		instanceID:        fmt.Sprintf("%d", os.Getpid()),
		fairnessPolicies:  cloneFairnessPolicies(options.FairnessPolicies),
		admissionPolicies: cloneAdmissionPolicies(options.AdmissionPolicies),
		routingPolicy:     options.RoutingPolicy,
		admissionStates:   make(map[string]taskforge.AdmissionStatusSnapshot),
		budgetStore:       newBudgetStore(client, metrics, defaultPrefix, options.DependencyBudgets),
		adaptiveStore:     newAdaptiveStateStore(client, defaultPrefix),
		workerStore:       newWorkerLifecycleStore(client, defaultPrefix),
		stateStore:        state,
		stateMode:         stateMode,
		configErr:         configErr,
		reclaimBatchSize:  int64(reclaimBatchSize),
		consumerGroups:    make(map[string]struct{}),
		reclaimNext:       make(map[string]time.Time),
		reclaimIntervals:  make(map[string]time.Duration),
		reclaimCursors:    make(map[string]string),
		reclaimScanning:   make(map[string]bool),
		reclaimScanSteps:  make(map[string]int),
		reclaimAuditNext:  make(map[string]time.Time),
	}
	b.deadLetters = newDeadLetterStore(client, b, logger.With("component", "dlq"))
	return b
}

func (b *Broker) Close() error {
	if b == nil || !b.ownedClient {
		return nil
	}
	return b.client.Close()
}

func (b *Broker) checkConfig() error {
	if b == nil {
		return fmt.Errorf("redis broker is nil")
	}
	return b.configErr
}

func (b *Broker) StateWritesEnabled() bool {
	return b != nil && b.configErr == nil && b.stateMode != StateModeDeliveryOnly
}

func (b *Broker) MetricsHandler() http.Handler { return b.metrics.Handler() }

func (b *Broker) MetricsGatherer() prometheus.Gatherer { return b.metrics.Registry }

func (b *Broker) Get(ctx context.Context, taskID string) (taskforge.TaskRecord, error) {
	if err := b.checkConfig(); err != nil {
		return taskforge.TaskRecord{}, err
	}
	return b.stateStore.Get(ctx, taskID)
}

func (b *Broker) RecordQueued(ctx context.Context, task taskforge.Task) error {
	if err := b.checkConfig(); err != nil {
		return err
	}
	if b.stateMode == StateModeDeliveryOnly {
		return nil
	}
	return b.stateStore.RecordQueued(ctx, task)
}

func (b *Broker) RecordDelivery(ctx context.Context, delivery taskforge.Delivery, state taskforge.State, payload []byte) error {
	if err := b.checkConfig(); err != nil {
		return err
	}
	if b.stateMode == StateModeDeliveryOnly {
		return nil
	}
	return b.stateStore.RecordDelivery(ctx, delivery, state, payload)
}

func (b *Broker) RecordDeliveryBatch(ctx context.Context, deliveries []taskforge.Delivery, state taskforge.State) error {
	if err := b.checkConfig(); err != nil {
		return err
	}
	if b.stateMode == StateModeDeliveryOnly {
		return nil
	}
	if store, ok := b.stateStore.(interface {
		RecordDeliveryBatch(context.Context, []taskforge.Delivery, taskforge.State) error
	}); ok {
		return store.RecordDeliveryBatch(ctx, deliveries, state)
	}
	for _, delivery := range deliveries {
		if err := b.stateStore.RecordDelivery(ctx, delivery, state, nil); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) OwnsStateStore(store taskforge.StateStore) bool {
	if b == nil || b.configErr != nil {
		return false
	}
	_, builtIn := b.stateStore.(*stateStore)
	return builtIn && store == b
}

func (b *Broker) AckAndRecord(ctx context.Context, delivery taskforge.Delivery, state taskforge.State) error {
	if err := b.checkConfig(); err != nil {
		return err
	}
	if b.stateMode == StateModeDeliveryOnly {
		return b.Ack(ctx, delivery)
	}
	store, ok := b.stateStore.(*stateStore)
	if !ok {
		return fmt.Errorf("ack and record requires the broker Redis state store")
	}
	record, err := store.deliveryRecord(delivery, state)
	if err != nil {
		return err
	}
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.redis",
		"taskforge.ack",
		delivery.Message,
		deliverySpanAttributes(delivery)...,
	)
	defer span.End()
	pending, ttl, err := b.finalizeDelivery(ctx, delivery, &record)
	if err != nil {
		b.logDeliveryRejection("ack rejected", delivery, pending, ttl, err)
		observability.MarkSpanError(span, err)
	}
	return err
}

func (b *Broker) PublishDeadLetter(ctx context.Context, envelope taskforge.DeadLetterEnvelope) error {
	return b.deadLetters.PublishDeadLetter(ctx, envelope)
}

func (b *Broker) ListDeadLetters(ctx context.Context, queue string, limit int64) ([]taskforge.DeadLetterEntry, error) {
	return b.deadLetters.List(ctx, queue, limit)
}

func (b *Broker) ReplayDeadLetter(ctx context.Context, queue, entryID string) error {
	return b.deadLetters.Replay(ctx, queue, entryID)
}

func (b *Broker) ReplayDeadLetters(ctx context.Context, queue string, limit int64) (int, error) {
	return b.deadLetters.ReplayBatch(ctx, queue, limit)
}

func (b *Broker) DiscardDeadLetter(ctx context.Context, queue, entryID, reason string) error {
	return b.deadLetters.Discard(ctx, queue, entryID, reason)
}

func (b *Broker) AcquireLease(ctx context.Context, budget, deliveryID string, tokens int, ttl time.Duration) (bool, error) {
	return b.budgetStore.AcquireLease(ctx, budget, deliveryID, tokens, ttl)
}

func (b *Broker) RenewLease(ctx context.Context, budget, deliveryID string, ttl time.Duration) error {
	return b.budgetStore.RenewLease(ctx, budget, deliveryID, ttl)
}

func (b *Broker) ReleaseLease(ctx context.Context, budget, deliveryID string) error {
	return b.budgetStore.ReleaseLease(ctx, budget, deliveryID)
}

func (b *Broker) StoreAdaptiveStatus(ctx context.Context, snapshot taskforge.AdaptivePoolSnapshot) error {
	return b.adaptiveStore.StoreAdaptiveStatus(ctx, snapshot)
}

func (b *Broker) StoreWorkerLifecycleSnapshot(ctx context.Context, snapshot taskforge.WorkerLifecycleSnapshot) error {
	return b.workerStore.StoreWorkerLifecycleSnapshot(ctx, snapshot)
}

func (b *Broker) DependencyBudgetUsageSnapshots(ctx context.Context) ([]taskforge.DependencyBudgetUsageSnapshot, error) {
	if b.budgetStore == nil {
		return nil, nil
	}
	return b.budgetStore.DependencyBudgetUsageSnapshots(ctx)
}

func (b *Broker) AdaptiveStatusSnapshot(ctx context.Context, pool string) (taskforge.AdaptivePoolSnapshot, error) {
	if b.adaptiveStore == nil {
		return taskforge.AdaptivePoolSnapshot{Pool: pool}, nil
	}
	return b.adaptiveStore.AdaptiveStatusSnapshot(ctx, pool)
}

func (b *Broker) WorkerLifecycleSnapshots(ctx context.Context) ([]taskforge.WorkerLifecycleSnapshot, error) {
	if b.workerStore == nil {
		return nil, nil
	}
	return b.workerStore.WorkerLifecycleSnapshots(ctx)
}

func (b *Broker) Ping(ctx context.Context) error {
	return b.client.Ping(ctx).Err()
}

func (b *Broker) Publish(ctx context.Context, msg taskforge.Task, opts taskforge.PublishOptions) (taskforge.PublishResult, error) {
	if err := b.checkConfig(); err != nil {
		return taskforge.PublishResult{}, err
	}
	if msg.ID == "" {
		return taskforge.PublishResult{}, fmt.Errorf("publish task: missing id")
	}
	msg = msg.Clone()

	now := time.Now().UTC()
	msg = normalizePublishedMessage(msg, now)
	opts = opts.Normalize()
	msg, placement := b.routePublishedMessage(msg, opts)
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.redis",
		"taskforge.publish",
		msg,
		attribute.Bool("taskforge.delayed", msg.ETA != nil && msg.ETA.After(now)),
	)
	defer span.End()
	msg.Headers = observability.InjectTraceContext(ctx, msg.Headers)

	result, queuedStateRecorded, err := b.publishMessage(ctx, msg, opts, now)
	if err != nil {
		observability.MarkSpanError(span, err)
		return taskforge.PublishResult{}, err
	}
	if placement.Shard != "" {
		result.Shard = placement.Shard
	}
	if placement.Rule != "" {
		result.RoutingRule = placement.Rule
	}
	if b.stateMode != StateModeDeliveryOnly && b.stateStore != nil && !queuedStateRecorded && result.Decision != taskforge.AdmissionDecisionRejected && !result.Deduplicated {
		if err := b.stateStore.RecordQueued(ctx, msg); err != nil {
			observability.MarkSpanError(span, err)
			b.logger.Warn("record queued task state failed", "task_id", msg.ID, "error", err)
		}
	}
	return result, nil
}

func (b *Broker) routePublishedMessage(msg taskforge.Task, opts taskforge.PublishOptions) (taskforge.Task, RoutingPlacement) {
	placement := RoutingPlacement{Queue: taskforge.EffectiveQueue(msg)}
	if b.routingPolicy == nil || opts.Source != taskforge.PublishSourceNew {
		return msg, placement
	}
	return b.routingPolicy.Apply(msg)
}

func (b *Broker) Reserve(ctx context.Context, queue, consumerID string) (taskforge.Delivery, error) {
	if err := b.checkConfig(); err != nil {
		return taskforge.Delivery{}, err
	}
	started := time.Now()
	defer func(queue string) {
		b.metrics.ObserveReserveLatency(normalizeQueue(queue), time.Since(started).Seconds())
	}(queue)

	queue = normalizeQueue(queue)
	if b.fairnessPolicy(queue) != nil {
		return b.reserveFair(ctx, queue, consumerID)
	}
	deliveries, err := b.reserveFIFO(ctx, queue, consumerID, 1)
	if err != nil {
		return taskforge.Delivery{}, err
	}
	return deliveries[0], nil
}

// ReserveBatch reserves up to max deliveries with one blocking stream read.
// Fairness queues deliberately retain single-candidate selection because a
// batch from one tenant stream would bypass weighted tier selection.
func (b *Broker) ReserveBatch(ctx context.Context, queue, consumerID string, max int) ([]taskforge.Delivery, error) {
	if err := b.checkConfig(); err != nil {
		return nil, err
	}
	started := time.Now()
	defer func(queue string) {
		b.metrics.ObserveReserveLatency(normalizeQueue(queue), time.Since(started).Seconds())
	}(queue)

	queue = normalizeQueue(queue)
	if max < 1 {
		max = 1
	}
	if b.fairnessPolicy(queue) != nil {
		delivery, err := b.reserveFair(ctx, queue, consumerID)
		if err != nil {
			return nil, err
		}
		return []taskforge.Delivery{delivery}, nil
	}
	return b.reserveFIFO(ctx, queue, consumerID, max)
}

func (b *Broker) reserveFIFO(ctx context.Context, queue, consumerID string, max int) ([]taskforge.Delivery, error) {
	streamKey := b.streamKey(queue)
	groupName := b.groupName(queue)
	consumerName := b.consumerName(consumerID)

	if err := b.ensureGroup(ctx, streamKey, groupName); err != nil {
		return nil, err
	}

	if b.shouldCheckReclaim(queue) {
		if reclaimed, ok, err := b.reclaimExpiredDelivery(ctx, queue, streamKey, groupName, consumerName); err != nil {
			return nil, err
		} else if ok {
			return []taskforge.Delivery{reclaimed}, nil
		}
	}

	args := &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamKey, ">"},
		Count:    int64(max),
		Block:    b.reserveTTL,
	}
	streams, err := b.readGroup(ctx, streamKey, groupName, args)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, taskforge.ErrNoTask
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("reserve task: %w", err)
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return nil, taskforge.ErrNoTask
	}

	now := time.Now().UTC()
	deliveries := make([]taskforge.Delivery, 0, len(streams[0].Messages))
	for _, entry := range streams[0].Messages {
		msg, err := decodeTask(entry)
		if err != nil {
			return nil, fmt.Errorf("reserve task: %w", err)
		}
		if msg.Queue == "" {
			msg.Queue = queue
		}

		spanCtx := observability.ExtractTraceContext(ctx, msg.Headers)
		ttl := b.effectiveLeaseTTL(msg)
		b.noteLeaseTTL(queue, ttl)
		delivery := newDelivery(msg, queue, consumerName, entry.ID, now, ttl, deliveryCount(msg, 0))
		_, span := observability.StartQueueSpan(
			spanCtx,
			"taskforge.redis",
			"taskforge.reserve",
			msg,
			deliverySpanAttributes(delivery)...,
		)
		span.End()

		logDeliveryReservation(ctx, b.logger, delivery)
		deliveries = append(deliveries, delivery)
	}
	b.recordLeaseDeadlines(ctx, streamKey, deliveries)

	return deliveries, nil
}

func (b *Broker) shouldCheckReclaim(queue string) bool {
	now := time.Now()
	b.reclaimMu.Lock()
	defer b.reclaimMu.Unlock()
	if now.Before(b.reclaimNext[queue]) {
		return false
	}
	interval := b.reclaimIntervals[queue]
	if interval <= 0 {
		interval = min(b.leaseTTL/4, 100*time.Millisecond)
	}
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	b.reclaimNext[queue] = now.Add(interval)
	return true
}

func (b *Broker) noteLeaseTTL(queue string, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	interval := min(ttl/4, 100*time.Millisecond)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	b.reclaimMu.Lock()
	if current := b.reclaimIntervals[queue]; current <= 0 || interval < current {
		b.reclaimIntervals[queue] = interval
	}
	b.reclaimMu.Unlock()
}

func (b *Broker) recordLeaseDeadlines(ctx context.Context, streamKey string, deliveries []taskforge.Delivery) {
	if len(deliveries) == 0 {
		return
	}
	now, err := b.redisNow(ctx)
	if err != nil {
		b.logger.Debug("record lease deadline index time failed", "stream", streamKey, "error", err)
		return
	}
	pipe := b.client.Pipeline()
	key := b.leaseDeadlineKey(streamKey)
	for _, delivery := range deliveries {
		deadline := now.Add(b.effectiveLeaseTTL(delivery.Message))
		pipe.ZAdd(ctx, key, redis.Z{Score: float64(deadline.UnixMilli()), Member: delivery.Execution.DeliveryID})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		b.logger.Debug("record lease deadline index failed", "stream", streamKey, "error", err)
	}
}

func logDeliveryReservation(ctx context.Context, logger *slog.Logger, delivery taskforge.Delivery) {
	if logger == nil || !logger.Enabled(ctx, slog.LevelDebug) {
		return
	}
	logging.WithDelivery(logger, delivery).Debug("reserved task delivery")
}

func (b *Broker) leaseDeadlineKey(streamKey string) string {
	return streamKey + ":lease-deadlines"
}

func (b *Broker) leaseIndexCoversPending(ctx context.Context, streamKey, groupName string) (bool, error) {
	pending, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  "-",
		End:    "+",
		Count:  maxReclaimIndexAuditEntries,
	}).Result()
	if err != nil {
		if isMissingGroup(err) || isMissingStream(err) {
			return true, nil
		}
		return false, fmt.Errorf("inspect pending lease index coverage: %w", err)
	}
	if len(pending) == 0 {
		return true, nil
	}
	if int64(len(pending)) >= maxReclaimIndexAuditEntries {
		return false, nil
	}

	pipe := b.client.Pipeline()
	scores := make([]*redis.FloatCmd, len(pending))
	for index, entry := range pending {
		scores[index] = pipe.ZScore(ctx, b.leaseDeadlineKey(streamKey), entry.ID)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		if isMissingStream(err) || isIndexTypeError(err) {
			return false, nil
		}
		return false, fmt.Errorf("inspect lease index members: %w", err)
	}
	for _, score := range scores {
		if _, err := score.Result(); err != nil {
			if errors.Is(err, redis.Nil) || isIndexTypeError(err) {
				return false, nil
			}
			return false, fmt.Errorf("read lease index member: %w", err)
		}
	}
	return true, nil
}

func (b *Broker) Ack(ctx context.Context, delivery taskforge.Delivery) error {
	if err := b.checkConfig(); err != nil {
		return err
	}
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.redis",
		"taskforge.ack",
		delivery.Message,
		deliverySpanAttributes(delivery)...,
	)
	defer span.End()

	pending, ttl, err := b.finalizeDelivery(ctx, delivery, nil)
	if err != nil {
		b.logDeliveryRejection("ack rejected", delivery, pending, ttl, err)
		observability.MarkSpanError(span, err)
		return err
	}

	return nil
}

func (b *Broker) Nack(ctx context.Context, delivery taskforge.Delivery, requeue bool) error {
	if err := b.checkConfig(); err != nil {
		return err
	}
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.redis",
		"taskforge.nack",
		delivery.Message,
		append(deliverySpanAttributes(delivery), attribute.Bool("taskforge.requeue", requeue))...,
	)
	defer span.End()

	if requeue {
		pending, ttl, err := b.validatePendingDelivery(ctx, delivery)
		if err != nil {
			b.logDeliveryRejection("nack rejected", delivery, pending, ttl, err)
			observability.MarkSpanError(span, err)
			return err
		}
		requeued := delivery.Message
		requeued.ETA = nil
		if _, err := b.Publish(ctx, requeued, taskforge.PublishOptions{
			Source:           taskforge.PublishSourceRequeue,
			DeduplicationKey: fmt.Sprintf("requeue:%s", delivery.OwnershipKey()),
		}); err != nil {
			observability.MarkSpanError(span, err)
			return err
		}
	}

	pending, ttl, err := b.finalizeDelivery(ctx, delivery, nil)
	if err != nil {
		b.logDeliveryRejection("nack rejected", delivery, pending, ttl, err)
		observability.MarkSpanError(span, err)
		return err
	}
	return nil
}

func (b *Broker) ExtendLease(ctx context.Context, delivery taskforge.Delivery, _ time.Duration) error {
	if err := b.checkConfig(); err != nil {
		return err
	}
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.redis",
		"taskforge.extend_lease",
		delivery.Message,
		deliverySpanAttributes(delivery)...,
	)
	defer span.End()

	authoritativeTTL := b.effectiveLeaseTTL(delivery.Message)
	errs, batchErr := b.ExtendLeases(ctx, []taskforge.Delivery{delivery})
	err := batchErr
	if len(errs) > 0 && errs[0] != nil {
		err = errs[0]
	}
	if err != nil {
		b.logLeaseExtensionRejection(ctx, delivery, authoritativeTTL, err)
		observability.MarkSpanError(span, err)
		return err
	}

	logging.WithDelivery(b.logger, delivery).Debug(
		"extended task lease",
		"lease_expiry", time.Now().UTC().Add(authoritativeTTL),
	)

	return nil
}

func (b *Broker) MoveDue(ctx context.Context, fence taskforge.LeadershipFence, now time.Time, limit int64) (int, error) {
	if limit <= 0 {
		limit = 100
	}

	queues, err := b.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     b.delayedQueueIndexKey(),
		Start:   "-inf",
		Stop:    fmt.Sprintf("%d", now.UTC().UnixMilli()),
		ByScore: true,
		Offset:  0,
		Count:   limit,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("move due tasks: query delayed queue index: %w", err)
	}

	moved := 0
	for _, queue := range queues {
		remaining := limit - int64(moved)
		if remaining <= 0 {
			break
		}
		values, err := b.client.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key:     b.delayedQueueKey(queue),
			Start:   "-inf",
			Stop:    fmt.Sprintf("%d", now.UTC().UnixMilli()),
			ByScore: true,
			Offset:  0,
			Count:   remaining,
		}).Result()
		if err != nil {
			return moved, fmt.Errorf("move due tasks: query delayed queue %q: %w", queue, err)
		}
		if len(values) == 0 {
			if err := b.refreshDelayedQueueIndex(ctx, queue); err != nil {
				return moved, err
			}
			continue
		}
		for _, raw := range values {
			entry, err := decodeDelayedEntry(raw)
			if err != nil {
				return moved, fmt.Errorf("move due tasks: decode delayed entry: %w", err)
			}

			msg := entry.Message
			if msg.Headers == nil {
				msg.Headers = map[string]string{}
			}
			scheduledFor := entry.ScheduledFor.UTC()
			msg.Headers[taskforge.HeaderScheduledFor] = scheduledFor.Format(time.RFC3339Nano)
			msg.Headers[taskforge.HeaderReleasedAt] = now.UTC().Format(time.RFC3339Nano)
			msg.Headers[taskforge.HeaderReleaseLagMS] = strconv.FormatInt(now.UTC().Sub(scheduledFor).Milliseconds(), 10)
			msg.ETA = nil
			if err := b.releaseDueEntry(ctx, fence, raw, entry, msg, now); err != nil {
				return moved, fmt.Errorf("move due tasks: release delayed message: %w", err)
			}
			moved++
		}
	}

	return moved, nil
}

func (b *Broker) releaseDueEntry(
	ctx context.Context,
	fence taskforge.LeadershipFence,
	raw string,
	entry delayedEntry,
	msg taskforge.Task,
	now time.Time,
) error {
	opts := taskforge.PublishOptions{
		Source:           taskforge.PublishSourceDueRelease,
		DeduplicationKey: fmt.Sprintf("delayed:%s", entry.EntryID),
	}
	eval, err := b.evaluateAdmission(ctx, msg, opts, now)
	if err != nil {
		return err
	}
	queue := taskforge.EffectiveQueue(msg)
	b.observeAdmissionDecision(queue, opts.Source, eval)
	if eval.decision == taskforge.AdmissionDecisionDeferred {
		if eval.deferUntil == nil {
			return fmt.Errorf("publish task: deferred admission missing defer deadline")
		}
		msg = b.annotateDeferredMessage(msg, opts.Source, eval.reason, *eval.deferUntil, now)
		return b.releaseDueIntoDelayed(ctx, fence, raw, msg, opts.DeduplicationKey)
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("publish task: marshal message: %w", err)
	}
	if b.fairnessPolicy(queue) != nil {
		return b.releaseDueIntoFairReady(ctx, fence, raw, entry.EntryID, queue, msg.FairnessKey, payload, opts.DeduplicationKey, now)
	}
	return b.releaseDueIntoReady(ctx, fence, raw, entry.EntryID, queue, payload, opts.DeduplicationKey)
}

func (b *Broker) releaseDueIntoReady(ctx context.Context, fence taskforge.LeadershipFence, raw, entryID, queue string, payload []byte, deduplicationKey string) error {
	values, err := fencedReleaseReadyScript.Run(
		ctx,
		b.client,
		[]string{
			b.schedulerLeadershipKey(),
			b.delayedQueueKey(queue),
			b.streamKey(queue),
			b.publishReceiptKey(deduplicationKey),
			b.delayedQueueIndexKey(),
			b.delayedRetryIndexKey(queue),
		},
		fence.Token,
		raw,
		streamPayloadField,
		string(payload),
		b.publishReceiptTTL().Milliseconds(),
		queue,
		entryID,
	).Result()
	if err != nil {
		return err
	}
	published, _, err := parseReleaseScriptResult(values, "move_due")
	if err != nil {
		return err
	}
	if published {
		b.metrics.IncPublished(queue)
	}
	return nil
}

func (b *Broker) releaseDueIntoFairReady(ctx context.Context, fence taskforge.LeadershipFence, raw, entryID, queue, fairnessKey string, payload []byte, deduplicationKey string, now time.Time) error {
	values, err := fencedReleaseFairReadyScript.Run(
		ctx,
		b.client,
		[]string{
			b.schedulerLeadershipKey(),
			b.delayedQueueKey(queue),
			b.fairnessKeysSetKey(queue),
			b.fairnessStreamKey(queue, NormalizeFairnessKey(fairnessKey)),
			b.fairnessNotifyKey(queue),
			b.publishReceiptKey(deduplicationKey),
			b.delayedQueueIndexKey(),
			b.delayedRetryIndexKey(queue),
		},
		fence.Token,
		raw,
		NormalizeFairnessKey(fairnessKey),
		streamPayloadField,
		string(payload),
		now.UTC().Format(time.RFC3339Nano),
		b.publishReceiptTTL().Milliseconds(),
		queue,
		entryID,
	).Result()
	if err != nil {
		return err
	}
	published, _, err := parseReleaseScriptResult(values, "move_due")
	if err != nil {
		return err
	}
	if published {
		b.metrics.IncPublished(queue)
	}
	return nil
}

func (b *Broker) releaseDueIntoDelayed(ctx context.Context, fence taskforge.LeadershipFence, raw string, msg taskforge.Task, deduplicationKey string) error {
	oldEntry, err := decodeDelayedEntry(raw)
	if err != nil {
		return fmt.Errorf("publish task: decode delayed entry: %w", err)
	}
	newEntryID := uuid.New().String()
	entryPayload, err := json.Marshal(delayedEntry{
		EntryID:      newEntryID,
		ScheduledFor: msg.ETA.UTC(),
		Message:      msg,
	})
	if err != nil {
		return fmt.Errorf("publish task: marshal delayed entry: %w", err)
	}
	values, err := fencedReleaseDelayedScript.Run(
		ctx,
		b.client,
		[]string{
			b.schedulerLeadershipKey(),
			b.delayedQueueKey(taskforge.EffectiveQueue(oldEntry.Message)),
			b.delayedQueueKey(taskforge.EffectiveQueue(msg)),
			b.publishReceiptKey(deduplicationKey),
			b.delayedQueueIndexKey(),
			b.delayedRetryIndexKey(taskforge.EffectiveQueue(oldEntry.Message)),
			b.delayedRetryIndexKey(taskforge.EffectiveQueue(msg)),
		},
		fence.Token,
		raw,
		msg.ETA.UTC().UnixMilli(),
		string(entryPayload),
		b.publishReceiptTTL().Milliseconds(),
		taskforge.EffectiveQueue(oldEntry.Message),
		taskforge.EffectiveQueue(msg),
		oldEntry.EntryID,
		newEntryID,
		retryIndexFlag(msg),
	).Result()
	if err != nil {
		return err
	}
	published, _, err := parseReleaseScriptResult(values, "move_due")
	if err != nil {
		return err
	}
	if published {
		b.metrics.IncPublished(taskforge.EffectiveQueue(msg))
	}
	return nil
}

func parseReleaseScriptResult(values interface{}, operation string) (bool, bool, error) {
	result, ok := values.([]interface{})
	if !ok || len(result) != 2 {
		return false, false, fmt.Errorf("unexpected release script response %T", values)
	}
	existed, err := redisScriptInt(result[0])
	if err != nil {
		return false, false, err
	}
	if existed == -1 {
		return false, false, taskforge.NewStaleLeadershipError(operation)
	}
	removed, err := redisScriptInt(result[1])
	if err != nil {
		return false, false, err
	}
	return existed == 0, removed == 1, nil
}

func (b *Broker) reclaimExpiredDelivery(ctx context.Context, queue, streamKey, groupName, consumerName string) (taskforge.Delivery, bool, error) {
	if reclaimed, ok, err := b.reclaimIndexedDelivery(ctx, queue, streamKey, groupName, consumerName); err != nil {
		return taskforge.Delivery{}, false, err
	} else if ok {
		return reclaimed, true, nil
	}

	covered, err := b.leaseIndexCoversPending(ctx, streamKey, groupName)
	if err != nil {
		return taskforge.Delivery{}, false, err
	}
	if covered && !b.shouldAuditReclaim(streamKey, time.Now()) {
		return taskforge.Delivery{}, false, nil
	}
	exists, err := b.client.Exists(ctx, b.leaseDeadlineKey(streamKey)).Result()
	if err != nil {
		return taskforge.Delivery{}, false, fmt.Errorf("inspect lease index presence: %w", err)
	}
	if exists == 0 {
		b.resetReclaimScan(streamKey)
	}
	return b.reclaimByScan(ctx, queue, streamKey, groupName, consumerName)
}

func (b *Broker) reclaimIndexedDelivery(ctx context.Context, queue, streamKey, groupName, consumerName string) (taskforge.Delivery, bool, error) {
	now, err := b.redisNow(ctx)
	if err != nil {
		return taskforge.Delivery{}, false, err
	}
	ids, err := b.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     b.leaseDeadlineKey(streamKey),
		Start:   "-inf",
		Stop:    strconv.FormatInt(now.UnixMilli(), 10),
		ByScore: true,
		Offset:  0,
		Count:   b.reclaimBatchSize,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || isIndexTypeError(err) {
			return taskforge.Delivery{}, false, nil
		}
		return taskforge.Delivery{}, false, fmt.Errorf("reclaim task: inspect lease deadlines: %w", err)
	}
	for _, id := range ids {
		delivery, claimed, err := b.reclaimPendingID(ctx, queue, streamKey, groupName, consumerName, id)
		if err != nil {
			if !errors.Is(err, taskforge.ErrUnknownDelivery) {
				return taskforge.Delivery{}, false, err
			}
			_ = b.client.ZRem(ctx, b.leaseDeadlineKey(streamKey), id).Err()
			continue
		}
		if claimed {
			return delivery, true, nil
		}
	}
	return taskforge.Delivery{}, false, nil
}

func (b *Broker) reclaimPendingID(ctx context.Context, queue, streamKey, groupName, consumerName, id string) (taskforge.Delivery, bool, error) {
	pendingEntries, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  id,
		End:    id,
		Count:  1,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || isMissingGroup(err) || isMissingStream(err) {
			return taskforge.Delivery{}, false, taskforge.ErrUnknownDelivery
		}
		return taskforge.Delivery{}, false, fmt.Errorf("inspect pending delivery %s: %w", id, err)
	}
	if len(pendingEntries) == 0 || pendingEntries[0].ID != id {
		return taskforge.Delivery{}, false, taskforge.ErrUnknownDelivery
	}
	messages, err := b.client.XRangeN(ctx, streamKey, id, id, 1).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || isMissingGroup(err) || isMissingStream(err) {
			return taskforge.Delivery{}, false, taskforge.ErrUnknownDelivery
		}
		return taskforge.Delivery{}, false, fmt.Errorf("load pending delivery %s: %w", id, err)
	}
	if len(messages) == 0 {
		return taskforge.Delivery{}, false, taskforge.ErrUnknownDelivery
	}
	msg, err := decodeTask(messages[0])
	if err != nil {
		return taskforge.Delivery{}, false, fmt.Errorf("decode pending delivery %s: %w", id, err)
	}
	return b.claimPendingDelivery(ctx, queue, streamKey, groupName, consumerName, pendingEntries[0], messages[0], msg)
}

func (b *Broker) reclaimByScan(ctx context.Context, queue, streamKey, groupName, consumerName string) (taskforge.Delivery, bool, error) {
	cursor, ok := b.beginReclaimScan(streamKey)
	if !ok {
		return taskforge.Delivery{}, false, nil
	}
	complete := false
	defer func() {
		b.endReclaimScan(streamKey, cursor, complete)
	}()

	start := cursor
	if cursor != "-" {
		start = nextStreamID(cursor)
	}
	pendingEntries, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  start,
		End:    "+",
		Count:  b.reclaimBatchSize,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || isMissingGroup(err) || isMissingStream(err) {
			complete = true
			return taskforge.Delivery{}, false, nil
		}
		complete = true
		return taskforge.Delivery{}, false, fmt.Errorf("reclaim task: inspect pending deliveries: %w", err)
	}
	if len(pendingEntries) == 0 {
		complete = true
		return taskforge.Delivery{}, false, nil
	}
	if int64(len(pendingEntries)) < b.reclaimBatchSize {
		complete = true
	}
	cursor = pendingEntries[len(pendingEntries)-1].ID
	for _, pending := range pendingEntries {
		messages, err := b.client.XRangeN(ctx, streamKey, pending.ID, pending.ID, 1).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || isMissingGroup(err) || isMissingStream(err) {
				_ = b.client.ZRem(ctx, b.leaseDeadlineKey(streamKey), pending.ID).Err()
				continue
			}
			complete = true
			return taskforge.Delivery{}, false, fmt.Errorf("reclaim task: load pending delivery %s: %w", pending.ID, err)
		}
		if len(messages) == 0 {
			_ = b.client.ZRem(ctx, b.leaseDeadlineKey(streamKey), pending.ID).Err()
			continue
		}
		msg, err := decodeTask(messages[0])
		if err != nil {
			complete = true
			return taskforge.Delivery{}, false, fmt.Errorf("reclaim task: decode pending delivery %s: %w", pending.ID, err)
		}
		delivery, claimed, err := b.claimPendingDelivery(ctx, queue, streamKey, groupName, consumerName, pending, messages[0], msg)
		if err != nil {
			complete = true
			return taskforge.Delivery{}, false, err
		}
		if claimed {
			cursor = pending.ID
			return delivery, true, nil
		}
	}
	return taskforge.Delivery{}, false, nil
}

func (b *Broker) reclaimAuditInterval() time.Duration {
	interval := b.leaseTTL / 4
	if interval < time.Second {
		interval = time.Second
	}
	if interval > 5*time.Second {
		interval = 5 * time.Second
	}
	return interval
}

func (b *Broker) shouldAuditReclaim(streamKey string, now time.Time) bool {
	b.reclaimMu.Lock()
	defer b.reclaimMu.Unlock()
	next, scheduled := b.reclaimAuditNext[streamKey]
	if !scheduled {
		b.reclaimAuditNext[streamKey] = now.Add(b.reclaimAuditInterval())
		return false
	}
	if now.Before(next) {
		return false
	}
	b.reclaimAuditNext[streamKey] = now.Add(b.reclaimAuditInterval())
	return true
}

func (b *Broker) resetReclaimScan(streamKey string) {
	b.reclaimMu.Lock()
	delete(b.reclaimCursors, streamKey)
	delete(b.reclaimScanSteps, streamKey)
	b.reclaimAuditNext[streamKey] = time.Time{}
	b.reclaimMu.Unlock()
}

func (b *Broker) beginReclaimScan(streamKey string) (string, bool) {
	b.reclaimMu.Lock()
	defer b.reclaimMu.Unlock()
	if b.reclaimScanning[streamKey] {
		return "", false
	}
	if b.reclaimScanSteps[streamKey] >= maxReclaimScanSteps {
		delete(b.reclaimCursors, streamKey)
		b.reclaimScanSteps[streamKey] = 0
	}
	b.reclaimScanning[streamKey] = true
	b.reclaimScanSteps[streamKey]++
	cursor := b.reclaimCursors[streamKey]
	if cursor == "" {
		return "-", true
	}
	return cursor, true
}

func (b *Broker) endReclaimScan(streamKey, cursor string, complete bool) {
	b.reclaimMu.Lock()
	delete(b.reclaimScanning, streamKey)
	if complete {
		delete(b.reclaimCursors, streamKey)
		delete(b.reclaimScanSteps, streamKey)
	} else if cursor != "" && cursor != "-" {
		b.reclaimCursors[streamKey] = cursor
	}
	b.reclaimMu.Unlock()
}

func (b *Broker) claimPendingDelivery(ctx context.Context, queue, streamKey, groupName, consumerName string, pending redis.XPendingExt, entry redis.XMessage, msg taskforge.Task) (taskforge.Delivery, bool, error) {
	ttl := b.effectiveLeaseTTL(msg)
	if ttl <= 0 {
		return taskforge.Delivery{}, false, nil
	}
	now, err := b.redisNow(ctx)
	if err != nil {
		return taskforge.Delivery{}, false, err
	}
	deadline := now.Add(ttl - pending.Idle)
	b.updateLeaseDeadline(ctx, streamKey, pending.ID, deadline)
	if pending.Idle < ttl {
		return taskforge.Delivery{}, false, nil
	}

	claimed, err := b.client.XClaimJustID(ctx, &redis.XClaimArgs{
		Stream:   streamKey,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  ttl,
		Messages: []string{pending.ID},
	}).Result()
	if err != nil {
		return taskforge.Delivery{}, false, fmt.Errorf("reclaim task: claim expired delivery %s: %w", pending.ID, err)
	}
	if len(claimed) == 0 {
		_ = b.client.ZRem(ctx, b.leaseDeadlineKey(streamKey), pending.ID).Err()
		return taskforge.Delivery{}, false, nil
	}

	b.updateLeaseDeadline(ctx, streamKey, pending.ID, now.Add(ttl))
	delivery := newDelivery(msg, queue, consumerName, entry.ID, now, ttl, deliveryCount(msg, pending.RetryCount+1))
	b.metrics.IncReclaimed(queue)
	reclaimCtx := observability.ExtractTraceContext(ctx, msg.Headers)
	_, span := observability.StartQueueSpan(
		reclaimCtx,
		"taskforge.redis",
		"taskforge.reclaim",
		msg,
		append(
			deliverySpanAttributes(delivery),
			attribute.String("taskforge.previous_owner", pending.Consumer),
		)...,
	)
	span.End()

	logging.WithDelivery(b.logger, delivery).Info(
		"reclaimed expired delivery",
		"previous_owner", pending.Consumer,
		"idle", pending.Idle,
	)
	return delivery, true, nil
}

func (b *Broker) updateLeaseDeadline(ctx context.Context, streamKey, deliveryID string, deadline time.Time) {
	if err := b.client.ZAdd(ctx, b.leaseDeadlineKey(streamKey), redis.Z{
		Score:  float64(deadline.UnixMilli()),
		Member: deliveryID,
	}).Err(); err != nil {
		b.logger.Debug("update lease deadline index failed", "stream", streamKey, "delivery_id", deliveryID, "error", err)
	}
}

func (b *Broker) publishMessage(ctx context.Context, msg taskforge.Task, opts taskforge.PublishOptions, now time.Time) (taskforge.PublishResult, bool, error) {
	opts = opts.Normalize()
	queue := taskforge.EffectiveQueue(msg)
	result := taskforge.PublishResult{
		Decision: taskforge.AdmissionDecisionAccepted,
		Queue:    queue,
	}

	if opts.DeduplicationKey != "" && opts.Source != taskforge.PublishSourceDeadLetter && b.admissionPolicy(queue).Mode != AdmissionModeDisabled {
		exists, err := b.publishReceiptExists(ctx, opts.DeduplicationKey)
		if err != nil {
			return taskforge.PublishResult{}, false, err
		}
		if exists {
			result.Deduplicated = true
			return result, false, nil
		}
	}

	eval, err := b.evaluateAdmission(ctx, msg, opts, now)
	if err != nil {
		return taskforge.PublishResult{}, false, err
	}
	b.observeAdmissionDecision(queue, opts.Source, eval)

	result.Decision = eval.decision
	result.Reason = eval.reason

	switch eval.decision {
	case taskforge.AdmissionDecisionRejected:
		return result, false, &taskforge.AdmissionError{Queue: queue, Reason: eval.reason}
	case taskforge.AdmissionDecisionDeferred:
		if eval.deferUntil == nil {
			return taskforge.PublishResult{}, false, fmt.Errorf("publish task: deferred admission missing defer deadline")
		}
		msg = b.annotateDeferredMessage(msg, opts.Source, eval.reason, *eval.deferUntil, now)
		result.DeferredUntil = eval.deferUntil
	}

	if msg.ETA != nil && msg.ETA.After(now) {
		published, err := b.publishDelayed(ctx, msg, opts.DeduplicationKey, now)
		if err != nil {
			return taskforge.PublishResult{}, false, err
		}
		result.Deduplicated = !published
		if published {
			b.metrics.IncPublished(queue)
		}
		_, builtIn := b.stateStore.(*stateStore)
		return result, published && builtIn && b.stateMode != StateModeDeliveryOnly, nil
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return taskforge.PublishResult{}, false, fmt.Errorf("publish task: marshal message: %w", err)
	}

	published, stateRecorded, err := b.publishReadyTask(ctx, msg, payload, opts.DeduplicationKey, now)
	if err != nil {
		return taskforge.PublishResult{}, false, err
	}
	result.Deduplicated = !published
	if published {
		b.metrics.IncPublished(queue)
	}
	return result, stateRecorded, nil
}

func (b *Broker) publishDelayed(ctx context.Context, msg taskforge.Task, deduplicationKey string, now time.Time) (bool, error) {
	queue := taskforge.EffectiveQueue(msg)
	entryID := uuid.New().String()
	entryPayload, err := json.Marshal(delayedEntry{
		EntryID:      entryID,
		ScheduledFor: msg.ETA.UTC(),
		Message:      msg,
	})
	if err != nil {
		return false, fmt.Errorf("publish task: marshal delayed entry: %w", err)
	}

	taskKey := ""
	stateArgs := []any{0}
	if store, ok := b.stateStore.(*stateStore); ok && b.stateMode != StateModeDeliveryOnly {
		record, err := store.queuedRecord(msg, now)
		if err != nil {
			return false, err
		}
		taskKey = store.taskKey(record.taskID)
		stateArgs = []any{len(record.fields)}
		for field, value := range record.fields {
			stateArgs = append(stateArgs, field, value)
		}
	}

	if deduplicationKey == "" {
		args := []any{
			msg.ETA.UTC().UnixMilli(),
			string(entryPayload),
			queue,
			entryID,
			retryIndexFlag(msg),
		}
		args = append(args, stateArgs...)
		if err := publishDelayedScript.Run(
			ctx,
			b.client,
			[]string{b.delayedQueueKey(queue), b.delayedQueueIndexKey(), b.delayedRetryIndexKey(queue), taskKey},
			args...,
		).Err(); err != nil {
			return false, err
		}
		return true, nil
	}
	args := []any{
		msg.ETA.UTC().UnixMilli(),
		string(entryPayload),
		b.publishReceiptTTL().Milliseconds(),
		queue,
		entryID,
		retryIndexFlag(msg),
	}
	args = append(args, stateArgs...)
	published, err := publishDelayedWithReceiptScript.Run(
		ctx,
		b.client,
		[]string{
			b.delayedQueueKey(queue),
			b.publishReceiptKey(deduplicationKey),
			b.delayedQueueIndexKey(),
			b.delayedRetryIndexKey(queue),
			taskKey,
		},
		args...,
	).Int64()
	if err != nil {
		return false, err
	}
	return published == 1, nil
}

func (b *Broker) ensureGroup(ctx context.Context, streamKey, groupName string) error {
	cacheKey := streamKey + "\x00" + groupName
	b.consumerGroupsMu.RLock()
	_, ok := b.consumerGroups[cacheKey]
	b.consumerGroupsMu.RUnlock()
	if ok {
		return nil
	}

	b.consumerGroupsMu.Lock()
	defer b.consumerGroupsMu.Unlock()
	if _, ok := b.consumerGroups[cacheKey]; ok {
		return nil
	}

	err := b.client.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Err()
	if err == nil || strings.HasPrefix(err.Error(), "BUSYGROUP ") {
		b.consumerGroups[cacheKey] = struct{}{}
		return nil
	}
	return fmt.Errorf("ensure consumer group: %w", err)
}

func (b *Broker) forgetGroup(streamKey, groupName string) {
	b.consumerGroupsMu.Lock()
	delete(b.consumerGroups, streamKey+"\x00"+groupName)
	b.consumerGroupsMu.Unlock()
	b.reclaimMu.Lock()
	delete(b.reclaimCursors, streamKey)
	delete(b.reclaimScanning, streamKey)
	delete(b.reclaimScanSteps, streamKey)
	delete(b.reclaimAuditNext, streamKey)
	b.reclaimMu.Unlock()
}

func (b *Broker) readGroup(ctx context.Context, streamKey, groupName string, args *redis.XReadGroupArgs) ([]redis.XStream, error) {
	streams, err := b.client.XReadGroup(ctx, args).Result()
	if !isMissingGroup(err) {
		return streams, err
	}

	// Redis may be reset independently of a long-lived broker. Invalidate the
	// positive cache and recreate the group before retrying once.
	b.forgetGroup(streamKey, groupName)
	if ensureErr := b.ensureGroup(ctx, streamKey, groupName); ensureErr != nil {
		return nil, ensureErr
	}
	return b.client.XReadGroup(ctx, args).Result()
}

func (b *Broker) finalizeDelivery(ctx context.Context, delivery taskforge.Delivery, record *stateRecord) (redis.XPendingExt, time.Duration, error) {
	ttl := b.effectiveLeaseTTL(delivery.Message)
	if delivery.Execution.DeliveryID == "" {
		return redis.XPendingExt{}, ttl, taskforge.ErrUnknownDelivery
	}

	queue := taskforge.EffectiveQueue(delivery.Message)
	streamKey := b.queueStreamKey(queue, delivery.Message.FairnessKey)
	keys := []string{streamKey, b.leaseDeadlineKey(streamKey)}
	args := []any{
		b.groupName(queue),
		delivery.Execution.DeliveryID,
		delivery.Execution.LeaseOwner,
		ttl.Milliseconds(),
	}
	if record != nil {
		keys = append(keys, b.stateStore.(*stateStore).taskKey(record.taskID))
		stateTTL := b.stateStore.(*stateStore).recordTTL(record.state)
		stateTTLMillis := int64(-1)
		if stateTTL > 0 {
			stateTTLMillis = stateTTL.Milliseconds()
		}
		args = append(args, stateTTLMillis, len(record.fields))
		for field, value := range record.fields {
			args = append(args, field, value)
		}
	}
	result, err := finalizeDeliveryScript.Run(
		ctx,
		b.client,
		keys,
		args...,
	).Result()
	if err != nil {
		return redis.XPendingExt{}, ttl, fmt.Errorf("finalize task: %w", err)
	}
	values, ok := result.([]any)
	if !ok || len(values) != 3 {
		return redis.XPendingExt{}, ttl, fmt.Errorf("finalize task: unexpected result %T", result)
	}
	code, ok := int64Value(values[0])
	if !ok {
		return redis.XPendingExt{}, ttl, fmt.Errorf("finalize task: unexpected status %T", values[0])
	}
	owner, _ := values[1].(string)
	idleMillis, _ := int64Value(values[2])
	pending := redis.XPendingExt{
		ID:       delivery.Execution.DeliveryID,
		Consumer: owner,
		Idle:     time.Duration(idleMillis) * time.Millisecond,
	}
	switch code {
	case 1:
		return pending, ttl, nil
	case 2:
		return pending, ttl, taskforge.ErrStaleDelivery
	case 3:
		return pending, ttl, taskforge.ErrDeliveryExpired
	default:
		return pending, ttl, taskforge.ErrUnknownDelivery
	}
}

func (b *Broker) validatePendingDelivery(ctx context.Context, delivery taskforge.Delivery) (redis.XPendingExt, time.Duration, error) {
	if delivery.Execution.DeliveryID == "" {
		return redis.XPendingExt{}, 0, taskforge.ErrUnknownDelivery
	}

	queue := taskforge.EffectiveQueue(delivery.Message)
	pendingEntries, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: b.queueStreamKey(queue, delivery.Message.FairnessKey),
		Group:  b.groupName(queue),
		Start:  delivery.Execution.DeliveryID,
		End:    delivery.Execution.DeliveryID,
		Count:  1,
	}).Result()
	if err != nil {
		return redis.XPendingExt{}, 0, fmt.Errorf("inspect pending delivery: %w", err)
	}
	if len(pendingEntries) == 0 || pendingEntries[0].ID != delivery.Execution.DeliveryID {
		return redis.XPendingExt{}, 0, taskforge.ErrUnknownDelivery
	}

	pending := pendingEntries[0]
	ttl := b.effectiveLeaseTTL(delivery.Message)
	if delivery.Execution.LeaseOwner != "" && pending.Consumer != delivery.Execution.LeaseOwner {
		return pending, ttl, taskforge.ErrStaleDelivery
	}
	if ttl > 0 && pending.Idle >= ttl {
		return pending, ttl, taskforge.ErrDeliveryExpired
	}

	return pending, ttl, nil
}

func (b *Broker) logLeaseExtensionRejection(ctx context.Context, delivery taskforge.Delivery, ttl time.Duration, err error) {
	pending := redis.XPendingExt{}
	if errors.Is(err, taskforge.ErrStaleDelivery) || errors.Is(err, taskforge.ErrDeliveryExpired) {
		if entries, inspectErr := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: b.queueStreamKey(taskforge.EffectiveQueue(delivery.Message), delivery.Message.FairnessKey),
			Group:  b.groupName(taskforge.EffectiveQueue(delivery.Message)),
			Start:  delivery.Execution.DeliveryID,
			End:    delivery.Execution.DeliveryID,
			Count:  1,
		}).Result(); inspectErr == nil && len(entries) == 1 {
			pending = entries[0]
		}
	}
	b.logDeliveryRejection("lease extension rejected", delivery, pending, ttl, err)
}

func (b *Broker) logDeliveryRejection(message string, delivery taskforge.Delivery, pending redis.XPendingExt, ttl time.Duration, err error) {
	if b.logger == nil {
		return
	}

	var expiresAt any
	if ttl > 0 && pending.Idle > 0 {
		expiresAt = time.Now().UTC().Add(ttl - pending.Idle)
	}

	logging.WithDelivery(b.logger, delivery).Warn(
		message,
		"current_owner", pending.Consumer,
		"lease_expiry", expiresAt,
		"error", err,
	)
}

type queueDepth struct {
	length       int64
	pendingCount int64
	consumers    int
}

// loadQueueDepth reads a non-fair queue's depth state in one pipelined round
// trip; consumers are only enumerated when requested.
func (b *Broker) loadQueueDepth(ctx context.Context, queue string, includeConsumers bool) (queueDepth, error) {
	streamKey := b.streamKey(queue)
	groupName := b.groupName(queue)

	pipe := b.client.Pipeline()
	lengthCmd := pipe.XLen(ctx, streamKey)
	pendingCmd := pipe.XPending(ctx, streamKey, groupName)
	consumersCmd := (*redis.XInfoConsumersCmd)(nil)
	if includeConsumers {
		consumersCmd = pipe.XInfoConsumers(ctx, streamKey, groupName)
	}
	_, _ = pipe.Exec(ctx)

	depth := queueDepth{}
	length, err := lengthCmd.Result()
	if err != nil {
		if !isMissingStream(err) {
			return queueDepth{}, fmt.Errorf("queue metrics: stream %q: %w", queue, err)
		}
	} else {
		depth.length = length
	}

	pending, err := pendingCmd.Result()
	if err != nil {
		if !isMissingGroup(err) && !isMissingStream(err) {
			return queueDepth{}, fmt.Errorf("queue metrics: pending %q: %w", queue, err)
		}
	} else {
		depth.pendingCount = pending.Count
	}

	if includeConsumers {
		consumers, err := consumersCmd.Result()
		if err != nil {
			if !isMissingGroup(err) && !isMissingStream(err) {
				return queueDepth{}, fmt.Errorf("queue metrics: consumers %q: %w", queue, err)
			}
		} else {
			depth.consumers = len(consumers)
		}
	}
	return depth, nil
}

func (b *Broker) QueueMetricsSnapshot(ctx context.Context, queue string) (taskforge.QueueMetricsSnapshot, error) {
	queue = normalizeQueue(queue)
	if b.fairnessPolicy(queue) != nil {
		return b.fairQueueMetricsSnapshot(ctx, queue)
	}

	depth, err := b.loadQueueDepth(ctx, queue, true)
	if err != nil {
		return taskforge.QueueMetricsSnapshot{}, err
	}

	ready := depth.length - depth.pendingCount
	if ready < 0 {
		ready = 0
	}

	return taskforge.QueueMetricsSnapshot{
		Depth:     float64(ready),
		Reserved:  float64(depth.pendingCount),
		Consumers: float64(depth.consumers),
	}, nil
}

func (b *Broker) DeadLetterQueueSize(ctx context.Context, queue string) (float64, error) {
	length, err := b.deadLetterQueueSizeInt(ctx, queue)
	if err != nil {
		return 0, err
	}
	return float64(length), nil
}

func (b *Broker) SchedulerLag(ctx context.Context, now time.Time, queue string) (float64, error) {
	queue = normalizeQueue(queue)
	values, err := b.client.ZRange(ctx, b.delayedQueueKey(queue), 0, 0).Result()
	if err != nil {
		if isMissingStream(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("scheduler lag metrics %q: %w", queue, err)
	}

	if len(values) == 0 {
		return 0, nil
	}
	entry, err := decodeDelayedEntry(values[0])
	if err != nil {
		return 0, fmt.Errorf("scheduler lag metrics %q: decode delayed entry: %w", queue, err)
	}
	lag := now.UTC().Sub(entry.ScheduledFor.UTC())
	if lag < 0 {
		return 0, nil
	}
	return lag.Seconds(), nil
}

func (b *Broker) incrementLeaseExtensionFailure(queue string) {
	b.metrics.IncLeaseExtensionFailure(queue)
}

func isIndexTypeError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "WRONGTYPE")
}

func isMissingGroup(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "NOGROUP")
}

func isMissingStream(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key")
}

func (b *Broker) oldestReadyAge(ctx context.Context, streamKey, groupName string, now time.Time) time.Duration {
	firstReady, ok, err := b.oldestReadyMessage(ctx, streamKey, groupName)
	if err != nil || !ok {
		return 0
	}

	msg, err := decodeTask(firstReady)
	if err != nil || msg.CreatedAt.IsZero() {
		return 0
	}
	age := now.UTC().Sub(msg.CreatedAt.UTC())
	if age < 0 {
		return 0
	}
	return age
}

func (b *Broker) oldestReadyMessage(ctx context.Context, streamKey, groupName string) (redis.XMessage, bool, error) {
	if groupName == "" {
		return b.firstStreamMessage(ctx, streamKey)
	}

	pending, err := b.client.XPending(ctx, streamKey, groupName).Result()
	switch {
	case err == nil && pending.Count == 0:
		return b.firstStreamMessage(ctx, streamKey)
	case err == nil:
	case isMissingGroup(err):
		return b.firstStreamMessage(ctx, streamKey)
	case isMissingStream(err):
		return redis.XMessage{}, false, nil
	default:
		return redis.XMessage{}, false, fmt.Errorf("oldest ready message: inspect pending: %w", err)
	}

	streamCursor := "-"
	pendingCursor := "-"
	var streamBatch []redis.XMessage
	var pendingBatch []redis.XPendingExt
	streamIndex := 0
	pendingIndex := 0

	for {
		if streamIndex >= len(streamBatch) {
			streamBatch, err = b.loadStreamBatch(ctx, streamKey, streamCursor)
			if err != nil {
				return redis.XMessage{}, false, err
			}
			streamIndex = 0
			if len(streamBatch) == 0 {
				return redis.XMessage{}, false, nil
			}
			streamCursor = streamBatch[len(streamBatch)-1].ID
		}

		if pendingIndex >= len(pendingBatch) {
			pendingBatch, err = b.loadPendingBatch(ctx, streamKey, groupName, pendingCursor)
			if err != nil {
				return redis.XMessage{}, false, err
			}
			pendingIndex = 0
			if len(pendingBatch) > 0 {
				pendingCursor = pendingBatch[len(pendingBatch)-1].ID
			}
		}

		streamEntry := streamBatch[streamIndex]
		if pendingIndex >= len(pendingBatch) {
			return streamEntry, true, nil
		}

		switch compareStreamIDs(streamEntry.ID, pendingBatch[pendingIndex].ID) {
		case -1:
			return streamEntry, true, nil
		case 0:
			streamIndex++
			pendingIndex++
		default:
			pendingIndex++
		}
	}
}

func (b *Broker) firstStreamMessage(ctx context.Context, streamKey string) (redis.XMessage, bool, error) {
	messages, err := b.client.XRangeN(ctx, streamKey, "-", "+", 1).Result()
	if err != nil {
		if isMissingStream(err) {
			return redis.XMessage{}, false, nil
		}
		return redis.XMessage{}, false, fmt.Errorf("oldest ready message: load first stream entry: %w", err)
	}
	if len(messages) == 0 {
		return redis.XMessage{}, false, nil
	}
	return messages[0], true, nil
}

func (b *Broker) loadStreamBatch(ctx context.Context, streamKey, cursor string) ([]redis.XMessage, error) {
	messages, err := b.client.XRangeN(ctx, streamKey, cursor, "+", oldestReadyScanCount).Result()
	if err != nil {
		if isMissingStream(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("oldest ready message: load stream batch: %w", err)
	}
	if cursor != "-" && len(messages) > 0 && messages[0].ID == cursor {
		messages = messages[1:]
	}
	return messages, nil
}

func (b *Broker) loadPendingBatch(ctx context.Context, streamKey, groupName, cursor string) ([]redis.XPendingExt, error) {
	start := cursor
	if cursor != "-" {
		start = nextStreamID(cursor)
	}
	pendingEntries, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  start,
		End:    "+",
		Count:  oldestReadyScanCount,
	}).Result()
	if err != nil {
		if isMissingGroup(err) || isMissingStream(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("oldest ready message: load pending batch: %w", err)
	}
	return pendingEntries, nil
}

func compareStreamIDs(left, right string) int {
	leftMillis, leftSeq, leftOK := parseStreamID(left)
	rightMillis, rightSeq, rightOK := parseStreamID(right)
	if !leftOK || !rightOK {
		return compareStrings(left, right)
	}
	switch {
	case leftMillis < rightMillis:
		return -1
	case leftMillis > rightMillis:
		return 1
	case leftSeq < rightSeq:
		return -1
	case leftSeq > rightSeq:
		return 1
	default:
		return 0
	}
}

func parseStreamID(value string) (int64, int64, bool) {
	parts := strings.SplitN(value, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	millis, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return millis, seq, true
}

func nextStreamID(value string) string {
	millis, seq, ok := parseStreamID(value)
	if !ok {
		return value
	}
	return fmt.Sprintf("%d-%d", millis, seq+1)
}

func (b *Broker) redisNow(ctx context.Context) (time.Time, error) {
	now, err := b.client.Time(ctx).Result()
	if err != nil {
		return time.Time{}, fmt.Errorf("redis time: %w", err)
	}
	return now.UTC(), nil
}

func (b *Broker) effectiveLeaseTTL(msg taskforge.Task) time.Duration {
	if msg.VisibilityTimeout > 0 {
		return normalizeLeaseTTL(msg.VisibilityTimeout)
	}
	return normalizeLeaseTTL(b.leaseTTL)
}

func (b *Broker) streamKey(queue string) string {
	return b.prefix + ":stream:" + normalizeQueue(queue)
}

func (b *Broker) groupName(queue string) string {
	return b.prefix + ":" + normalizeQueue(queue)
}

func (b *Broker) consumerName(consumerID string) string {
	base := consumerID
	if base == "" {
		base = "worker"
	}
	return base + ":" + b.hostname + ":" + b.instanceID
}

func (b *Broker) delayedQueueKey(queue string) string {
	return b.prefix + ":delayed:queue:" + normalizeQueue(queue)
}

func (b *Broker) delayedQueueIndexKey() string {
	return b.prefix + ":delayed:queues"
}

func (b *Broker) delayedRetryIndexKey(queue string) string {
	return b.prefix + ":delayed:retry:" + normalizeQueue(queue)
}

func (b *Broker) schedulerLeadershipKey() string {
	return b.prefix + ":scheduler:leader"
}

func (b *Broker) publishReceiptKey(deduplicationKey string) string {
	sum := sha256Sum(deduplicationKey)
	return b.prefix + ":publish:receipt:" + hex.EncodeToString(sum[:])
}

func (b *Broker) publishReceiptTTL() time.Duration {
	return defaultPublishReceiptTTL
}

func (b *Broker) publishReceiptExists(ctx context.Context, deduplicationKey string) (bool, error) {
	exists, err := b.client.Exists(ctx, b.publishReceiptKey(deduplicationKey)).Result()
	if err != nil {
		return false, fmt.Errorf("publish task: inspect receipt: %w", err)
	}
	return exists > 0, nil
}

func (b *Broker) refreshDelayedQueueIndex(ctx context.Context, queue string) error {
	queue = normalizeQueue(queue)
	values, err := b.client.ZRangeWithScores(ctx, b.delayedQueueKey(queue), 0, 0).Result()
	if err != nil {
		return fmt.Errorf("refresh delayed queue index %q: %w", queue, err)
	}
	if len(values) == 0 {
		if err := b.client.ZRem(ctx, b.delayedQueueIndexKey(), queue).Err(); err != nil {
			return fmt.Errorf("refresh delayed queue index %q: remove queue: %w", queue, err)
		}
		return nil
	}
	if err := b.client.ZAdd(ctx, b.delayedQueueIndexKey(), redis.Z{
		Score:  values[0].Score,
		Member: queue,
	}).Err(); err != nil {
		return fmt.Errorf("refresh delayed queue index %q: update queue: %w", queue, err)
	}
	return nil
}

func normalizeQueue(queue string) string {
	if queue == "" {
		return "default"
	}
	return queue
}

func normalizePublishedMessage(msg taskforge.Task, now time.Time) taskforge.Task {
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	if msg.Queue == "" {
		msg.Queue = "default"
	}
	msg.FairnessKey = strings.TrimSpace(msg.FairnessKey)
	return msg
}

func decodeTask(entry redis.XMessage) (taskforge.Task, error) {
	raw, ok := entry.Values[streamPayloadField]
	if !ok {
		return taskforge.Task{}, fmt.Errorf("missing %q field", streamPayloadField)
	}

	payload, err := messagePayload(raw)
	if err != nil {
		return taskforge.Task{}, err
	}

	var msg taskforge.Task
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		return taskforge.Task{}, fmt.Errorf("unmarshal message: %w", err)
	}
	return msg, nil
}

func messagePayload(raw interface{}) (string, error) {
	switch value := raw.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	default:
		return "", fmt.Errorf("unexpected stream payload type %T", raw)
	}
}

type delayedEntry struct {
	EntryID      string         `json:"entry_id"`
	ScheduledFor time.Time      `json:"scheduled_for"`
	Message      taskforge.Task `json:"message"`
}

func decodeDelayedEntry(raw string) (delayedEntry, error) {
	var entry delayedEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return delayedEntry{}, err
	}
	if entry.EntryID == "" {
		return delayedEntry{}, fmt.Errorf("missing delayed entry id")
	}
	return entry, nil
}

func retryIndexFlag(msg taskforge.Task) string {
	if msg.Headers != nil && msg.Headers[taskforge.HeaderRetryScheduledAt] != "" {
		return "1"
	}
	return "0"
}

func deliveryCount(msg taskforge.Task, fallback int64) int {
	count := msg.Attempt + 1
	if fallback > int64(count) {
		count = int(fallback)
	}
	if count < 1 {
		return 1
	}
	return count
}

func redisScriptInt(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected redis script integer type %T", value)
	}
}

func newDelivery(msg taskforge.Task, queue, consumerID, deliveryID string, now time.Time, ttl time.Duration, count int) taskforge.Delivery {
	firstEnqueuedAt := msg.CreatedAt
	if firstEnqueuedAt.IsZero() {
		firstEnqueuedAt = now
	}

	return taskforge.Delivery{
		Message: msg,
		Execution: taskforge.ExecutionMetadata{
			TaskID:          msg.ID,
			DeliveryID:      deliveryID,
			DeliveryCount:   count,
			FirstEnqueuedAt: firstEnqueuedAt,
			LeasedAt:        now,
			LeaseExpiresAt:  now.Add(ttl),
			LeaseOwner:      consumerID,
			LastError:       messageLastError(msg),
			State:           taskforge.StateLeased,
		},
	}
}

func messageLastError(msg taskforge.Task) string {
	if msg.Headers == nil {
		return ""
	}
	return msg.Headers["last_error"]
}

func deliverySpanAttributes(delivery taskforge.Delivery) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("taskforge.delivery_id", delivery.Execution.DeliveryID),
		attribute.String("taskforge.worker_identity", delivery.Execution.LeaseOwner),
		attribute.Int("taskforge.delivery_count", delivery.Execution.DeliveryCount),
	}
}
