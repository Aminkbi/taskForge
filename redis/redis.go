package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge"
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
