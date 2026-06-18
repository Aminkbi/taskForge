package taskforge

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/brokerredis"
	"github.com/aminkbi/taskforge/internal/dlq"
	"github.com/aminkbi/taskforge/internal/observability"
	"github.com/aminkbi/taskforge/internal/store"
	"github.com/aminkbi/taskforge/internal/storeredis"
)

const (
	defaultRedisAddr            = "localhost:6379"
	defaultLeaseTTL             = 30 * time.Second
	defaultTaskSuccessRetention = 24 * time.Hour
	defaultTaskFailureRetention = 168 * time.Hour
	defaultTaskPayloadRetention = 24 * time.Hour
)

type RedisOptions struct {
	Addr                  string
	Password              string
	DB                    int
	Client                *redis.Client
	LeaseTTL              time.Duration
	ReserveTimeout        time.Duration
	Logger                *slog.Logger
	TaskSuccessRetention  time.Duration
	TaskFailureRetention  time.Duration
	TaskPayloadRetention  time.Duration
	DisableTaskExpiration bool
}

type RedisBroker struct {
	client      *redis.Client
	ownedClient bool
	broker      *brokerredis.RedisBroker
	deadLetter  *dlq.Service
	stateStore  *storeredis.RedisStore
	metrics     *observability.Metrics
	logger      *slog.Logger
	leaseTTL    time.Duration
}

func NewRedisBroker(options RedisOptions) (*RedisBroker, error) {
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
		addr := options.Addr
		if addr == "" {
			addr = defaultRedisAddr
		}
		client = redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: options.Password,
			DB:       options.DB,
		})
		ownedClient = true
	}

	metrics := observability.NewMetrics()
	stateStore := storeredis.New(client, retentionPolicy(options))
	internalBroker := brokerredis.NewWithOptions(client, logger.With("component", "brokerredis"), leaseTTL, metrics, brokerredis.Options{
		ReserveTimeout: options.ReserveTimeout,
		StateStore:     stateStore,
	})

	return &RedisBroker{
		client:      client,
		ownedClient: ownedClient,
		broker:      internalBroker,
		deadLetter:  dlq.NewService(client, internalBroker, logger.With("component", "dlq")),
		stateStore:  stateStore,
		metrics:     metrics,
		logger:      logger,
		leaseTTL:    leaseTTL,
	}, nil
}

func (b *RedisBroker) Publish(ctx context.Context, task Task, options PublishOptions) (PublishResult, error) {
	result, err := b.broker.Publish(ctx, task.toBrokerMessage(), options.toBrokerOptions())
	if err != nil {
		return PublishResult{}, admissionErrorFromBroker(err)
	}
	return publishResultFromBroker(result), nil
}

func (b *RedisBroker) Ping(ctx context.Context) error {
	return b.broker.Ping(ctx)
}

func (b *RedisBroker) GetTask(ctx context.Context, taskID string) (TaskRecord, error) {
	record, err := b.stateStore.Get(ctx, taskID)
	if err != nil {
		return TaskRecord{}, err
	}
	return taskRecordFromStore(record), nil
}

func (b *RedisBroker) MetricsHandler() http.Handler {
	return b.metrics.Handler()
}

func (b *RedisBroker) Close() error {
	if !b.ownedClient {
		return nil
	}
	return b.client.Close()
}

func (b *RedisBroker) Reserve(ctx context.Context, queue, consumerID string) (Delivery, error) {
	delivery, err := b.broker.Reserve(ctx, queue, consumerID)
	if err != nil {
		return Delivery{}, err
	}
	return deliveryFromBroker(delivery), nil
}

func (b *RedisBroker) Ack(ctx context.Context, delivery Delivery) error {
	return b.broker.Ack(ctx, delivery.toBrokerDelivery())
}

func (b *RedisBroker) Nack(ctx context.Context, delivery Delivery, requeue bool) error {
	return b.broker.Nack(ctx, delivery.toBrokerDelivery(), requeue)
}

func (b *RedisBroker) ExtendLease(ctx context.Context, delivery Delivery, ttl time.Duration) error {
	return b.broker.ExtendLease(ctx, delivery.toBrokerDelivery(), ttl)
}

func retentionPolicy(options RedisOptions) store.RetentionPolicy {
	if options.DisableTaskExpiration {
		return store.RetentionPolicy{}
	}
	successRetention := options.TaskSuccessRetention
	if successRetention <= 0 {
		successRetention = defaultTaskSuccessRetention
	}
	failureRetention := options.TaskFailureRetention
	if failureRetention <= 0 {
		failureRetention = defaultTaskFailureRetention
	}
	payloadRetention := options.TaskPayloadRetention
	if payloadRetention <= 0 {
		payloadRetention = defaultTaskPayloadRetention
	}
	return store.RetentionPolicy{
		SucceededState: successRetention,
		FailedState:    failureRetention,
		ResultPayload:  payloadRetention,
	}
}

func (b *RedisBroker) internalBroker() broker.Broker {
	return b.broker
}
