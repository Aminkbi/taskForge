package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/brokerredis"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/demo"
	"github.com/aminkbi/taskforge/internal/logging"
	"github.com/aminkbi/taskforge/internal/observability"
	runtimepkg "github.com/aminkbi/taskforge/internal/runtime"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
	"github.com/aminkbi/taskforge/internal/shutdown"
)

func main() {
	ctx, stop := shutdown.NotifyContext(context.Background())
	defer stop()

	cfg, err := config.Load("taskforge-demo")
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	if os.Getenv("TASKFORGE_REDIS_DB") == "" {
		cfg.RedisDB = 15
	}

	settings, err := loadDemoSettings()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load demo settings: %v\n", err)
		os.Exit(1)
	}

	logger, err := logging.New(cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build logger: %v\n", err)
		os.Exit(1)
	}

	shutdownTracing, err := observability.SetupTracing(ctx, observability.TraceConfig{
		Enabled:     cfg.OTELEnabled,
		ServiceName: cfg.ServiceName,
	}, logger)
	if err != nil {
		logger.Error("setup tracing", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			logger.Error("shutdown tracing", "error", err)
		}
	}()

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer client.Close()

	metrics := observability.NewMetrics()
	queueNames := make([]string, 0, len(cfg.WorkerPools))
	workers := make([]*runtimepkg.Worker, 0, len(cfg.WorkerPools))
	globalLimiter := runtimepkg.NewTaskTypeLimiter(cfg.TaskTypeLimits)
	fairnessPolicies := config.FairnessPoliciesByQueue(cfg.WorkerPools)
	for _, pool := range cfg.WorkerPools {
		queueNames = append(queueNames, pool.Queue)
		poolBroker := brokerredis.NewWithOptions(client, logger.With("component", "brokerredis", "pool", pool.Name, "queue", pool.Queue), pool.LeaseTTL, metrics, brokerredis.Options{
			FairnessPolicies: fairnessPolicies,
		})
		workers = append(workers, &runtimepkg.Worker{
			Broker:            poolBroker,
			Handler:           demo.Handler{Logger: logger.With("component", "demo-handler", "pool", pool.Name, "queue", pool.Queue)},
			Logger:            logger.With("component", "worker-runtime", "pool", pool.Name, "queue", pool.Queue),
			Metrics:           metrics,
			Clock:             clock.RealClock{},
			RetryPolicy:       pool.RetryPolicy,
			PoolName:          pool.Name,
			Queue:             pool.Queue,
			ConsumerID:        cfg.ServiceName,
			LeaseTTL:          pool.LeaseTTL,
			Concurrency:       pool.Concurrency,
			Prefetch:          pool.Prefetch,
			GlobalTaskLimiter: globalLimiter,
			PoolTaskLimiter:   runtimepkg.NewTaskTypeLimiter(pool.TaskTypeLimits),
		})
	}

	if len(cfg.WorkerPools) == 0 {
		fmt.Fprintln(os.Stderr, "demo requires at least one worker pool")
		os.Exit(1)
	}

	brokerInstance := brokerredis.NewWithOptions(client, logger.With("component", "brokerredis"), cfg.WorkerPools[0].LeaseTTL, metrics, brokerredis.Options{
		FairnessPolicies: fairnessPolicies,
	})
	if err := brokerInstance.Ping(ctx); err != nil {
		logger.Error("ping redis", "error", err)
		os.Exit(1)
	}
	_ = metrics.RegisterQueueMetricsCollector(brokerInstance, queueNames)
	_ = metrics.RegisterFairnessMetricsCollector(brokerInstance, queueNames)
	manager := &runtimepkg.Manager{Workers: workers}
	demoQueue := cfg.WorkerPools[0].Queue

	recurringSchedules := cfg.RecurringSchedules
	if settings.RecurringEvery > 0 {
		recurringSchedules = append(recurringSchedules, buildRecurringSchedule(demoQueue, settings))
	}

	elector := schedulerpkg.NewRedisLeaderElector(
		client,
		clock.RealClock{},
		logger.With("component", "scheduler-leader"),
		cfg.ServiceName+":demo",
		cfg.SchedulerLockTTL,
		cfg.SchedulerRenewInterval,
	)
	recurring := schedulerpkg.NewRecurringService(
		brokerInstance,
		schedulerpkg.NewRedisScheduleStateStore(client),
		recurringSchedules,
		logger.With("component", "scheduler-recurring"),
	)
	scheduler := schedulerpkg.New(
		brokerInstance,
		recurring,
		elector,
		clock.RealClock{},
		logger.With("component", "scheduler-runtime"),
		cfg.PollInterval,
		cfg.SchedulerRenewInterval,
	)

	if err := client.FlushDB(ctx).Err(); err != nil {
		logger.Error("flush demo redis db", "error", err)
		os.Exit(1)
	}

	if err := publishStartupTasks(ctx, brokerInstance, settings, cfg.WorkerPools); err != nil {
		logger.Error("publish startup demo tasks", "error", err)
		os.Exit(1)
	}

	if settings.DelayedAfter > 0 {
		if err := publishDelayedDemoTask(ctx, brokerInstance, demoQueue, settings); err != nil {
			logger.Error("publish delayed demo task", "error", err)
			os.Exit(1)
		}
	}

	logger.Info(
		"demo starting",
		"redis_addr", cfg.RedisAddr,
		"redis_db", cfg.RedisDB,
		"output_file", settings.OutputFile,
		"worker_pools", len(cfg.WorkerPools),
		"demo_queue", demoQueue,
		"delayed_after", settings.DelayedAfter,
		"recurring_every", settings.RecurringEvery,
		"run_for", settings.RunFor,
	)

	runCtx := ctx
	if settings.RunFor > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, settings.RunFor)
		defer cancel()
	}

	errCh := make(chan error, 2)
	go func() {
		errCh <- manager.Run(runCtx)
	}()
	go func() {
		errCh <- scheduler.Run(runCtx)
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			logger.Error("demo exited with error", "error", err)
			os.Exit(1)
		}
	}

	logger.Info("demo finished", "output_file", settings.OutputFile)
}

type demoSettings struct {
	OutputFile     string
	DelayedAfter   time.Duration
	RecurringEvery time.Duration
	RunFor         time.Duration
}

func loadDemoSettings() (demoSettings, error) {
	outputFile := os.Getenv("TASKFORGE_DEMO_OUTPUT_FILE")
	if outputFile == "" {
		outputFile = "/tmp/taskforge-demo.log"
	}

	delayedAfter, err := parseDurationEnv("TASKFORGE_DEMO_DELAYED_AFTER", 3*time.Second)
	if err != nil {
		return demoSettings{}, err
	}
	recurringEvery, err := parseDurationEnv("TASKFORGE_DEMO_RECURRING_EVERY", 2*time.Second)
	if err != nil {
		return demoSettings{}, err
	}
	runFor, err := parseDurationEnv("TASKFORGE_DEMO_RUN_FOR", 8*time.Second)
	if err != nil {
		return demoSettings{}, err
	}

	return demoSettings{
		OutputFile:     outputFile,
		DelayedAfter:   delayedAfter,
		RecurringEvery: recurringEvery,
		RunFor:         runFor,
	}, nil
}

func parseDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: parse duration: %w", key, err)
	}
	return parsed, nil
}

func publishDelayedDemoTask(ctx context.Context, brokerInstance broker.Broker, queue string, settings demoSettings) error {
	payload, err := json.Marshal(demo.AppendFilePayload{
		Path: settings.OutputFile,
		Line: "delayed hello from scheduler",
	})
	if err != nil {
		return err
	}
	eta := time.Now().UTC().Add(settings.DelayedAfter)
	_, err = brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:        uuid.NewString(),
		Name:      demo.TaskAppendFile,
		Queue:     queue,
		Payload:   payload,
		ETA:       &eta,
		CreatedAt: time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew})
	return err
}

func publishStartupTasks(ctx context.Context, brokerInstance broker.Broker, settings demoSettings, pools []config.WorkerPoolConfig) error {
	for _, pool := range pools {
		payload, err := json.Marshal(demo.AppendFilePayload{
			Path: settings.OutputFile,
			Line: fmt.Sprintf("startup hello from pool=%s queue=%s", pool.Name, pool.Queue),
		})
		if err != nil {
			return err
		}
		if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
			ID:        uuid.NewString(),
			Name:      demo.TaskAppendFile,
			Queue:     pool.Queue,
			Payload:   payload,
			CreatedAt: time.Now().UTC(),
		}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			return err
		}
	}
	return nil
}

func buildRecurringSchedule(queue string, settings demoSettings) schedulerpkg.ScheduleDefinition {
	payload, _ := json.Marshal(demo.AppendFilePayload{
		Path: settings.OutputFile,
		Line: "recurring hello from scheduler",
	})
	startAt := time.Now().UTC().Add(settings.RecurringEvery)
	return schedulerpkg.ScheduleDefinition{
		ID:            "demo-recurring-append-file",
		Interval:      settings.RecurringEvery,
		Queue:         queue,
		TaskName:      demo.TaskAppendFile,
		Payload:       payload,
		Enabled:       true,
		MisfirePolicy: schedulerpkg.MisfirePolicyCoalesce,
		StartAt:       &startAt,
	}
}
