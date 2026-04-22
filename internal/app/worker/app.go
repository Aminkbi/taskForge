package worker

import (
	"context"
	"log/slog"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/brokerredis"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/dlq"
	"github.com/aminkbi/taskforge/internal/healthcheck"
	"github.com/aminkbi/taskforge/internal/httpserver"
	"github.com/aminkbi/taskforge/internal/observability"
	runtimepkg "github.com/aminkbi/taskforge/internal/runtime"
)

type App struct {
	server *httpserver.Server
	worker *runtimepkg.Manager
}

func New(cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) *App {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	fairnessPolicies := config.FairnessPoliciesByQueue(cfg.WorkerPools)
	admissionPolicies := admissionPoliciesByQueue(cfg.WorkerPools)
	sharedBroker := brokerredis.NewWithOptions(client, logger.With("component", "brokerredis"), cfg.WorkerPools[0].LeaseTTL, metrics, brokerredis.Options{
		FairnessPolicies:  fairnessPolicies,
		AdmissionPolicies: admissionPolicies,
		DependencyBudgets: dependencyBudgetCapacities(cfg.DependencyBudgets),
	})
	dispatcher := dlq.NewService(client, sharedBroker, logger.With("component", "dlq"))

	globalLimiter := runtimepkg.NewTaskTypeLimiter(cfg.TaskTypeLimits)
	workers := make([]*runtimepkg.Worker, 0, len(cfg.WorkerPools))
	queues := make([]string, 0, len(cfg.WorkerPools))
	recoveryChecks := make(map[string]*healthcheck.Reporter, len(cfg.WorkerPools))
	for _, pool := range cfg.WorkerPools {
		poolBroker := brokerredis.NewWithOptions(client, logger.With("component", "brokerredis", "pool", pool.Name, "queue", pool.Queue), pool.LeaseTTL, metrics, brokerredis.Options{
			FairnessPolicies:  fairnessPolicies,
			AdmissionPolicies: admissionPolicies,
			DependencyBudgets: dependencyBudgetCapacities(cfg.DependencyBudgets),
		})
		queues = append(queues, pool.Queue)
		recoveryHealth := healthcheck.NewReporter("not_ready", "worker starting")
		recoveryChecks[pool.Name] = recoveryHealth
		workers = append(workers, &runtimepkg.Worker{
			Broker:            poolBroker,
			DeadLetter:        dispatcher,
			Handler:           runtimepkg.HandlerFunc(func(context.Context, broker.TaskMessage) error { return nil }),
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
			RecoveryHealth:    recoveryHealth,
			GlobalTaskLimiter: globalLimiter,
			PoolTaskLimiter:   runtimepkg.NewTaskTypeLimiter(pool.TaskTypeLimits),
			BudgetManager:     sharedBroker.BudgetManager(),
			TaskBudgets:       runtimeTaskBudgets(cfg.TaskBudgets),
			QueueMetrics:      poolBroker,
			Adaptive:          runtimeAdaptiveConfig(pool.Adaptive),
			AdaptiveStore:     sharedBroker.AdaptiveStateStore(),
			LifecycleWriter:   sharedBroker.WorkerLifecycleStore(),
		})
	}
	manager := &runtimepkg.Manager{
		Workers:         workers,
		ShutdownTimeout: cfg.ShutdownTimeout,
	}
	_ = metrics.RegisterQueueMetricsCollector(sharedBroker, queues)
	_ = metrics.RegisterFairnessMetricsCollector(sharedBroker, queues)
	_ = metrics.RegisterDeadLetterMetricsCollector(sharedBroker, queues)
	_ = metrics.RegisterAdmissionStatusCollector(sharedBroker, queues)
	_ = metrics.RegisterDependencyBudgetCollector(sharedBroker)
	_ = metrics.RegisterWorkerLifecycleCollector(manager)
	server := httpserver.New(cfg.HTTPAddr, logger.With("component", "httpserver"), metrics.Handler(), map[string]httpserver.CheckFunc{
		"redis": func(ctx context.Context) httpserver.CheckResult {
			if err := sharedBroker.Ping(ctx); err != nil {
				return httpserver.CheckResult{
					Status: "failed",
					Detail: err.Error(),
				}
			}
			return httpserver.CheckResult{
				Ready:  true,
				Status: "ready",
				Detail: "redis reachable",
			}
		},
		"recovery_loop": func(context.Context) httpserver.CheckResult {
			unhealthy := make([]string, 0)
			for poolName, reporter := range recoveryChecks {
				snapshot := reporter.Snapshot()
				if snapshot.Ready {
					continue
				}
				status := snapshot.Status
				if snapshot.Detail != "" {
					status = status + ":" + snapshot.Detail
				}
				unhealthy = append(unhealthy, poolName+"="+status)
			}
			if len(unhealthy) > 0 {
				return httpserver.CheckResult{
					Status: "failed",
					Detail: strings.Join(unhealthy, ","),
				}
			}
			return httpserver.CheckResult{
				Ready:  true,
				Status: "ready",
				Detail: "worker reserve and reclaim loops healthy",
			}
		},
		"worker_lifecycle": func(context.Context) httpserver.CheckResult {
			unavailable := make([]string, 0)
			for _, worker := range workers {
				snapshot, ok := worker.LifecycleSnapshot()
				if !ok {
					continue
				}
				if snapshot.State == "accepting" {
					continue
				}
				unavailable = append(unavailable, snapshot.Pool+"="+snapshot.State)
			}
			if len(unavailable) > 0 {
				return httpserver.CheckResult{
					Status: "not_ready",
					Detail: strings.Join(unavailable, ","),
				}
			}
			return httpserver.CheckResult{
				Ready:  true,
				Status: "ready",
				Detail: "workers accepting new deliveries",
			}
		},
	}, nil)

	return &App{
		server: server,
		worker: manager,
	}
}

func runtimeAdaptiveConfig(cfg config.AdaptiveConcurrencyConfig) runtimepkg.AdaptiveConfig {
	return runtimepkg.AdaptiveConfig{
		Enabled:                cfg.Enabled,
		MinConcurrency:         cfg.MinConcurrency,
		MaxConcurrency:         cfg.MaxConcurrency,
		ControlPeriod:          cfg.ControlPeriod,
		Cooldown:               cfg.Cooldown,
		ScaleUpStep:            cfg.ScaleUpStep,
		ScaleDownStep:          cfg.ScaleDownStep,
		LatencyThreshold:       cfg.LatencyThreshold,
		ErrorRateThreshold:     cfg.ErrorRateThreshold,
		BacklogThreshold:       cfg.BacklogThreshold,
		HealthyWindowsRequired: cfg.HealthyWindowsRequired,
	}
}

func runtimeTaskBudgets(cfg map[string]config.TaskBudgetConfig) map[string]runtimepkg.TaskBudget {
	if len(cfg) == 0 {
		return nil
	}
	budgets := make(map[string]runtimepkg.TaskBudget, len(cfg))
	for taskName, budget := range cfg {
		budgets[taskName] = runtimepkg.TaskBudget{
			Budget: budget.Budget,
			Tokens: budget.Tokens,
		}
	}
	return budgets
}

func dependencyBudgetCapacities(cfg map[string]config.DependencyBudgetConfig) map[string]int {
	if len(cfg) == 0 {
		return nil
	}
	capacities := make(map[string]int, len(cfg))
	for name, budget := range cfg {
		capacities[name] = budget.Capacity
	}
	return capacities
}

func admissionPoliciesByQueue(pools []config.WorkerPoolConfig) map[string]brokerredis.AdmissionPolicy {
	policies := make(map[string]brokerredis.AdmissionPolicy)
	for queue, policy := range config.AdmissionPoliciesByQueue(pools) {
		policies[queue] = brokerredis.AdmissionPolicy{
			Mode:                     brokerredis.AdmissionMode(policy.Mode),
			MaxPending:               policy.MaxPending,
			MaxPendingPerFairnessKey: policy.MaxPendingPerFairnessKey,
			MaxOldestReadyAge:        policy.MaxOldestReadyAge,
			MaxRetryBacklog:          policy.MaxRetryBacklog,
			MaxDeadLetterSize:        policy.MaxDeadLetterSize,
			DeferInterval:            policy.DeferInterval,
		}
	}
	if len(policies) == 0 {
		return nil
	}
	return policies
}

func (a *App) Run(ctx context.Context) error {
	a.server.SetReady(true)

	errCh := make(chan error, 2)
	serverCtx, cancelServer := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelServer()
	go func() {
		errCh <- a.server.Run(serverCtx)
	}()
	go func() {
		errCh <- a.worker.Run(ctx)
	}()

	for i := 0; i < 2; i++ {
		err := <-errCh
		if err == nil {
			cancelServer()
			continue
		}
		cancelServer()
		return err
	}
	return nil
}
