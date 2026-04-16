package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/brokerredis"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/healthcheck"
	"github.com/aminkbi/taskforge/internal/httpserver"
	"github.com/aminkbi/taskforge/internal/observability"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
)

type App struct {
	server    *httpserver.Server
	scheduler *schedulerpkg.Scheduler
}

func New(cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) *App {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	fairnessPolicies := config.FairnessPoliciesByQueue(cfg.WorkerPools)
	admissionPolicies := admissionPoliciesByQueue(cfg.WorkerPools)
	leaseTTL := config.DefaultLeaseTTL()
	if len(cfg.WorkerPools) > 0 {
		leaseTTL = cfg.WorkerPools[0].LeaseTTL
	}
	b := brokerredis.NewWithOptions(client, logger.With("component", "brokerredis"), leaseTTL, metrics, brokerredis.Options{
		FairnessPolicies:  fairnessPolicies,
		AdmissionPolicies: admissionPolicies,
		DependencyBudgets: dependencyBudgetCapacities(cfg.DependencyBudgets),
	})
	store := schedulerpkg.NewRedisScheduleStateStore(client)
	elector := schedulerpkg.NewRedisLeaderElector(
		client,
		clock.RealClock{},
		logger.With("component", "scheduler-leader"),
		schedulerOwnerToken(cfg.ServiceName),
		cfg.SchedulerLockTTL,
		cfg.SchedulerRenewInterval,
	)
	loopHealth := healthcheck.NewReporter("not_ready", "scheduler starting")
	recurring := schedulerpkg.NewRecurringService(
		b,
		store,
		cfg.RecurringSchedules,
		logger.With("component", "scheduler-recurring"),
	)
	queues := make([]string, 0, len(cfg.WorkerPools))
	for _, pool := range cfg.WorkerPools {
		queues = append(queues, pool.Queue)
	}
	_ = metrics.RegisterQueueMetricsCollector(b, queues)
	_ = metrics.RegisterFairnessMetricsCollector(b, queues)
	_ = metrics.RegisterSchedulerLagCollector(b, queues)
	_ = metrics.RegisterAdmissionStatusCollector(b, queues)
	_ = metrics.RegisterDependencyBudgetCollector(b)
	server := httpserver.New(cfg.HTTPAddr, logger.With("component", "httpserver"), metrics.Handler(), map[string]httpserver.CheckFunc{
		"redis": func(ctx context.Context) httpserver.CheckResult {
			if err := b.Ping(ctx); err != nil {
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
		"scheduler_leadership": func(context.Context) httpserver.CheckResult {
			snapshot := elector.Snapshot()
			if snapshot.Leader {
				return httpserver.CheckResult{
					Ready:  true,
					Status: "ready",
					Detail: "leader",
					Leader: true,
				}
			}
			return httpserver.CheckResult{
				Ready:  true,
				Status: "ready",
				Detail: "standby",
				Leader: false,
			}
		},
		"recovery_loop": func(context.Context) httpserver.CheckResult {
			snapshot := loopHealth.Snapshot()
			return httpserver.CheckResult{
				Ready:     snapshot.Ready,
				Status:    snapshot.Status,
				Detail:    snapshot.Detail,
				UpdatedAt: snapshot.UpdatedAt,
			}
		},
	}, nil)

	schedulerRuntime := schedulerpkg.New(
		b,
		recurring,
		elector,
		clock.RealClock{},
		logger.With("component", "scheduler-runtime"),
		cfg.PollInterval,
		cfg.SchedulerRenewInterval,
	)
	schedulerRuntime.LoopHealth = loopHealth

	return &App{
		server:    server,
		scheduler: schedulerRuntime,
	}
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
	go func() {
		errCh <- a.server.Run(ctx)
	}()
	go func() {
		errCh <- a.scheduler.Run(ctx)
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}

func schedulerOwnerToken(serviceName string) string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s:%s:%d", serviceName, hostname, os.Getpid())
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
