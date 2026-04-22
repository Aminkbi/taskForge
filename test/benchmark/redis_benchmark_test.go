package benchmark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/brokerredis"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/dlq"
	"github.com/aminkbi/taskforge/internal/runtime"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
	"github.com/aminkbi/taskforge/internal/tasks"
)

const (
	benchReserveTimeout = 10 * time.Millisecond
	benchRedisDB        = 14
)

type benchEnv struct {
	ctx    context.Context
	cancel context.CancelFunc
	client *redis.Client
	broker *brokerredis.RedisBroker
	logger *slog.Logger
}

func benchLeadershipFence(owner string, epoch int64) schedulerpkg.LeadershipFence {
	return schedulerpkg.LeadershipFence{
		Owner: owner,
		Epoch: epoch,
		Token: fmt.Sprintf("%s|%d", owner, epoch),
	}
}

func setBenchLeadership(tb testing.TB, client *redis.Client, fence schedulerpkg.LeadershipFence, ttl time.Duration) {
	tb.Helper()
	if err := client.Set(context.Background(), "taskforge:scheduler:leader", fence.Token, ttl).Err(); err != nil {
		tb.Fatalf("Set() scheduler leadership error = %v", err)
	}
	if err := client.Set(context.Background(), "taskforge:scheduler:leader:epoch", fence.Epoch, 0).Err(); err != nil {
		tb.Fatalf("Set() scheduler leadership epoch error = %v", err)
	}
}

func BenchmarkPublishThroughput(b *testing.B) {
	env := newBenchEnv(b, 30*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := env.broker.Publish(env.ctx, benchmarkMessage("publish", i), broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			b.Fatalf("Publish() error = %v", err)
		}
	}
}

func BenchmarkReserveAckThroughput(b *testing.B) {
	env := newBenchEnv(b, 30*time.Second)
	for i := 0; i < b.N; i++ {
		if _, err := env.broker.Publish(env.ctx, benchmarkMessage("reserve", i), broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			b.Fatalf("Publish() error = %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		delivery, err := env.broker.Reserve(env.ctx, "default", "bench-reserve")
		if err != nil {
			b.Fatalf("Reserve() error = %v", err)
		}
		if err := env.broker.Ack(env.ctx, delivery); err != nil {
			b.Fatalf("Ack() error = %v", err)
		}
	}
}

func BenchmarkEndToEndLatency(b *testing.B) {
	env := newBenchEnv(b, 30*time.Second)
	done := &sync.Map{}
	worker := newBenchWorker(env.broker, nil, runtime.HandlerFunc(func(_ context.Context, msg broker.TaskMessage) error {
		value, ok := done.Load(msg.ID)
		if ok {
			close(value.(chan struct{}))
		}
		return nil
	}), 30*time.Second)

	managerCtx, managerCancel := context.WithCancel(env.ctx)
	defer managerCancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- (&runtime.Manager{Workers: []*runtime.Worker{worker}}).Run(managerCtx)
	}()

	var total time.Duration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		taskID := fmt.Sprintf("e2e-%d", i)
		doneCh := make(chan struct{})
		done.Store(taskID, doneCh)

		msg := benchmarkMessage("e2e", i)
		msg.ID = taskID
		start := time.Now()
		if _, err := env.broker.Publish(env.ctx, msg, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			b.Fatalf("Publish() error = %v", err)
		}

		select {
		case <-doneCh:
			total += time.Since(start)
		case <-time.After(2 * time.Second):
			b.Fatalf("timed out waiting for task %s", taskID)
		}
		done.Delete(taskID)
	}
	b.StopTimer()

	managerCancel()
	if err := <-errCh; err != nil {
		b.Fatalf("worker manager error = %v", err)
	}
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "ns/e2e")
}

func BenchmarkReclaimLatencyAfterWorkerDeath(b *testing.B) {
	env := newBenchEnv(b, 20*time.Millisecond)
	var total time.Duration

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := env.broker.Publish(env.ctx, benchmarkMessage("reclaim", i), broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			b.Fatalf("Publish() error = %v", err)
		}
		if _, err := env.broker.Reserve(env.ctx, "default", "bench-dead-worker"); err != nil {
			b.Fatalf("Reserve() first error = %v", err)
		}

		time.Sleep(30 * time.Millisecond)
		start := time.Now()
		delivery, err := env.broker.Reserve(env.ctx, "default", "bench-reclaimer")
		if err != nil {
			b.Fatalf("Reserve() reclaim error = %v", err)
		}
		total += time.Since(start)
		if err := env.broker.Ack(env.ctx, delivery); err != nil {
			b.Fatalf("Ack() error = %v", err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "ns/reclaim")
}

func BenchmarkSchedulerReleaseLag(b *testing.B) {
	env := newBenchEnv(b, 30*time.Second)
	scheduler := schedulerpkg.New(
		env.broker,
		nil,
		schedulerpkg.NewRedisLeaderElector(env.client, clock.RealClock{}, env.logger, "bench-scheduler", 100*time.Millisecond, 25*time.Millisecond),
		clock.RealClock{},
		env.logger,
		nil,
		5*time.Millisecond,
		25*time.Millisecond,
	)

	schedulerCtx, schedulerCancel := context.WithCancel(env.ctx)
	defer schedulerCancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- scheduler.Run(schedulerCtx)
	}()

	var total time.Duration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eta := time.Now().UTC().Add(25 * time.Millisecond)
		msg := benchmarkMessage("scheduler", i)
		msg.ETA = &eta
		if _, err := env.broker.Publish(env.ctx, msg, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			b.Fatalf("Publish() delayed error = %v", err)
		}

		for {
			delivery, err := env.broker.Reserve(env.ctx, "default", "bench-scheduler-worker")
			if err == nil {
				total += time.Since(eta)
				if err := env.broker.Ack(env.ctx, delivery); err != nil {
					b.Fatalf("Ack() error = %v", err)
				}
				break
			}
			if !errors.Is(err, broker.ErrNoTask) {
				b.Fatalf("Reserve() error = %v", err)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	b.StopTimer()

	schedulerCancel()
	if err := <-errCh; err != nil {
		b.Fatalf("scheduler error = %v", err)
	}
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "ns/scheduler_lag")
}

func BenchmarkRecurringSchedulerTickScaling(b *testing.B) {
	dueAt := time.Date(2026, 4, 15, 10, 0, 0, 123000000, time.UTC)
	warmupNow := dueAt.Add(-time.Hour)
	dispatchNow := dueAt.Add(250 * time.Millisecond)

	for _, totalSchedules := range recurringBenchmarkScheduleCounts() {
		b.Run(fmt.Sprintf("schedules_%d", totalSchedules), func(b *testing.B) {
			env := newBenchEnv(b, 30*time.Second)
			schedules := benchmarkRecurringSchedules(totalSchedules, 5, dueAt)

			b.StopTimer()
			for i := 0; i < b.N; i++ {
				if err := env.client.FlushDB(env.ctx).Err(); err != nil {
					b.Fatalf("FlushDB() error = %v", err)
				}

				recurring := schedulerpkg.NewRecurringService(
					env.broker,
					schedulerpkg.NewRedisScheduleStateStore(env.client),
					schedules,
					env.logger,
				)
				fence := benchLeadershipFence("bench-recurring", 1)
				setBenchLeadership(b, env.client, fence, time.Minute)

				if dispatched, err := recurring.SyncDue(env.ctx, fence, warmupNow); err != nil {
					b.Fatalf("warmup recurring SyncDue() error = %v", err)
				} else if dispatched != 0 {
					b.Fatalf("warmup recurring SyncDue() dispatched = %d, want 0", dispatched)
				}

				b.StartTimer()
				dispatched, err := recurring.SyncDue(env.ctx, fence, dispatchNow)
				b.StopTimer()
				if err != nil {
					b.Fatalf("recurring SyncDue() error = %v", err)
				}
				if dispatched != 5 {
					b.Fatalf("recurring SyncDue() dispatched = %d, want 5", dispatched)
				}
			}
		})
	}
}

func BenchmarkRetryStormThroughput(b *testing.B) {
	env := newBenchEnv(b, 30*time.Second)
	deadLetters := dlq.NewService(env.client, env.broker, env.logger)
	worker := newBenchWorker(env.broker, deadLetters, runtime.HandlerFunc(func(context.Context, broker.TaskMessage) error {
		return runtime.Retryable(errors.New("upstream unavailable"))
	}), 30*time.Second)
	worker.RetryPolicy = tasks.DefaultRetryPolicy(2)

	scheduler := schedulerpkg.New(
		env.broker,
		nil,
		schedulerpkg.NewRedisLeaderElector(env.client, clock.RealClock{}, env.logger, "bench-retry-storm", 100*time.Millisecond, 25*time.Millisecond),
		clock.RealClock{},
		env.logger,
		nil,
		5*time.Millisecond,
		25*time.Millisecond,
	)

	runCtx, cancel := context.WithCancel(env.ctx)
	defer cancel()
	workerErrCh := make(chan error, 1)
	go func() {
		workerErrCh <- (&runtime.Manager{Workers: []*runtime.Worker{worker}}).Run(runCtx)
	}()
	schedulerErrCh := make(chan error, 1)
	go func() {
		schedulerErrCh <- scheduler.Run(runCtx)
	}()

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		msg := benchmarkMessage("retry-storm", i)
		msg.Headers = map[string]string{
			tasks.HeaderRetryMaxDeliveries:  "2",
			tasks.HeaderRetryInitialBackoff: "5ms",
			tasks.HeaderRetryMaxBackoff:     "5ms",
			tasks.HeaderRetryMultiplier:     "1",
		}
		if _, err := env.broker.Publish(env.ctx, msg, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			b.Fatalf("Publish() error = %v", err)
		}
	}

	waitCtx, waitCancel := context.WithTimeout(env.ctx, 5*time.Second)
	defer waitCancel()
	for {
		entries, err := deadLetters.List(waitCtx, "default", int64(b.N))
		if err != nil {
			b.Fatalf("List() error = %v", err)
		}
		if len(entries) == b.N {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	cancel()
	if err := <-workerErrCh; err != nil {
		b.Fatalf("worker manager error = %v", err)
	}
	if err := <-schedulerErrCh; err != nil {
		b.Fatalf("scheduler error = %v", err)
	}
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "final_tasks/s")
}

func newBenchEnv(b *testing.B, leaseTTL time.Duration) *benchEnv {
	b.Helper()

	if os.Getenv("TASKFORGE_RUN_BENCHMARKS") != "1" {
		b.Skip("set TASKFORGE_RUN_BENCHMARKS=1 to run Redis benchmarks")
	}

	db := benchRedisDB
	if raw := os.Getenv("TASKFORGE_REDIS_DB"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			b.Fatalf("parse TASKFORGE_REDIS_DB: %v", err)
		}
		db = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	client := redis.NewClient(&redis.Options{
		Addr: envOrDefault("TASKFORGE_REDIS_ADDR", "localhost:6379"),
		DB:   db,
	})
	b.Cleanup(func() {
		cancel()
		_ = client.Close()
	})

	if err := client.Ping(ctx).Err(); err != nil {
		b.Skipf("redis unavailable: %v", err)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		b.Fatalf("FlushDB() error = %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &benchEnv{
		ctx:    ctx,
		cancel: cancel,
		client: client,
		broker: brokerredis.NewWithOptions(client, logger, leaseTTL, nil, brokerredis.Options{
			ReserveTimeout: benchReserveTimeout,
		}),
		logger: logger,
	}
}

func benchmarkMessage(prefix string, i int) broker.TaskMessage {
	payload, _ := json.Marshal(map[string]any{
		"index": i,
	})
	return broker.TaskMessage{
		ID:        fmt.Sprintf("%s-%d", prefix, i),
		Name:      "benchmark.task",
		Queue:     "default",
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
	}
}

func benchmarkRecurringSchedules(total, dueCount int, dueAt time.Time) []schedulerpkg.ScheduleDefinition {
	schedules := make([]schedulerpkg.ScheduleDefinition, 0, total)
	for i := 0; i < total; i++ {
		startAt := dueAt.Add(365 * 24 * time.Hour)
		if i < dueCount {
			startAt = dueAt
		}

		schedules = append(schedules, schedulerpkg.ScheduleDefinition{
			ID:            fmt.Sprintf("bench-recurring-%d", i),
			Interval:      time.Second,
			Queue:         "default",
			TaskName:      "benchmark.recurring",
			Payload:       json.RawMessage(fmt.Sprintf(`{"index":%d}`, i)),
			Enabled:       true,
			MisfirePolicy: schedulerpkg.MisfirePolicyCoalesce,
			StartAt:       &startAt,
		})
	}
	return schedules
}

func recurringBenchmarkScheduleCounts() []int {
	counts := []int{10, 1000}
	if os.Getenv("TASKFORGE_RUN_HEAVY_BENCHMARKS") == "1" {
		counts = append(counts, 100000)
	}
	return counts
}

func newBenchWorker(b broker.Broker, deadLetters dlq.Publisher, handler runtime.Handler, leaseTTL time.Duration) *runtime.Worker {
	return &runtime.Worker{
		Broker:      b,
		DeadLetter:  deadLetters,
		Handler:     handler,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Clock:       clock.RealClock{},
		Metrics:     nil,
		RetryPolicy: tasks.DefaultRetryPolicy(1),
		PoolName:    "bench",
		Queue:       "default",
		ConsumerID:  "bench-worker",
		LeaseTTL:    leaseTTL,
		Concurrency: 1,
		Prefetch:    1,
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
