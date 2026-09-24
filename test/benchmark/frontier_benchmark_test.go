package benchmark

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
	taskforgeredis "github.com/aminkbi/taskforge/redis"
	runtimepkg "github.com/aminkbi/taskforge/worker"
)

func BenchmarkControlPlaneCategories(b *testing.B) {
	b.Run("state_transition", func(b *testing.B) {
		env := newBenchEnv(b, 30*time.Second)
		deliveries := make([]taskforge.Delivery, b.N)
		for index := range deliveries {
			deliveries[index] = benchmarkDelivery(index)
		}
		measureRedisOperation(b, env, func(index int) {
			if err := env.broker.RecordDelivery(env.ctx, deliveries[index], taskforge.StateRunning, nil); err != nil {
				b.Fatal(err)
			}
		})
	})

	b.Run("budget_acquire_release", func(b *testing.B) {
		env := newBenchEnvWithOptions(b, 30*time.Second, taskforgeredis.Options{ReserveTimeout: benchReserveTimeout, DependencyBudgets: map[string]int{"api": max(b.N, 1)}})
		measureRedisOperation(b, env, func(index int) {
			key := fmt.Sprintf("budget-%d", index)
			acquired, err := env.broker.AcquireLease(env.ctx, "api", key, 1, time.Minute)
			if err != nil || !acquired {
				b.Fatalf("AcquireLease() = %v, %v", acquired, err)
			}
			if err := env.broker.ReleaseLease(env.ctx, "api", key); err != nil {
				b.Fatal(err)
			}
		})
	})

	b.Run("adaptive_persistence", func(b *testing.B) {
		env := newBenchEnv(b, 30*time.Second)
		measureRedisOperation(b, env, func(index int) {
			if err := env.broker.StoreAdaptiveStatus(env.ctx, taskforge.AdaptivePoolSnapshot{Pool: "bench", EffectiveConcurrency: float64(index%16 + 1)}); err != nil {
				b.Fatal(err)
			}
		})
	})

	b.Run("idle_scheduler_poll", func(b *testing.B) {
		env := newBenchEnv(b, 30*time.Second)
		fence := benchLeadershipFence("frontier-idle", 1)
		setBenchLeadership(b, env.client, fence, time.Minute)
		measureRedisOperation(b, env, func(int) {
			moved, err := env.broker.MoveDue(env.ctx, fence, time.Now().UTC(), 100)
			if err != nil || moved != 0 {
				b.Fatalf("MoveDue() = %d, %v", moved, err)
			}
		})
	})
}

func BenchmarkShortTaskReservationFeeder(b *testing.B) {
	for _, variant := range []struct {
		name  string
		batch bool
		mode  taskforgeredis.StateMode
	}{
		{name: "sequential", mode: taskforgeredis.StateModeFull},
		{name: "bounded_batch", batch: true, mode: taskforgeredis.StateModeFull},
		{name: "delivery_only", batch: true, mode: taskforgeredis.StateModeDeliveryOnly},
	} {
		b.Run(variant.name, func(b *testing.B) {
			env := newBenchEnvWithOptions(b, 30*time.Second, taskforgeredis.Options{ReserveTimeout: benchReserveTimeout, StateMode: variant.mode})
			for index := 0; index < b.N; index++ {
				if _, err := env.broker.Publish(env.ctx, benchmarkMessage("short-feeder", index), taskforge.PublishOptions{Source: taskforge.PublishSourceNew}); err != nil {
					b.Fatal(err)
				}
			}

			done := make(chan struct{})
			counter := &ackCounter{remaining: int64(b.N), done: done}
			base := &countingBroker{broker: env.broker, counter: counter}
			var broker taskforge.Broker = base
			if variant.batch {
				broker = &batchCountingBroker{countingBroker: base}
			}
			var running atomic.Int64
			var peak atomic.Int64
			var handlerNanos atomic.Int64
			worker, err := runtimepkg.New(runtimepkg.Options{
				Broker: broker, DeadLetter: env.broker, StateStore: env.broker,
				Handler: taskforge.HandlerFunc(func(context.Context, taskforge.Task) error {
					current := running.Add(1)
					for observed := peak.Load(); current > observed && !peak.CompareAndSwap(observed, current); observed = peak.Load() {
					}
					started := time.Now()
					time.Sleep(time.Millisecond)
					handlerNanos.Add(time.Since(started).Nanoseconds())
					running.Add(-1)
					return nil
				}),
				Logger: env.logger, Queue: "default", PoolName: "frontier", ConsumerID: "frontier",
				LeaseTTL: 30 * time.Second, Concurrency: 16, Prefetch: 32,
			})
			if err != nil {
				b.Fatal(err)
			}
			ctx, cancel := context.WithCancel(env.ctx)
			errCh := make(chan error, 1)
			go func() { errCh <- worker.Run(ctx) }()

			started := time.Now()
			b.ResetTimer()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				b.Fatal("short-task worker did not drain")
			}
			b.StopTimer()
			elapsed := time.Since(started)
			cancel()
			if err := <-errCh; err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "tasks/s")
			b.ReportMetric(float64(peak.Load()), "peak_concurrency")
			b.ReportMetric(float64(handlerNanos.Load())/float64(elapsed.Nanoseconds()), "avg_concurrency")
		})
	}
}

type ackCounter struct {
	remaining int64
	done      chan struct{}
	once      sync.Once
}

type countingBroker struct {
	broker  *taskforgeredis.Broker
	counter *ackCounter
}

func (b *countingBroker) Publish(ctx context.Context, task taskforge.Task, opts taskforge.PublishOptions) (taskforge.PublishResult, error) {
	return b.broker.Publish(ctx, task, opts)
}

func (b *countingBroker) Reserve(ctx context.Context, queue, consumerID string) (taskforge.Delivery, error) {
	return b.broker.Reserve(ctx, queue, consumerID)
}

func (b *countingBroker) Ack(ctx context.Context, delivery taskforge.Delivery) error {
	if err := b.broker.Ack(ctx, delivery); err != nil {
		return err
	}
	b.recordAck()
	return nil
}

func (b *countingBroker) Nack(ctx context.Context, delivery taskforge.Delivery, requeue bool) error {
	return b.broker.Nack(ctx, delivery, requeue)
}

func (b *countingBroker) ExtendLease(ctx context.Context, delivery taskforge.Delivery, ttl time.Duration) error {
	return b.broker.ExtendLease(ctx, delivery, ttl)
}

func (b *countingBroker) ExtendLeases(ctx context.Context, deliveries []taskforge.Delivery) ([]error, error) {
	return b.broker.ExtendLeases(ctx, deliveries)
}

func (b *countingBroker) StateWritesEnabled() bool {
	return b.broker.StateWritesEnabled()
}

func (b *countingBroker) OwnsStateStore(store taskforge.StateStore) bool {
	return b.broker.OwnsStateStore(store)
}

func (b *countingBroker) AckAndRecord(ctx context.Context, delivery taskforge.Delivery, state taskforge.State) error {
	if err := b.broker.AckAndRecord(ctx, delivery, state); err != nil {
		return err
	}
	b.recordAck()
	return nil
}

func (b *countingBroker) recordAck() {
	if atomic.AddInt64(&b.counter.remaining, -1) == 0 {
		b.counter.once.Do(func() { close(b.counter.done) })
	}
}

type batchCountingBroker struct{ *countingBroker }

func (b *batchCountingBroker) ReserveBatch(ctx context.Context, queue, consumerID string, max int) ([]taskforge.Delivery, error) {
	return b.broker.ReserveBatch(ctx, queue, consumerID, max)
}

type reserveBatchCapability interface {
	ReserveBatch(context.Context, string, string, int) ([]taskforge.Delivery, error)
}

type leaseBatchCapability interface {
	ExtendLeases(context.Context, []taskforge.Delivery) ([]error, error)
}

type stateWritesCapability interface {
	StateWritesEnabled() bool
}

func TestCountingBrokerMethodSetMatchesVariant(t *testing.T) {
	var broker taskforge.Broker = &countingBroker{}
	if _, ok := broker.(reserveBatchCapability); ok {
		t.Fatal("sequential counting broker unexpectedly advertises ReserveBatch")
	}
	if _, ok := broker.(leaseBatchCapability); !ok {
		t.Fatal("counting broker does not advertise batch lease renewal")
	}
	if _, ok := broker.(stateWritesCapability); !ok {
		t.Fatal("counting broker does not advertise state policy")
	}
	if _, ok := taskforge.Broker(&batchCountingBroker{countingBroker: &countingBroker{}}).(reserveBatchCapability); !ok {
		t.Fatal("batch counting broker does not advertise ReserveBatch")
	}
}

func benchmarkDelivery(index int) taskforge.Delivery {
	now := time.Now().UTC()
	msg := benchmarkMessage("state", index)
	return taskforge.Delivery{Message: msg, Execution: taskforge.ExecutionMetadata{
		TaskID: msg.ID, DeliveryID: fmt.Sprintf("%d-0", index+1), DeliveryCount: 1,
		FirstEnqueuedAt: now, LeasedAt: now, LeaseExpiresAt: now.Add(time.Minute), LeaseOwner: "bench", State: taskforge.StateLeased,
	}}
}

func measureRedisOperation(b *testing.B, env *benchEnv, operation func(index int)) {
	b.Helper()
	env.redisStats = &redisRoundTripCounter{}
	env.client.AddHook(env.redisStats)
	before := redisNetworkBytes(b, env)
	env.redisStats.reset()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		operation(index)
	}
	b.StopTimer()
	commands := env.redisStats.commands.Load()
	roundTrips := env.redisStats.roundTrips.Load()
	after := redisNetworkBytes(b, env)
	b.ReportMetric(float64(commands)/float64(b.N), "redis_commands/op")
	b.ReportMetric(float64(roundTrips)/float64(b.N), "redis_round_trips/op")
	b.ReportMetric(float64(after-before)/float64(b.N), "redis_network_bytes/op")
}

func redisNetworkBytes(b *testing.B, env *benchEnv) int64 {
	b.Helper()
	info, err := env.client.Info(env.ctx, "stats").Result()
	if err != nil {
		b.Fatal(err)
	}
	var total int64
	for _, line := range strings.Split(info, "\n") {
		for _, key := range []string{"total_net_input_bytes:", "total_net_output_bytes:"} {
			if strings.HasPrefix(line, key) {
				value, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, key)), 10, 64)
				if err != nil {
					b.Fatal(err)
				}
				total += value
			}
		}
	}
	return total
}
