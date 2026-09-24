package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
	redisclient "github.com/redis/go-redis/v9"
)

type benchmarkCommandCounter struct {
	trips    atomic.Int64
	commands atomic.Int64
}

func (c *benchmarkCommandCounter) DialHook(next redisclient.DialHook) redisclient.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}

func (c *benchmarkCommandCounter) ProcessHook(next redisclient.ProcessHook) redisclient.ProcessHook {
	return func(ctx context.Context, cmd redisclient.Cmder) error {
		c.trips.Add(1)
		c.commands.Add(1)
		return next(ctx, cmd)
	}
}

func (c *benchmarkCommandCounter) ProcessPipelineHook(next redisclient.ProcessPipelineHook) redisclient.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redisclient.Cmder) error {
		c.trips.Add(1)
		c.commands.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

func benchmarkBroker(b *testing.B) (*Broker, context.Context, string) {
	b.Helper()
	if os.Getenv("TASKFORGE_RUN_BENCHMARKS") != "1" {
		b.Skip("set TASKFORGE_RUN_BENCHMARKS=1")
	}
	addr := os.Getenv("TASKFORGE_REDIS_ADDR")
	if addr == "" {
		b.Fatal("TASKFORGE_REDIS_ADDR is required for Redis benchmarks")
	}
	rawDB := os.Getenv("TASKFORGE_REDIS_DB")
	if rawDB == "" {
		b.Fatal("TASKFORGE_REDIS_DB is required for Redis benchmarks")
	}
	db, err := strconv.Atoi(rawDB)
	if err != nil {
		b.Fatalf("parse TASKFORGE_REDIS_DB: %v", err)
	}
	if db <= 0 {
		b.Fatal("TASKFORGE_REDIS_DB must be a non-zero dedicated database")
	}
	ctx := context.Background()
	client := redisclient.NewClient(&redisclient.Options{Addr: addr, DB: db})
	if err := client.Ping(ctx).Err(); err != nil {
		b.Fatalf("Redis benchmark endpoint %s/%d unavailable: %v", addr, db, err)
	}
	b.Cleanup(func() { _ = client.Close() })
	queue := fmt.Sprintf("benchmark-%d", time.Now().UnixNano())
	broker := New(Options{Client: client, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), LeaseTTL: time.Hour, ReserveTimeout: time.Millisecond})
	b.Cleanup(func() {
		_ = client.Unlink(ctx, broker.streamKey(queue), broker.leaseDeadlineKey(broker.streamKey(queue))).Err()
	})
	return broker, ctx, queue
}

func BenchmarkRedisPublishKeyed(b *testing.B) {
	broker, ctx, queue := benchmarkBroker(b)
	keys := make([]string, 0, b.N*2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("%s-%d", queue, i)
		key := "benchmark:" + id
		_, err := broker.Publish(ctx, taskforge.Task{ID: id, Name: "benchmark.publish", Queue: queue}, taskforge.PublishOptions{DeduplicationKey: key})
		if err != nil {
			b.Fatal(err)
		}
		keys = append(keys, broker.publishReceiptKey(key), broker.stateStore.(*stateStore).taskKey(id))
	}
	b.StopTimer()
	if len(keys) > 0 {
		_ = broker.client.Unlink(ctx, keys...).Err()
	}
}

func BenchmarkRedisRenewLease(b *testing.B) {
	broker, ctx, queue := benchmarkBroker(b)
	id := queue + "-lease"
	if _, err := broker.Publish(ctx, taskforge.Task{ID: id, Name: "benchmark.renew", Queue: queue}, taskforge.PublishOptions{}); err != nil {
		b.Fatal(err)
	}
	delivery, err := broker.Reserve(ctx, queue, "owner")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = broker.client.Unlink(ctx, broker.stateStore.(*stateStore).taskKey(id)).Err() })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := broker.ExtendLease(ctx, delivery, time.Hour); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisReclaimHealthy1000(b *testing.B) {
	broker, ctx, queue := benchmarkBroker(b)
	stream := broker.streamKey(queue)
	group := broker.groupName(queue)
	if err := broker.ensureGroup(ctx, stream, group); err != nil {
		b.Fatal(err)
	}
	pipe := broker.client.Pipeline()
	for i := range 1000 {
		payload, err := json.Marshal(taskforge.Task{ID: fmt.Sprintf("%s-%d", queue, i), Name: "benchmark.reclaim", Queue: queue})
		if err != nil {
			b.Fatal(err)
		}
		pipe.XAdd(ctx, &redisclient.XAddArgs{Stream: stream, Values: map[string]any{streamPayloadField: string(payload)}})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		b.Fatal(err)
	}
	entries, err := broker.client.XReadGroup(ctx, &redisclient.XReadGroupArgs{Group: group, Consumer: "owner", Streams: []string{stream, ">"}, Count: 1000, Block: time.Millisecond}).Result()
	if err != nil || len(entries) != 1 || len(entries[0].Messages) != 1000 {
		b.Fatalf("prepare pending entries: count=%d, error=%v", len(entries), err)
	}
	reserved := make([]taskforge.Delivery, 0, len(entries[0].Messages))
	for _, entry := range entries[0].Messages {
		msg, err := decodeTask(entry)
		if err != nil {
			b.Fatal(err)
		}
		reserved = append(reserved, newDelivery(msg, queue, "owner", entry.ID, time.Now(), broker.effectiveLeaseTTL(msg), 1))
	}
	broker.recordLeaseDeadlines(ctx, stream, reserved)
	counter := &benchmarkCommandCounter{}
	broker.client.AddHook(counter)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, found, err := broker.reclaimExpiredDelivery(ctx, queue, stream, group, "other-owner")
		if err != nil || found {
			b.Fatalf("reclaim healthy pending: found=%v, error=%v", found, err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(counter.trips.Load())/float64(b.N), "roundtrips/op")
	b.ReportMetric(float64(counter.commands.Load())/float64(b.N), "commands/op")
}

func BenchmarkRedisTaskCycle(b *testing.B) {
	for _, mode := range []StateMode{StateModeFull, StateModeDeliveryOnly} {
		b.Run(string(mode), func(b *testing.B) {
			base, ctx, queue := benchmarkBroker(b)
			broker := New(Options{Client: base.client, Logger: base.logger, LeaseTTL: time.Hour, ReserveTimeout: time.Millisecond, StateMode: mode})
			keys := make([]string, 0, b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := fmt.Sprintf("%s-%d", queue, i)
				if _, err := broker.Publish(ctx, taskforge.Task{ID: id, Name: "benchmark.cycle", Queue: queue}, taskforge.PublishOptions{}); err != nil {
					b.Fatal(err)
				}
				delivery, err := broker.Reserve(ctx, queue, "owner")
				if err != nil {
					b.Fatal(err)
				}
				if err := broker.RecordDelivery(ctx, delivery, taskforge.StateLeased, nil); err != nil {
					b.Fatal(err)
				}
				if err := broker.RecordDelivery(ctx, delivery, taskforge.StateRunning, nil); err != nil {
					b.Fatal(err)
				}
				if err := broker.AckAndRecord(ctx, delivery, taskforge.StateSucceeded); err != nil {
					b.Fatal(err)
				}
				if mode == StateModeFull {
					keys = append(keys, broker.stateStore.(*stateStore).taskKey(id))
				}
			}
			b.StopTimer()
			if len(keys) > 0 {
				_ = broker.client.Unlink(ctx, keys...).Err()
			}
		})
	}
}

func BenchmarkRedisRenew64(b *testing.B) {
	broker, ctx, queue := benchmarkBroker(b)
	for i := range 64 {
		id := fmt.Sprintf("%s-%d", queue, i)
		if _, err := broker.Publish(ctx, taskforge.Task{ID: id, Name: "benchmark.renew64", Queue: queue}, taskforge.PublishOptions{}); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = broker.client.Unlink(ctx, broker.stateStore.(*stateStore).taskKey(id)).Err() })
	}
	deliveries, err := broker.ReserveBatch(ctx, queue, "owner", 64)
	if err != nil || len(deliveries) != 64 {
		b.Fatalf("ReserveBatch() count = %d, error = %v", len(deliveries), err)
	}
	renewals := make([]taskforge.Delivery, 64)
	copy(renewals, deliveries)
	b.Run("legacy_scalar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, delivery := range renewals {
				if err := legacyExtendLease(ctx, broker, delivery); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("batch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			errs, err := broker.ExtendLeases(ctx, renewals)
			if err != nil {
				b.Fatal(err)
			}
			for _, itemErr := range errs {
				if itemErr != nil {
					b.Fatal(itemErr)
				}
			}
		}
	})
}

func legacyExtendLease(ctx context.Context, broker *Broker, delivery taskforge.Delivery) error {
	pending, _, err := broker.validatePendingDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	queue := taskforge.EffectiveQueue(delivery.Message)
	ids, err := broker.client.XClaimJustID(ctx, &redisclient.XClaimArgs{
		Stream:   broker.queueStreamKey(queue, delivery.Message.FairnessKey),
		Group:    broker.groupName(queue),
		Consumer: pending.Consumer,
		MinIdle:  0,
		Messages: []string{delivery.Execution.DeliveryID},
	}).Result()
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return taskforge.ErrUnknownDelivery
	}
	return nil
}
