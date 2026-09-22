package integration

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
	taskforgeredis "github.com/aminkbi/taskforge/redis"
	"github.com/redis/go-redis/v9"
)

type commandCounter struct {
	mu       sync.Mutex
	commands []string
	trips    int
}

func (c *commandCounter) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (c *commandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		c.record([]redis.Cmder{cmd})
		return next(ctx, cmd)
	}
}
func (c *commandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error { c.record(cmds); return next(ctx, cmds) }
}
func (c *commandCounter) record(cmds []redis.Cmder) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trips++
	for _, cmd := range cmds {
		c.commands = append(c.commands, cmd.Name())
	}
}
func (c *commandCounter) reset() { c.mu.Lock(); defer c.mu.Unlock(); c.commands = nil; c.trips = 0 }
func (c *commandCounter) snapshot() ([]string, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.commands), c.trips
}

func TestRedisPublishCombinesQueuedState(t *testing.T) {
	for _, fair := range []bool{false, true} {
		for _, delayed := range []bool{false, true} {
			for _, dedup := range []bool{false, true} {
				t.Run(fmt.Sprintf("fair_%t/delayed_%t/dedup_%t", fair, delayed, dedup), func(t *testing.T) {
					ctx, _, client := newIntegrationBroker(t, time.Minute)
					options := taskforgeredis.Options{Client: client}
					if fair {
						options.FairnessPolicies = map[string]*taskforgeredis.FairnessPolicy{"default": mustFairnessPolicy(t, taskforgeredis.FairnessRule{}, nil)}
					}
					broker := taskforgeredis.New(options)
					counter := &commandCounter{}
					client.AddHook(counter)
					msg := taskforge.Task{ID: "warm", Name: "state", Queue: "default", FairnessKey: "tenant"}
					if delayed {
						eta := time.Now().Add(time.Hour)
						msg.ETA = &eta
					}
					opts := taskforge.PublishOptions{}
					if dedup {
						opts.DeduplicationKey = "warm-receipt"
					}
					if _, err := broker.Publish(ctx, msg, opts); err != nil {
						t.Fatal(err)
					}
					msg.ID = "state-target"
					if dedup {
						opts.DeduplicationKey = "target-receipt"
					}
					counter.reset()
					if _, err := broker.Publish(ctx, msg, opts); err != nil {
						t.Fatal(err)
					}
					commands, trips := counter.snapshot()
					want := 1
					if dedup {
						want++
					}
					if trips != want {
						t.Fatalf("publish trips = %d, want %d; commands %v", trips, want, commands)
					}
					record, err := broker.Get(ctx, msg.ID)
					if err != nil || record.State != taskforge.StateQueued {
						t.Fatalf("queued state = %+v, %v", record, err)
					}
					// A receipt hit must not reset a later state or extend its retention.
					key := "taskforge:v2:task:" + msg.ID
					if err := client.HSet(ctx, key, "state", string(taskforge.StateRunning)).Err(); err != nil {
						t.Fatal(err)
					}
					if err := client.PExpire(ctx, key, time.Minute).Err(); err != nil {
						t.Fatal(err)
					}
					result, err := broker.Publish(ctx, msg, opts)
					if err != nil {
						t.Fatal(err)
					}
					record, err = broker.Get(ctx, msg.ID)
					if err != nil {
						t.Fatal(err)
					}
					ttl, err := client.PTTL(ctx, key).Result()
					if err != nil {
						t.Fatal(err)
					}
					if dedup {
						if !result.Deduplicated || record.State != taskforge.StateRunning || ttl <= 0 {
							t.Fatalf("duplicate changed state/retention: %+v, %+v, %v", result, record, ttl)
						}
						if err := client.Del(ctx, receiptKeyForTest(opts.DeduplicationKey)).Err(); err != nil {
							t.Fatal(err)
						}
						// Receipt expiry permits a new publication and restores persistent queued state.
						if _, err := broker.Publish(ctx, msg, opts); err != nil {
							t.Fatal(err)
						}
						record, err = broker.Get(ctx, msg.ID)
						if err != nil {
							t.Fatal(err)
						}
						ttl, err = client.PTTL(ctx, key).Result()
						if err != nil {
							t.Fatal(err)
						}
					}
					if record.State != taskforge.StateQueued || ttl != -1 {
						t.Fatalf("new publication state/retention = %+v, %v", record, ttl)
					}
				})
			}
		}
	}
}

func TestRedisSnapshotCommandBudgets(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, time.Minute)
	policy := mustFairnessPolicy(t, taskforgeredis.FairnessRule{}, nil)
	broker := taskforgeredis.New(taskforgeredis.Options{Client: client,
		FairnessPolicies: map[string]*taskforgeredis.FairnessPolicy{"default": policy},
		AdmissionPolicies: map[string]taskforgeredis.AdmissionPolicy{"default": {
			Mode: taskforgeredis.AdmissionModeReject, MaxPending: 100, MaxOldestReadyAge: time.Hour,
		}},
	})
	const tenants = 3
	now := time.Now()
	for index := range tenants {
		if _, err := broker.Publish(ctx, taskforge.Task{ID: fmt.Sprint(index), Name: "snapshot", Queue: "default", FairnessKey: fmt.Sprint(index), Payload: make([]byte, 65536), CreatedAt: now.Add(-time.Minute)}, taskforge.PublishOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	counter := &commandCounter{}
	client.AddHook(counter)
	metrics, err := broker.QueueMetricsSnapshot(ctx, "default")
	if err != nil || metrics.Depth != tenants || metrics.Reserved != 0 || metrics.Consumers != 0 {
		t.Fatalf("metrics = %+v, %v", metrics, err)
	}
	commands, trips := counter.snapshot()
	if trips != 2 || len(commands) != 1+3*tenants || countCommand(commands, "xlen") != tenants {
		t.Fatalf("metrics commands = %v, trips %d", commands, trips)
	}
	counter.reset()
	status, err := broker.AdmissionStatusSnapshot(ctx, "default", now)
	if err != nil || status.QueuePending != tenants || status.OldestReadyAge != 60 {
		t.Fatalf("admission = %+v, %v", status, err)
	}
	commands, trips = counter.snapshot()
	if countCommand(commands, "smembers") != 1 || countCommand(commands, "xlen") != tenants || slices.Contains(commands, "xinfo") || trips != 2+2*tenants {
		t.Fatalf("admission commands = %v, trips %d", commands, trips)
	}
	// Wrong-type keys must remain errors; XLEN must not turn them into an empty queue.
	if err := client.Set(ctx, "taskforge:v2:stream:broken", "not a stream", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.QueueMetricsSnapshot(ctx, "broken"); err == nil {
		t.Fatal("wrong-type stream accepted")
	}
	empty, err := broker.QueueMetricsSnapshot(ctx, "missing")
	if err != nil || empty != (taskforge.QueueMetricsSnapshot{}) {
		t.Fatalf("missing stream = %+v, %v", empty, err)
	}
}

func countCommand(commands []string, name string) int {
	count := 0
	for _, command := range commands {
		if command == name {
			count++
		}
	}
	return count
}

func receiptKeyForTest(key string) string {
	return fmt.Sprintf("taskforge:v2:publish:receipt:%x", sha256.Sum256([]byte(key)))
}

type recordingStateStore struct {
	mu    sync.Mutex
	calls int
}

func (s *recordingStateStore) RecordQueued(context.Context, taskforge.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return nil
}
func (*recordingStateStore) RecordDelivery(context.Context, taskforge.Delivery, taskforge.State, []byte) error {
	return nil
}
func (*recordingStateStore) Get(context.Context, string) (taskforge.TaskRecord, error) {
	return taskforge.TaskRecord{}, nil
}

func TestRedisConcurrentPublishStateAndCustomFallback(t *testing.T) {
	for _, custom := range []bool{false, true} {
		for _, dedup := range []bool{false, true} {
			t.Run(fmt.Sprintf("custom_%t/dedup_%t", custom, dedup), func(t *testing.T) {
				ctx, _, client := newIntegrationBroker(t, time.Minute)
				store := &recordingStateStore{}
				options := taskforgeredis.Options{Client: client, FairnessPolicies: map[string]*taskforgeredis.FairnessPolicy{"default": mustFairnessPolicy(t, taskforgeredis.FairnessRule{}, nil)}}
				if custom {
					options.StateStore = store
				}
				broker := taskforgeredis.New(options)
				const publishers = 16
				errors := make(chan error, publishers)
				var wg sync.WaitGroup
				for range publishers {
					wg.Go(func() {
						opts := taskforge.PublishOptions{}
						if dedup {
							opts.DeduplicationKey = "concurrent-receipt"
						}
						_, err := broker.Publish(ctx, taskforge.Task{ID: "same-task", Name: "concurrent", Queue: "default"}, opts)
						errors <- err
					})
				}
				wg.Wait()
				close(errors)
				for err := range errors {
					if err != nil {
						t.Fatal(err)
					}
				}
				want := publishers
				if dedup {
					want = 1
				}
				metrics, err := broker.QueueMetricsSnapshot(ctx, "default")
				if err != nil || metrics.Depth != float64(want) {
					t.Fatalf("concurrent publications = %+v, %v, want %d", metrics, err, want)
				}
				if custom {
					if store.calls != want {
						t.Fatalf("custom RecordQueued calls = %d, want %d", store.calls, want)
					}
				} else {
					record, err := broker.Get(ctx, "same-task")
					if err != nil || record.State != taskforge.StateQueued {
						t.Fatalf("concurrent queued state = %+v, %v", record, err)
					}
				}
			})
		}
	}
}

func TestRedisWeightedSelectionPreservesSortedOrder(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, time.Minute)
	policy := mustFairnessPolicy(t, taskforgeredis.FairnessRule{}, []taskforgeredis.FairnessRule{
		{Name: "a", Keys: []string{"a"}, Weight: 2},
		{Name: "z", Keys: []string{"z"}, Weight: 1},
	})
	broker := taskforgeredis.New(taskforgeredis.Options{Client: client, FairnessPolicies: map[string]*taskforgeredis.FairnessPolicy{"default": policy}})
	for index := range 12 {
		for _, key := range []string{"z", "a"} {
			if _, err := broker.Publish(ctx, taskforge.Task{ID: fmt.Sprintf("%s-%d", key, index), Queue: "default", FairnessKey: key, Name: "weighted"}, taskforge.PublishOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for index := range 9 {
		delivery, err := broker.Reserve(ctx, "default", "weighted")
		if err != nil {
			t.Fatal(err)
		}
		want := "a"
		if index%3 == 2 {
			want = "z"
		}
		if delivery.Message.FairnessKey != want {
			t.Fatalf("selection %d = %q, want %q", index, delivery.Message.FairnessKey, want)
		}
		if err := broker.Ack(ctx, delivery); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRedisDuplicatePrecedesAdmissionAndDeferredCustomState(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, time.Minute)
	store := &recordingStateStore{}
	broker := taskforgeredis.New(taskforgeredis.Options{
		Client: client, StateStore: store,
		AdmissionPolicies: map[string]taskforgeredis.AdmissionPolicy{"default": {
			Mode: taskforgeredis.AdmissionModeDefer, MaxPending: 1, DeferInterval: time.Minute,
		}},
	})
	msg := taskforge.Task{ID: "custom-ready", Name: "custom", Queue: "default"}
	opts := taskforge.PublishOptions{DeduplicationKey: "custom-ready-receipt"}
	if _, err := broker.Publish(ctx, msg, opts); err != nil {
		t.Fatal(err)
	}
	duplicate, err := broker.Publish(ctx, msg, opts)
	if err != nil || !duplicate.Deduplicated || duplicate.Decision != taskforge.AdmissionDecisionAccepted {
		t.Fatalf("duplicate should bypass admission: %+v, %v", duplicate, err)
	}
	msg.ID = "custom-deferred"
	opts.DeduplicationKey = "custom-deferred-receipt"
	deferred, err := broker.Publish(ctx, msg, opts)
	if err != nil || deferred.Decision != taskforge.AdmissionDecisionDeferred {
		t.Fatalf("deferred publish = %+v, %v", deferred, err)
	}
	duplicate, err = broker.Publish(ctx, msg, opts)
	if err != nil || !duplicate.Deduplicated {
		t.Fatalf("deferred duplicate = %+v, %v", duplicate, err)
	}
	if store.calls != 2 {
		t.Fatalf("custom state writes = %d, want one per new publication", store.calls)
	}
	if count := client.ZCard(ctx, "taskforge:v2:delayed:queue:default").Val(); count != 1 {
		t.Fatalf("deferred entries = %d, want 1", count)
	}
}

func TestRedisFairPublishRejectsCorruptRegistryBeforeEnqueue(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, time.Minute)
	broker := taskforgeredis.New(taskforgeredis.Options{Client: client,
		FairnessPolicies: map[string]*taskforgeredis.FairnessPolicy{"default": mustFairnessPolicy(t, taskforgeredis.FairnessRule{}, nil)},
	})
	if err := client.Set(ctx, "taskforge:v2:fairness:default:keys", "wrong type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	msg := taskforge.Task{ID: "corrupt-registry", Name: "corrupt", Queue: "default", FairnessKey: "tenant"}
	if _, err := broker.Publish(ctx, msg, taskforge.PublishOptions{DeduplicationKey: "corrupt-registry"}); err == nil {
		t.Fatal("corrupt registry accepted")
	}
	stream := fmt.Sprintf("taskforge:v2:stream:default:fair:%x", sha256.Sum256([]byte("tenant")))
	length, err := client.XLen(ctx, stream).Result()
	if err != nil || length != 0 {
		t.Fatalf("failed publish enqueued %d entries: %v", length, err)
	}
}
