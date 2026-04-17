package integration

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/brokerredis"
	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/dlq"
	"github.com/aminkbi/taskforge/internal/fairness"
	"github.com/aminkbi/taskforge/internal/observability"
	runtimepkg "github.com/aminkbi/taskforge/internal/runtime"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
	"github.com/aminkbi/taskforge/internal/tasks"
)

const (
	ciLeaseTTL          = time.Second
	ciWaitForExpiry     = 1200 * time.Millisecond
	ciRenewBeforeExpiry = 400 * time.Millisecond
	ciPostRenewWindow   = 700 * time.Millisecond
	ciReserveTimeout    = 50 * time.Millisecond
)

func TestRedisBrokerPublishReserveAndAck(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)

	message := broker.TaskMessage{
		ID:          "integration-task-1",
		Name:        "integration.echo",
		Queue:       "default",
		Payload:     []byte(`{"hello":"world"}`),
		MaxAttempts: 3,
		CreatedAt:   time.Now().UTC(),
	}

	if _, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery, err := brokerInstance.Reserve(ctx, "default", "integration-worker")
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if delivery.Message.ID != message.ID {
		t.Fatalf("Reserve() task id = %q, want %q", delivery.Message.ID, message.ID)
	}
	if delivery.Execution.DeliveryID == "" {
		t.Fatalf("Reserve() delivery id is empty")
	}

	pendingBeforeAck, err := client.XPending(ctx, "taskforge:stream:default", "taskforge:default").Result()
	if err != nil {
		t.Fatalf("XPending() before ack error = %v", err)
	}
	if pendingBeforeAck.Count != 1 {
		t.Fatalf("pending count before ack = %d, want 1", pendingBeforeAck.Count)
	}

	if err := brokerInstance.Ack(ctx, delivery); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	pendingAfterAck, err := client.XPending(ctx, "taskforge:stream:default", "taskforge:default").Result()
	if err != nil {
		t.Fatalf("XPending() after ack error = %v", err)
	}
	if pendingAfterAck.Count != 0 {
		t.Fatalf("pending count after ack = %d, want 0", pendingAfterAck.Count)
	}
}

func TestDependencyBudgetCapacityIsSharedAcrossWorkerPools(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	metrics := observability.NewMetrics()
	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, metrics, brokerredis.Options{
		ReserveTimeout:    ciReserveTimeout,
		DependencyBudgets: map[string]int{"downstream": 1},
	})
	for _, msg := range []broker.TaskMessage{
		{
			ID:        "budgeted-1",
			Name:      "integration.shared-budget",
			Queue:     "critical",
			CreatedAt: time.Now().UTC(),
		},
		{
			ID:        "budgeted-2",
			Name:      "integration.shared-budget",
			Queue:     "bulk",
			CreatedAt: time.Now().UTC(),
		},
	} {
		if _, err := brokerInstance.Publish(ctx, msg, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}

	started := make(chan string, 2)
	release := make(chan struct{})
	handler := runtimepkg.HandlerFunc(func(ctx context.Context, msg broker.TaskMessage) error {
		started <- msg.ID
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return nil
		}
	})

	workers := &runtimepkg.Manager{
		Workers: []*runtimepkg.Worker{
			{
				Broker:        brokerInstance,
				Handler:       handler,
				Logger:        slog.Default(),
				Metrics:       metrics,
				Clock:         clock.RealClock{},
				RetryPolicy:   tasks.DefaultRetryPolicy(1),
				PoolName:      "critical",
				Queue:         "critical",
				ConsumerID:    "integration-worker",
				LeaseTTL:      30 * time.Second,
				Concurrency:   1,
				Prefetch:      1,
				BudgetManager: brokerInstance.BudgetManager(),
				TaskBudgets: map[string]runtimepkg.TaskBudget{
					"integration.shared-budget": {Budget: "downstream", Tokens: 1},
				},
			},
			{
				Broker:        brokerInstance,
				Handler:       handler,
				Logger:        slog.Default(),
				Metrics:       metrics,
				Clock:         clock.RealClock{},
				RetryPolicy:   tasks.DefaultRetryPolicy(1),
				PoolName:      "bulk",
				Queue:         "bulk",
				ConsumerID:    "integration-worker",
				LeaseTTL:      30 * time.Second,
				Concurrency:   1,
				Prefetch:      1,
				BudgetManager: brokerInstance.BudgetManager(),
				TaskBudgets: map[string]runtimepkg.TaskBudget{
					"integration.shared-budget": {Budget: "downstream", Tokens: 1},
				},
			},
		},
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- workers.Run(runCtx)
	}()

	first := waitForTaskStart(t, started)
	select {
	case second := <-started:
		t.Fatalf("second task started before budget capacity was released: %q", second)
	case <-time.After(150 * time.Millisecond):
	}

	close(release)
	second := waitForTaskStart(t, started)
	if first == second {
		t.Fatalf("expected distinct tasks to start, got %q twice", first)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("workers.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("workers did not stop before timeout")
	}
}

func TestRedisBrokerQueueMetricsTrackReserveAndAckCleanup(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)

	message := broker.TaskMessage{
		ID:        "integration-task-metrics",
		Name:      "integration.metrics",
		Queue:     "critical",
		Payload:   []byte(`{"hello":"metrics"}`),
		CreatedAt: time.Now().UTC(),
	}
	if _, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	snapshot, err := brokerInstance.QueueMetricsSnapshot(ctx, "critical")
	if err != nil {
		t.Fatalf("QueueMetricsSnapshot() before reserve error = %v", err)
	}
	if snapshot.Depth != 1 || snapshot.Reserved != 0 {
		t.Fatalf("pre-reserve queue snapshot = %+v, want depth=1 reserved=0", snapshot)
	}

	delivery, err := brokerInstance.Reserve(ctx, "critical", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}

	snapshot, err = brokerInstance.QueueMetricsSnapshot(ctx, "critical")
	if err != nil {
		t.Fatalf("QueueMetricsSnapshot() after reserve error = %v", err)
	}
	if snapshot.Depth != 0 || snapshot.Reserved != 1 || snapshot.Consumers != 1 {
		t.Fatalf("reserved queue snapshot = %+v, want depth=0 reserved=1 consumers=1", snapshot)
	}

	if err := brokerInstance.Ack(ctx, delivery); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	streamLen, err := client.XLen(ctx, "taskforge:stream:critical").Result()
	if err != nil && !strings.Contains(err.Error(), "no such key") {
		t.Fatalf("XLen() error = %v", err)
	}
	if streamLen != 0 {
		t.Fatalf("stream length after ack = %d, want 0", streamLen)
	}

	snapshot, err = brokerInstance.QueueMetricsSnapshot(ctx, "critical")
	if err != nil {
		t.Fatalf("QueueMetricsSnapshot() after ack error = %v", err)
	}
	if snapshot.Depth != 0 || snapshot.Reserved != 0 {
		t.Fatalf("post-ack queue snapshot = %+v, want depth=0 reserved=0", snapshot)
	}
}

func TestRedisBrokerConsumersDoNotDuplicateGroupDelivery(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t, 30*time.Second)

	message := broker.TaskMessage{
		ID:        "integration-task-2",
		Name:      "integration.echo",
		Queue:     "default",
		Payload:   []byte(`{"hello":"stream"}`),
		CreatedAt: time.Now().UTC(),
	}

	if _, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	firstDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() first consumer error = %v", err)
	}

	secondCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()

	_, err = brokerInstance.Reserve(secondCtx, "default", "consumer-b")
	if !errors.Is(err, broker.ErrNoTask) {
		t.Fatalf("Reserve() second consumer error = %v, want %v", err, broker.ErrNoTask)
	}

	if err := brokerInstance.Ack(ctx, firstDelivery); err != nil {
		t.Fatalf("Ack() first delivery error = %v", err)
	}
}

func TestRedisBrokerFairnessPreventsTenantStarvationOnSharedQueue(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	policy := mustFairnessPolicy(t, fairness.Rule{}, []fairness.Rule{
		{Name: "tenant-a", Keys: []string{"tenant-a"}},
		{Name: "tenant-b", Keys: []string{"tenant-b"}},
	})
	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, observability.NewMetrics(), brokerredis.Options{
		ReserveTimeout:   ciReserveTimeout,
		FairnessPolicies: map[string]*fairness.Policy{"default": policy},
	})

	for i := 0; i < 6; i++ {
		if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
			ID:          "tenant-a-" + strconv.Itoa(i),
			Name:        "integration.shared",
			Queue:       "default",
			FairnessKey: "tenant-a",
			Payload:     []byte(`{"tenant":"a"}`),
			CreatedAt:   time.Now().UTC(),
		}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			t.Fatalf("Publish() tenant-a error = %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
			ID:          "tenant-b-" + strconv.Itoa(i),
			Name:        "integration.shared",
			Queue:       "default",
			FairnessKey: "tenant-b",
			Payload:     []byte(`{"tenant":"b"}`),
			CreatedAt:   time.Now().UTC(),
		}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			t.Fatalf("Publish() tenant-b error = %v", err)
		}
	}

	first, err := brokerInstance.Reserve(ctx, "default", "fairness-consumer")
	if err != nil {
		t.Fatalf("Reserve() first error = %v", err)
	}
	second, err := brokerInstance.Reserve(ctx, "default", "fairness-consumer")
	if err != nil {
		t.Fatalf("Reserve() second error = %v", err)
	}

	keys := map[string]bool{
		first.Message.FairnessKey:  true,
		second.Message.FairnessKey: true,
	}
	if !keys["tenant-a"] || !keys["tenant-b"] {
		t.Fatalf("first two fairness keys = %q, %q, want tenant-a and tenant-b", first.Message.FairnessKey, second.Message.FairnessKey)
	}

	if err := brokerInstance.Ack(ctx, first); err != nil {
		t.Fatalf("Ack() first error = %v", err)
	}
	if err := brokerInstance.Ack(ctx, second); err != nil {
		t.Fatalf("Ack() second error = %v", err)
	}
}

func TestRedisBrokerFairnessReservedCapacityProtectsVipTraffic(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	policy := mustFairnessPolicy(t, fairness.Rule{HardQuota: 1}, []fairness.Rule{
		{Name: "vip", Keys: []string{"vip"}, ReservedConcurrency: 1, HardQuota: 1},
	})
	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, observability.NewMetrics(), brokerredis.Options{
		ReserveTimeout:   ciReserveTimeout,
		FairnessPolicies: map[string]*fairness.Policy{"default": policy},
	})

	for i := 0; i < 3; i++ {
		if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
			ID:          "noise-" + strconv.Itoa(i),
			Name:        "integration.shared",
			Queue:       "default",
			FairnessKey: "noise",
			Payload:     []byte(`{"tenant":"noise"}`),
			CreatedAt:   time.Now().UTC(),
		}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			t.Fatalf("Publish() noise error = %v", err)
		}
	}

	first, err := brokerInstance.Reserve(ctx, "default", "fairness-consumer")
	if err != nil {
		t.Fatalf("Reserve() first error = %v", err)
	}
	if first.Message.FairnessKey != "noise" {
		t.Fatalf("first fairness key = %q, want noise", first.Message.FairnessKey)
	}

	if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:          "vip-1",
		Name:        "integration.shared",
		Queue:       "default",
		FairnessKey: "vip",
		Payload:     []byte(`{"tenant":"vip"}`),
		CreatedAt:   time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() vip error = %v", err)
	}

	second, err := brokerInstance.Reserve(ctx, "default", "fairness-consumer")
	if err != nil {
		t.Fatalf("Reserve() second error = %v", err)
	}
	if second.Message.FairnessKey != "vip" {
		t.Fatalf("second fairness key = %q, want vip", second.Message.FairnessKey)
	}

	if err := brokerInstance.Ack(ctx, first); err != nil {
		t.Fatalf("Ack() first error = %v", err)
	}
	if err := brokerInstance.Ack(ctx, second); err != nil {
		t.Fatalf("Ack() second error = %v", err)
	}
}

func TestRedisBrokerFairnessHardQuotaThrottlesBusyTenant(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	policy := mustFairnessPolicy(t, fairness.Rule{}, []fairness.Rule{
		{Name: "alpha", Keys: []string{"alpha"}, HardQuota: 1},
		{Name: "beta", Keys: []string{"beta"}, HardQuota: 1},
	})
	metrics := observability.NewMetrics()
	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, metrics, brokerredis.Options{
		ReserveTimeout:   ciReserveTimeout,
		FairnessPolicies: map[string]*fairness.Policy{"default": policy},
	})

	for _, fairnessKey := range []string{"alpha", "alpha", "beta"} {
		if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
			ID:          fairnessKey + "-" + uuid.NewString(),
			Name:        "integration.shared",
			Queue:       "default",
			FairnessKey: fairnessKey,
			Payload:     []byte(`{"tenant":"` + fairnessKey + `"}`),
			CreatedAt:   time.Now().UTC(),
		}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			t.Fatalf("Publish() %s error = %v", fairnessKey, err)
		}
	}

	first, err := brokerInstance.Reserve(ctx, "default", "fairness-consumer")
	if err != nil {
		t.Fatalf("Reserve() first error = %v", err)
	}
	if first.Message.FairnessKey != "alpha" {
		t.Fatalf("first fairness key = %q, want alpha", first.Message.FairnessKey)
	}

	second, err := brokerInstance.Reserve(ctx, "default", "fairness-consumer")
	if err != nil {
		t.Fatalf("Reserve() second error = %v", err)
	}
	if second.Message.FairnessKey != "beta" {
		t.Fatalf("second fairness key = %q, want beta", second.Message.FairnessKey)
	}

	metricValue := metricCounterValue(t, metrics.Registry, "taskforge_fairness_quota_deferrals_total", map[string]string{
		"queue":           "default",
		"fairness_bucket": "alpha",
		"reason":          "hard_quota",
	})
	if metricValue < 1 {
		t.Fatalf("quota deferrals = %v, want >= 1", metricValue)
	}

	if err := brokerInstance.Ack(ctx, first); err != nil {
		t.Fatalf("Ack() first error = %v", err)
	}
	if err := brokerInstance.Ack(ctx, second); err != nil {
		t.Fatalf("Ack() second error = %v", err)
	}
}

func TestRedisBrokerAdmissionDefersWhenPendingCapReached(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, observability.NewMetrics(), brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
		AdmissionPolicies: map[string]brokerredis.AdmissionPolicy{
			"default": {
				Mode:          brokerredis.AdmissionModeDefer,
				MaxPending:    1,
				DeferInterval: 50 * time.Millisecond,
			},
		},
	})

	if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:        "admission-ready",
		Name:      "integration.admission",
		Queue:     "default",
		Payload:   []byte(`{"hello":"ready"}`),
		CreatedAt: time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() first error = %v", err)
	}

	result, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:        "admission-deferred",
		Name:      "integration.admission",
		Queue:     "default",
		Payload:   []byte(`{"hello":"deferred"}`),
		CreatedAt: time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew})
	if err != nil {
		t.Fatalf("Publish() second error = %v", err)
	}
	if result.Decision != broker.AdmissionDecisionDeferred || result.DeferredUntil == nil || result.Reason != "queue_pending_cap" {
		t.Fatalf("second publish result = %+v, want deferred queue_pending_cap with deferred_until", result)
	}

	streamLen, err := client.XLen(ctx, "taskforge:stream:default").Result()
	if err != nil {
		t.Fatalf("XLen() error = %v", err)
	}
	if streamLen != 1 {
		t.Fatalf("stream length = %d, want 1", streamLen)
	}
	delayedCount, err := client.ZCard(ctx, "taskforge:delayed").Result()
	if err != nil {
		t.Fatalf("ZCard() error = %v", err)
	}
	if delayedCount != 1 {
		t.Fatalf("delayed count = %d, want 1", delayedCount)
	}
}

func TestRedisBrokerAdmissionRejectsWhenPendingCapReached(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, observability.NewMetrics(), brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
		AdmissionPolicies: map[string]brokerredis.AdmissionPolicy{
			"default": {
				Mode:          brokerredis.AdmissionModeReject,
				MaxPending:    1,
				DeferInterval: 50 * time.Millisecond,
			},
		},
	})

	if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:        "admission-reject-1",
		Name:      "integration.admission",
		Queue:     "default",
		Payload:   []byte(`{"hello":"ready"}`),
		CreatedAt: time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() first error = %v", err)
	}

	_, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:        "admission-reject-2",
		Name:      "integration.admission",
		Queue:     "default",
		Payload:   []byte(`{"hello":"blocked"}`),
		CreatedAt: time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew})
	var admissionErr *broker.AdmissionError
	if !errors.As(err, &admissionErr) {
		t.Fatalf("Publish() error = %v, want admission error", err)
	}
	if admissionErr.Reason != "queue_pending_cap" {
		t.Fatalf("Admission reason = %q, want %q", admissionErr.Reason, "queue_pending_cap")
	}

	streamLen, err := client.XLen(ctx, "taskforge:stream:default").Result()
	if err != nil {
		t.Fatalf("XLen() error = %v", err)
	}
	if streamLen != 1 {
		t.Fatalf("stream length = %d, want 1", streamLen)
	}
	delayedCount, err := client.ZCard(ctx, "taskforge:delayed").Result()
	if err != nil {
		t.Fatalf("ZCard() error = %v", err)
	}
	if delayedCount != 0 {
		t.Fatalf("delayed count = %d, want 0", delayedCount)
	}
}

func TestRedisBrokerAdmissionIgnoresOldPendingHeadForOldestReadyAge(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, observability.NewMetrics(), brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
		AdmissionPolicies: map[string]brokerredis.AdmissionPolicy{
			"default": {
				Mode:              brokerredis.AdmissionModeReject,
				MaxOldestReadyAge: 100 * time.Millisecond,
			},
		},
	})

	oldCreatedAt := time.Now().UTC().Add(-time.Hour)
	if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:        "admission-old-pending",
		Name:      "integration.admission",
		Queue:     "default",
		Payload:   []byte(`{"hello":"old"}`),
		CreatedAt: oldCreatedAt,
	}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() old error = %v", err)
	}
	if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:        "admission-fresh-ready",
		Name:      "integration.admission",
		Queue:     "default",
		Payload:   []byte(`{"hello":"fresh"}`),
		CreatedAt: time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() fresh error = %v", err)
	}

	if _, err := brokerInstance.Reserve(ctx, "default", "consumer-a"); err != nil {
		t.Fatalf("Reserve() old pending error = %v", err)
	}

	result, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:        "admission-follow-up",
		Name:      "integration.admission",
		Queue:     "default",
		Payload:   []byte(`{"hello":"follow-up"}`),
		CreatedAt: time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew})
	if err != nil {
		t.Fatalf("Publish() follow-up error = %v", err)
	}
	if result.Decision != broker.AdmissionDecisionAccepted {
		t.Fatalf("follow-up result = %+v, want accepted", result)
	}
}

func TestRedisBrokerAdmissionFairnessKeyCapDefersOnlyNoisyKey(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	policy := mustFairnessPolicy(t, fairness.Rule{}, []fairness.Rule{
		{Name: "tenant-a", Keys: []string{"tenant-a"}},
		{Name: "tenant-b", Keys: []string{"tenant-b"}},
	})
	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, observability.NewMetrics(), brokerredis.Options{
		ReserveTimeout:   ciReserveTimeout,
		FairnessPolicies: map[string]*fairness.Policy{"default": policy},
		AdmissionPolicies: map[string]brokerredis.AdmissionPolicy{
			"default": {
				Mode:                     brokerredis.AdmissionModeDefer,
				MaxPendingPerFairnessKey: 1,
				DeferInterval:            50 * time.Millisecond,
			},
		},
	})

	if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:          "tenant-a-1",
		Name:        "integration.shared",
		Queue:       "default",
		FairnessKey: "tenant-a",
		Payload:     []byte(`{"tenant":"tenant-a"}`),
		CreatedAt:   time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() tenant-a first error = %v", err)
	}

	result, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:          "tenant-a-2",
		Name:        "integration.shared",
		Queue:       "default",
		FairnessKey: "tenant-a",
		Payload:     []byte(`{"tenant":"tenant-a"}`),
		CreatedAt:   time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew})
	if err != nil {
		t.Fatalf("Publish() tenant-a second error = %v", err)
	}
	if result.Decision != broker.AdmissionDecisionDeferred || result.Reason != "fairness_key_pending_cap" {
		t.Fatalf("tenant-a result = %+v, want deferred fairness_key_pending_cap", result)
	}

	result, err = brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:          "tenant-b-1",
		Name:        "integration.shared",
		Queue:       "default",
		FairnessKey: "tenant-b",
		Payload:     []byte(`{"tenant":"tenant-b"}`),
		CreatedAt:   time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew})
	if err != nil {
		t.Fatalf("Publish() tenant-b error = %v", err)
	}
	if result.Decision != broker.AdmissionDecisionAccepted {
		t.Fatalf("tenant-b result = %+v, want accepted", result)
	}

	delayedCount, err := client.ZCard(ctx, "taskforge:delayed").Result()
	if err != nil {
		t.Fatalf("ZCard() error = %v", err)
	}
	if delayedCount != 1 {
		t.Fatalf("delayed count = %d, want 1", delayedCount)
	}
}

func TestRedisBrokerAdmissionIgnoresOldPendingFairnessHeadForOldestReadyAge(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	policy := mustFairnessPolicy(t, fairness.Rule{}, []fairness.Rule{
		{Name: "tenant-a", Keys: []string{"tenant-a"}},
	})
	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, observability.NewMetrics(), brokerredis.Options{
		ReserveTimeout:   ciReserveTimeout,
		FairnessPolicies: map[string]*fairness.Policy{"default": policy},
		AdmissionPolicies: map[string]brokerredis.AdmissionPolicy{
			"default": {
				Mode:              brokerredis.AdmissionModeReject,
				MaxOldestReadyAge: 100 * time.Millisecond,
			},
		},
	})

	if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:          "fairness-old-pending",
		Name:        "integration.shared",
		Queue:       "default",
		FairnessKey: "tenant-a",
		Payload:     []byte(`{"tenant":"tenant-a"}`),
		CreatedAt:   time.Now().UTC().Add(-time.Hour),
	}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() old fairness error = %v", err)
	}
	if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:          "fairness-fresh-ready",
		Name:        "integration.shared",
		Queue:       "default",
		FairnessKey: "tenant-a",
		Payload:     []byte(`{"tenant":"tenant-a"}`),
		CreatedAt:   time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() fresh fairness error = %v", err)
	}

	if _, err := brokerInstance.Reserve(ctx, "default", "fairness-consumer"); err != nil {
		t.Fatalf("Reserve() fairness pending error = %v", err)
	}

	result, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:          "fairness-follow-up",
		Name:        "integration.shared",
		Queue:       "default",
		FairnessKey: "tenant-a",
		Payload:     []byte(`{"tenant":"tenant-a"}`),
		CreatedAt:   time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew})
	if err != nil {
		t.Fatalf("Publish() fairness follow-up error = %v", err)
	}
	if result.Decision != broker.AdmissionDecisionAccepted {
		t.Fatalf("fairness follow-up result = %+v, want accepted", result)
	}
}

func TestRedisBrokerFairnessWakeSignalStaysBounded(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	policy := mustFairnessPolicy(t, fairness.Rule{}, []fairness.Rule{
		{Name: "tenant-a", Keys: []string{"tenant-a"}},
	})
	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, observability.NewMetrics(), brokerredis.Options{
		ReserveTimeout:   ciReserveTimeout,
		FairnessPolicies: map[string]*fairness.Policy{"default": policy},
	})

	for i := 0; i < 10; i++ {
		if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
			ID:          "fairness-signal-" + strconv.Itoa(i),
			Name:        "integration.shared",
			Queue:       "default",
			FairnessKey: "tenant-a",
			Payload:     []byte(`{"tenant":"tenant-a"}`),
			CreatedAt:   time.Now().UTC(),
		}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			t.Fatalf("Publish() fairness signal %d error = %v", i, err)
		}
	}

	length, err := client.LLen(ctx, "taskforge:fairness:default:ready").Result()
	if err != nil {
		t.Fatalf("LLen() error = %v", err)
	}
	if length != 1 {
		t.Fatalf("fairness notify length = %d, want 1", length)
	}
}

func TestRedisBrokerAdmissionRedefersDueWorkUnderRejectMode(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)
	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, observability.NewMetrics(), brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
		AdmissionPolicies: map[string]brokerredis.AdmissionPolicy{
			"default": {
				Mode:          brokerredis.AdmissionModeReject,
				MaxPending:    1,
				DeferInterval: 50 * time.Millisecond,
			},
		},
	})

	eta := time.Now().UTC().Add(20 * time.Millisecond)
	if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:        "due-work",
		Name:      "integration.delayed",
		Queue:     "default",
		Payload:   []byte(`{"hello":"later"}`),
		ETA:       &eta,
		CreatedAt: time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() delayed error = %v", err)
	}
	if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
		ID:        "blocking-work",
		Name:      "integration.ready",
		Queue:     "default",
		Payload:   []byte(`{"hello":"now"}`),
		CreatedAt: time.Now().UTC(),
	}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() blocking error = %v", err)
	}

	time.Sleep(25 * time.Millisecond)
	moved, err := brokerInstance.MoveDue(ctx, time.Now().UTC(), 10)
	if err != nil {
		t.Fatalf("MoveDue() error = %v", err)
	}
	if moved != 1 {
		t.Fatalf("MoveDue() moved = %d, want 1", moved)
	}

	streamLen, err := client.XLen(ctx, "taskforge:stream:default").Result()
	if err != nil {
		t.Fatalf("XLen() error = %v", err)
	}
	if streamLen != 1 {
		t.Fatalf("stream length = %d, want 1", streamLen)
	}
	delayedCount, err := client.ZCard(ctx, "taskforge:delayed").Result()
	if err != nil {
		t.Fatalf("ZCard() error = %v", err)
	}
	if delayedCount != 1 {
		t.Fatalf("delayed count = %d, want 1", delayedCount)
	}
}

func TestIntegrationWorkersIsolateQueuesByPool(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	criticalBroker := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, observability.NewMetrics(), brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	})
	bulkBroker := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, observability.NewMetrics(), brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	})
	publisher := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, observability.NewMetrics(), brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	})
	deadLetters := dlq.NewService(client, publisher, slog.Default())

	for _, message := range []broker.TaskMessage{
		{
			ID:        "integration-critical-1",
			Name:      "integration.critical",
			Queue:     "critical",
			Payload:   []byte(`{"hello":"critical"}`),
			CreatedAt: time.Now().UTC(),
		},
		{
			ID:        "integration-bulk-1",
			Name:      "integration.bulk",
			Queue:     "bulk",
			Payload:   []byte(`{"hello":"bulk"}`),
			CreatedAt: time.Now().UTC(),
		},
	} {
		if _, err := publisher.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}

	var mu sync.Mutex
	processedByQueue := map[string][]string{}
	manager := &runtimepkg.Manager{
		Workers: []*runtimepkg.Worker{
			newIntegrationWorkerWithQueue(criticalBroker, deadLetters, "critical", runtimepkg.HandlerFunc(func(ctx context.Context, msg broker.TaskMessage) error {
				mu.Lock()
				processedByQueue["critical"] = append(processedByQueue["critical"], msg.ID)
				mu.Unlock()
				return nil
			}), tasks.DefaultRetryPolicy(1)),
			newIntegrationWorkerWithQueue(bulkBroker, deadLetters, "bulk", runtimepkg.HandlerFunc(func(ctx context.Context, msg broker.TaskMessage) error {
				mu.Lock()
				processedByQueue["bulk"] = append(processedByQueue["bulk"], msg.ID)
				mu.Unlock()
				return nil
			}), tasks.DefaultRetryPolicy(1)),
		},
	}

	runManagerUntil(t, manager, func() (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		return len(processedByQueue["critical"]) == 1 && len(processedByQueue["bulk"]) == 1, nil
	})

	mu.Lock()
	defer mu.Unlock()
	if got := processedByQueue["critical"]; len(got) != 1 || got[0] != "integration-critical-1" {
		t.Fatalf("critical worker processed = %+v, want [integration-critical-1]", got)
	}
	if got := processedByQueue["bulk"]; len(got) != 1 || got[0] != "integration-bulk-1" {
		t.Fatalf("bulk worker processed = %+v, want [integration-bulk-1]", got)
	}
}

func TestRedisBrokerReclaimsExpiredDelivery(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t, ciLeaseTTL)

	message := broker.TaskMessage{
		ID:        "integration-task-reclaim",
		Name:      "integration.reclaim",
		Queue:     "default",
		Payload:   []byte(`{"hello":"reclaim"}`),
		CreatedAt: time.Now().UTC(),
	}

	if _, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	firstDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() first consumer error = %v", err)
	}

	time.Sleep(ciWaitForExpiry)

	reclaimedDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-b")
	if err != nil {
		t.Fatalf("Reserve() reclaimed consumer error = %v", err)
	}

	if reclaimedDelivery.Message.ID != firstDelivery.Message.ID {
		t.Fatalf("reclaimed task id = %q, want %q", reclaimedDelivery.Message.ID, firstDelivery.Message.ID)
	}
	if reclaimedDelivery.Execution.LeaseOwner == firstDelivery.Execution.LeaseOwner {
		t.Fatalf("reclaimed lease owner = %q, want different owner", reclaimedDelivery.Execution.LeaseOwner)
	}
	if reclaimedDelivery.Execution.DeliveryCount < 2 {
		t.Fatalf("reclaimed delivery count = %d, want >= 2", reclaimedDelivery.Execution.DeliveryCount)
	}
}

func TestRedisBrokerReclaimsExpiredDeliveryBeyondInitialPendingWindow(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, observability.NewMetrics(), brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	})

	for i := 0; i < 20; i++ {
		longTTL := time.Hour
		if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
			ID:                "stable-" + strconv.Itoa(i),
			Name:              "integration.reclaim",
			Queue:             "default",
			Payload:           []byte(`{"kind":"stable"}`),
			VisibilityTimeout: longTTL,
			CreatedAt:         time.Now().UTC(),
		}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			t.Fatalf("Publish() stable %d error = %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		shortTTL := 50 * time.Millisecond
		if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
			ID:                "expired-" + strconv.Itoa(i),
			Name:              "integration.reclaim",
			Queue:             "default",
			Payload:           []byte(`{"kind":"expired"}`),
			VisibilityTimeout: shortTTL,
			CreatedAt:         time.Now().UTC(),
		}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			t.Fatalf("Publish() expired %d error = %v", i, err)
		}
	}

	for i := 0; i < 25; i++ {
		if _, err := brokerInstance.Reserve(ctx, "default", "consumer-a"); err != nil {
			t.Fatalf("Reserve() pending %d error = %v", i, err)
		}
	}

	time.Sleep(120 * time.Millisecond)

	reclaimed, err := brokerInstance.Reserve(ctx, "default", "consumer-b")
	if err != nil {
		t.Fatalf("Reserve() reclaimed error = %v", err)
	}
	if !strings.HasPrefix(reclaimed.Message.ID, "expired-") {
		t.Fatalf("reclaimed task id = %q, want expired-*", reclaimed.Message.ID)
	}
	if reclaimed.Execution.DeliveryCount < 2 {
		t.Fatalf("reclaimed delivery count = %d, want >= 2", reclaimed.Execution.DeliveryCount)
	}
}

func TestRedisBrokerRejectsStaleAckAfterReclaim(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t, ciLeaseTTL)

	message := broker.TaskMessage{
		ID:        "integration-task-stale-ack",
		Name:      "integration.stale_ack",
		Queue:     "default",
		Payload:   []byte(`{"hello":"stale"}`),
		CreatedAt: time.Now().UTC(),
	}

	if _, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	firstDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() first consumer error = %v", err)
	}

	time.Sleep(ciWaitForExpiry)

	reclaimedDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-b")
	if err != nil {
		t.Fatalf("Reserve() reclaimed consumer error = %v", err)
	}

	if err := brokerInstance.Ack(ctx, firstDelivery); !errors.Is(err, broker.ErrStaleDelivery) {
		t.Fatalf("Ack() stale delivery error = %v, want %v", err, broker.ErrStaleDelivery)
	}

	if err := brokerInstance.Ack(ctx, reclaimedDelivery); err != nil {
		t.Fatalf("Ack() reclaimed delivery error = %v", err)
	}
}

func TestRedisBrokerRejectsStaleNackAfterReclaim(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t, ciLeaseTTL)

	message := broker.TaskMessage{
		ID:        "integration-task-stale-nack",
		Name:      "integration.stale_nack",
		Queue:     "default",
		Payload:   []byte(`{"hello":"stale-nack"}`),
		CreatedAt: time.Now().UTC(),
	}

	if _, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	firstDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() first consumer error = %v", err)
	}

	time.Sleep(ciWaitForExpiry)

	reclaimedDelivery, err := brokerInstance.Reserve(ctx, "default", "consumer-b")
	if err != nil {
		t.Fatalf("Reserve() reclaimed consumer error = %v", err)
	}

	if err := brokerInstance.Nack(ctx, firstDelivery, false); !errors.Is(err, broker.ErrStaleDelivery) {
		t.Fatalf("Nack() stale delivery error = %v, want %v", err, broker.ErrStaleDelivery)
	}

	if err := brokerInstance.Ack(ctx, reclaimedDelivery); err != nil {
		t.Fatalf("Ack() reclaimed delivery error = %v", err)
	}
}

func TestRedisBrokerExpiresCurrentOwnerAck(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t, ciLeaseTTL)

	message := broker.TaskMessage{
		ID:        "integration-task-expired-ack",
		Name:      "integration.expired_ack",
		Queue:     "default",
		Payload:   []byte(`{"hello":"expired"}`),
		CreatedAt: time.Now().UTC(),
	}

	if _, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery, err := brokerInstance.Reserve(ctx, "default", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}

	time.Sleep(ciWaitForExpiry)

	if err := brokerInstance.Ack(ctx, delivery); !errors.Is(err, broker.ErrDeliveryExpired) {
		t.Fatalf("Ack() expired delivery error = %v, want %v", err, broker.ErrDeliveryExpired)
	}
}

func TestRedisBrokerExtendLeasePreventsReclaim(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, ciLeaseTTL)

	message := broker.TaskMessage{
		ID:        "integration-task-extend",
		Name:      "integration.extend",
		Queue:     "default",
		Payload:   []byte(`{"hello":"extend"}`),
		CreatedAt: time.Now().UTC(),
	}

	if _, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	delivery, err := brokerInstance.Reserve(ctx, "default", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}

	time.Sleep(ciRenewBeforeExpiry)
	if err := brokerInstance.ExtendLease(ctx, delivery, ciLeaseTTL); err != nil {
		t.Fatalf("ExtendLease() error = %v", err)
	}

	time.Sleep(ciPostRenewWindow)

	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: "taskforge:stream:default",
		Group:  "taskforge:default",
		Start:  delivery.Execution.DeliveryID,
		End:    delivery.Execution.DeliveryID,
		Count:  1,
	}).Result()
	if err != nil {
		t.Fatalf("XPendingExt() error = %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending entry count = %d, want 1", len(pending))
	}
	if pending[0].Consumer != delivery.Execution.LeaseOwner {
		t.Fatalf("pending owner = %q, want %q", pending[0].Consumer, delivery.Execution.LeaseOwner)
	}
	if pending[0].Idle >= ciLeaseTTL {
		t.Fatalf("pending idle = %v, want less than %v", pending[0].Idle, ciLeaseTTL)
	}

	if err := brokerInstance.Ack(ctx, delivery); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
}

func TestWorkerKeepsPendingDeliveryLeasedWhileLocallyBlocked(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t, ciLeaseTTL)

	for _, id := range []string{"integration-pending-1", "integration-pending-2"} {
		if _, err := brokerInstance.Publish(ctx, broker.TaskMessage{
			ID:        id,
			Name:      "integration.pending_blocked",
			Queue:     "default",
			Payload:   []byte(`{"hello":"pending"}`),
			CreatedAt: time.Now().UTC(),
		}, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}

	started := make(chan string, 2)
	releaseFirst := make(chan struct{})
	worker := newIntegrationWorker(brokerInstance, nil, runtimepkg.HandlerFunc(func(ctx context.Context, msg broker.TaskMessage) error {
		started <- msg.ID
		if msg.ID != "integration-pending-1" {
			return nil
		}
		select {
		case <-releaseFirst:
			return nil
		case <-ctx.Done():
			return nil
		}
	}), tasks.DefaultRetryPolicy(1))
	worker.Concurrency = 2
	worker.Prefetch = 2
	worker.LeaseTTL = ciLeaseTTL
	worker.GlobalTaskLimiter = runtimepkg.NewTaskTypeLimiter(map[string]int{"integration.pending_blocked": 1})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(runCtx)
	}()

	if first := waitForTaskStart(t, started); first != "integration-pending-1" {
		t.Fatalf("first started task = %q, want integration-pending-1", first)
	}

	time.Sleep(ciWaitForExpiry)

	reserveCtx, reserveCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer reserveCancel()
	if _, err := brokerInstance.Reserve(reserveCtx, "default", "consumer-b"); !errors.Is(err, broker.ErrNoTask) {
		t.Fatalf("Reserve() while second delivery is locally pending error = %v, want %v", err, broker.ErrNoTask)
	}

	close(releaseFirst)
	if second := waitForTaskStart(t, started); second != "integration-pending-2" {
		t.Fatalf("second started task = %q, want integration-pending-2", second)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("worker.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop before timeout")
	}
}

func TestRedisBrokerMoveDueReleasesIntoStreamQueueInETAOrder(t *testing.T) {
	ctx, brokerInstance, _ := newIntegrationBroker(t, 30*time.Second)

	base := time.Now().UTC()
	messages := []broker.TaskMessage{
		{
			ID:        "integration-task-delayed-3",
			Name:      "integration.delayed",
			Queue:     "default",
			Payload:   []byte(`{"hello":"third"}`),
			CreatedAt: base,
		},
		{
			ID:        "integration-task-delayed-1",
			Name:      "integration.delayed",
			Queue:     "default",
			Payload:   []byte(`{"hello":"first"}`),
			CreatedAt: base,
		},
		{
			ID:        "integration-task-delayed-2",
			Name:      "integration.delayed",
			Queue:     "default",
			Payload:   []byte(`{"hello":"second"}`),
			CreatedAt: base,
		},
	}
	eta3 := base.Add(30 * time.Millisecond)
	eta1 := base.Add(10 * time.Millisecond)
	eta2 := base.Add(20 * time.Millisecond)
	messages[0].ETA = &eta3
	messages[1].ETA = &eta1
	messages[2].ETA = &eta2

	for _, message := range messages {
		if _, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
			t.Fatalf("Publish() delayed error = %v", err)
		}
	}

	releasedAt := base.Add(time.Second)
	moved, err := brokerInstance.MoveDue(ctx, releasedAt, 10)
	if err != nil {
		t.Fatalf("MoveDue() error = %v", err)
	}
	if moved != 3 {
		t.Fatalf("MoveDue() moved = %d, want 3", moved)
	}

	expectedOrder := []struct {
		id  string
		eta time.Time
	}{
		{id: "integration-task-delayed-1", eta: eta1},
		{id: "integration-task-delayed-2", eta: eta2},
		{id: "integration-task-delayed-3", eta: eta3},
	}

	for _, expected := range expectedOrder {
		delivery, err := brokerInstance.Reserve(ctx, "default", "delayed-consumer")
		if err != nil {
			t.Fatalf("Reserve() moved task error = %v", err)
		}
		if delivery.Message.ID != expected.id {
			t.Fatalf("Reserve() moved task id = %q, want %q", delivery.Message.ID, expected.id)
		}
		if delivery.Message.Headers[schedulerpkg.HeaderScheduledFor] != expected.eta.Format(time.RFC3339Nano) {
			t.Fatalf("scheduled_for = %q, want %q", delivery.Message.Headers[schedulerpkg.HeaderScheduledFor], expected.eta.Format(time.RFC3339Nano))
		}
		if delivery.Message.Headers[schedulerpkg.HeaderReleasedAt] != releasedAt.Format(time.RFC3339Nano) {
			t.Fatalf("released_at = %q, want %q", delivery.Message.Headers[schedulerpkg.HeaderReleasedAt], releasedAt.Format(time.RFC3339Nano))
		}
		lag, err := strconv.ParseInt(delivery.Message.Headers[schedulerpkg.HeaderReleaseLagMS], 10, 64)
		if err != nil {
			t.Fatalf("parse release lag = %v", err)
		}
		if lag < 0 {
			t.Fatalf("release lag = %d, want >= 0", lag)
		}
		if err := brokerInstance.Ack(ctx, delivery); err != nil {
			t.Fatalf("Ack() error = %v", err)
		}
	}
}

func TestRedisBrokerPublishDeduplicationKeyPublishesOnce(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)

	message := broker.TaskMessage{
		ID:        "integration-dedup-publish",
		Name:      "integration.dedup",
		Queue:     "default",
		Payload:   []byte(`{"hello":"dedup"}`),
		CreatedAt: time.Now().UTC(),
	}

	first, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{
		Source:           broker.PublishSourceNew,
		DeduplicationKey: "test:publish-once",
	})
	if err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}
	second, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{
		Source:           broker.PublishSourceNew,
		DeduplicationKey: "test:publish-once",
	})
	if err != nil {
		t.Fatalf("second Publish() error = %v", err)
	}
	if first.Deduplicated {
		t.Fatal("first Publish() should not be marked deduplicated")
	}
	if !second.Deduplicated {
		t.Fatal("second Publish() should be marked deduplicated")
	}

	streamLen, err := client.XLen(ctx, "taskforge:stream:default").Result()
	if err != nil {
		t.Fatalf("XLen() error = %v", err)
	}
	if streamLen != 1 {
		t.Fatalf("stream length = %d, want 1", streamLen)
	}
}

func TestRedisBrokerMoveDueConcurrentReleasePublishesOnce(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	brokerA := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, nil, brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	})
	brokerB := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, nil, brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	})

	eta := time.Now().UTC().Add(-time.Second)
	message := broker.TaskMessage{
		ID:        "integration-move-due-dedup",
		Name:      "integration.delayed",
		Queue:     "default",
		Payload:   []byte(`{"hello":"once"}`),
		ETA:       &eta,
		CreatedAt: time.Now().UTC(),
	}
	if _, err := brokerA.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	releasedAt := time.Now().UTC()
	start := make(chan struct{})
	results := make(chan int, 2)
	errs := make(chan error, 2)
	move := func(b *brokerredis.RedisBroker) {
		<-start
		moved, err := b.MoveDue(ctx, releasedAt, 10)
		if err != nil {
			errs <- err
			return
		}
		results <- moved
	}

	go move(brokerA)
	go move(brokerB)
	close(start)

	for range 2 {
		select {
		case err := <-errs:
			t.Fatalf("MoveDue() error = %v", err)
		case <-results:
		}
	}

	streamLen, err := client.XLen(ctx, "taskforge:stream:default").Result()
	if err != nil {
		t.Fatalf("XLen() error = %v", err)
	}
	if streamLen != 1 {
		t.Fatalf("stream length = %d, want 1", streamLen)
	}
	delayedCount, err := client.ZCard(ctx, "taskforge:delayed").Result()
	if err != nil {
		t.Fatalf("ZCard() error = %v", err)
	}
	if delayedCount != 0 {
		t.Fatalf("delayed count = %d, want 0", delayedCount)
	}
}

func TestWorkerRetryableErrorSchedulesAnotherAttempt(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)
	deadLetters := dlq.NewService(client, brokerInstance, slog.Default())
	worker := newIntegrationWorker(brokerInstance, deadLetters, runtimepkg.HandlerFunc(func(context.Context, broker.TaskMessage) error {
		return runtimepkg.Retryable(errors.New("boom"))
	}), tasks.RetryPolicy{
		MaxDeliveries:  3,
		InitialBackoff: 2 * time.Second,
		MaxBackoff:     2 * time.Second,
		Multiplier:     1,
	})

	message := broker.TaskMessage{
		ID:        "integration-retry-worker",
		Name:      "integration.retryable",
		Queue:     "default",
		Payload:   []byte(`{"hello":"retry"}`),
		CreatedAt: time.Now().UTC(),
	}
	if _, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	runWorkerUntil(t, worker, func() (bool, error) {
		values, err := client.ZRange(ctx, "taskforge:delayed", 0, -1).Result()
		if err != nil {
			return false, err
		}
		return len(values) == 1, nil
	})

	values, err := client.ZRange(ctx, "taskforge:delayed", 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRange() error = %v", err)
	}
	var retried struct {
		Message broker.TaskMessage `json:"message"`
	}
	if err := json.Unmarshal([]byte(values[0]), &retried); err != nil {
		t.Fatalf("unmarshal retried delayed entry: %v", err)
	}
	if retried.Message.Attempt != 1 {
		t.Fatalf("retried attempt = %d, want 1", retried.Message.Attempt)
	}
	if retried.Message.Headers[tasks.HeaderRetryFailureClass] != string(dlq.FailureClassTransientRetryable) {
		t.Fatalf("retry failure class = %q, want %q", retried.Message.Headers[tasks.HeaderRetryFailureClass], dlq.FailureClassTransientRetryable)
	}
}

func TestSchedulerLeaderElectionDispatchesRecurringOnce(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)

	startAt := time.Now().UTC().Add(-time.Second)
	schedules := []schedulerpkg.ScheduleDefinition{{
		ID:            "integration-recurring-once",
		Interval:      500 * time.Millisecond,
		Queue:         "default",
		TaskName:      "integration.recurring",
		Payload:       json.RawMessage(`{"hello":"recurring"}`),
		Enabled:       true,
		MisfirePolicy: schedulerpkg.MisfirePolicyCoalesce,
		StartAt:       &startAt,
	}}

	schedulerA := newIntegrationScheduler(t, client, "scheduler-a", schedules)
	schedulerB := newIntegrationScheduler(t, client, "scheduler-b", schedules)

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	errChA := runScheduler(ctxA, schedulerA)
	errChB := runScheduler(ctxB, schedulerB)

	waitForStreamLength(t, client, "taskforge:stream:default", 1)
	cancelA()
	cancelB()
	waitForSchedulerStop(t, errChA)
	waitForSchedulerStop(t, errChB)

	messages := loadStreamTaskMessages(t, ctx, client, "taskforge:stream:default")
	if len(messages) != 1 {
		t.Fatalf("stream messages = %d, want 1", len(messages))
	}
	if messages[0].Headers[schedulerpkg.HeaderScheduleID] != "integration-recurring-once" {
		t.Fatalf("schedule_id header = %q, want %q", messages[0].Headers[schedulerpkg.HeaderScheduleID], "integration-recurring-once")
	}

	delivery, err := brokerInstance.Reserve(ctx, "default", "recurring-consumer")
	if err != nil {
		t.Fatalf("Reserve() recurring task error = %v", err)
	}
	if delivery.Message.ID == "" {
		t.Fatal("recurring task id is empty")
	}
}

func TestSchedulerFastFailoverDoesNotDuplicateRecurringRun(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	startAt := time.Now().UTC().Add(-20 * time.Millisecond)
	schedules := []schedulerpkg.ScheduleDefinition{{
		ID:            "integration-recurring-fast-failover",
		Interval:      300 * time.Millisecond,
		Queue:         "default",
		TaskName:      "integration.recurring",
		Payload:       json.RawMessage(`{"hello":"fast-failover"}`),
		Enabled:       true,
		MisfirePolicy: schedulerpkg.MisfirePolicyCoalesce,
		StartAt:       &startAt,
	}}

	schedulerA := newIntegrationScheduler(t, client, "scheduler-a", schedules)
	schedulerB := newIntegrationScheduler(t, client, "scheduler-b", schedules)

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	errChA := runScheduler(ctxA, schedulerA)
	errChB := runScheduler(ctxB, schedulerB)

	waitForStreamLength(t, client, "taskforge:stream:default", 1)
	leaderOwner := waitForSchedulerLeaderOwner(t, client)
	switch {
	case strings.HasPrefix(leaderOwner, "scheduler-a"):
		cancelA()
		waitForSchedulerStop(t, errChA)
	case strings.HasPrefix(leaderOwner, "scheduler-b"):
		cancelB()
		waitForSchedulerStop(t, errChB)
	default:
		t.Fatalf("unexpected scheduler leader owner %q", leaderOwner)
	}

	waitForStreamLength(t, client, "taskforge:stream:default", 2)
	cancelA()
	cancelB()
	waitForSchedulerStopIfRunning(t, errChA)
	waitForSchedulerStopIfRunning(t, errChB)

	messages := loadStreamTaskMessages(t, ctx, client, "taskforge:stream:default")
	if len(messages) != 2 {
		t.Fatalf("stream messages = %d, want 2", len(messages))
	}
	if messages[0].ID == messages[1].ID {
		t.Fatalf("recurring task ids are identical: %q", messages[0].ID)
	}
	if messages[1].Headers[schedulerpkg.HeaderScheduleID] != "integration-recurring-fast-failover" {
		t.Fatalf("schedule_id header = %q, want %q", messages[1].Headers[schedulerpkg.HeaderScheduleID], "integration-recurring-fast-failover")
	}
	firstNominalAt := mustParseRFC3339Time(t, messages[0].Headers[schedulerpkg.HeaderScheduleNominalAt])
	secondNominalAt := mustParseRFC3339Time(t, messages[1].Headers[schedulerpkg.HeaderScheduleNominalAt])
	if !secondNominalAt.After(firstNominalAt) {
		t.Fatalf("second nominal_at = %v, want later than first nominal_at %v", secondNominalAt, firstNominalAt)
	}
	missedRuns, err := strconv.Atoi(messages[1].Headers[schedulerpkg.HeaderScheduleMissedRuns])
	if err != nil {
		t.Fatalf("parse missed runs = %v", err)
	}
	if missedRuns < 0 {
		t.Fatalf("missed runs = %d, want >= 0", missedRuns)
	}
}

func TestRecurringSyncDueConcurrentDispatchPublishesOneNominalRun(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	now := time.Now().UTC()
	startAt := now.Add(-time.Second)
	schedule := schedulerpkg.ScheduleDefinition{
		ID:            "integration-recurring-concurrent",
		Interval:      time.Minute,
		Queue:         "default",
		TaskName:      "integration.recurring",
		Payload:       json.RawMessage(`{"hello":"recurring"}`),
		Enabled:       true,
		MisfirePolicy: schedulerpkg.MisfirePolicyCoalesce,
		StartAt:       &startAt,
	}
	store := schedulerpkg.NewRedisScheduleStateStore(client)
	if err := store.ReconcileConfigured(ctx, []schedulerpkg.ScheduleDefinition{schedule}, now); err != nil {
		t.Fatalf("ReconcileConfigured() error = %v", err)
	}

	brokerA := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, nil, brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	})
	brokerB := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, nil, brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	})
	serviceA := schedulerpkg.NewRecurringService(brokerA, store, []schedulerpkg.ScheduleDefinition{schedule}, slog.Default())
	serviceB := schedulerpkg.NewRecurringService(brokerB, schedulerpkg.NewRedisScheduleStateStore(client), []schedulerpkg.ScheduleDefinition{schedule}, slog.Default())

	start := make(chan struct{})
	results := make(chan int, 2)
	errs := make(chan error, 2)
	syncDue := func(service *schedulerpkg.RecurringService) {
		<-start
		dispatched, err := service.SyncDue(ctx, now)
		if err != nil {
			errs <- err
			return
		}
		results <- dispatched
	}

	go syncDue(serviceA)
	go syncDue(serviceB)
	close(start)

	totalDispatched := 0
	for range 2 {
		select {
		case err := <-errs:
			t.Fatalf("SyncDue() error = %v", err)
		case dispatched := <-results:
			totalDispatched += dispatched
		}
	}

	streamLen, err := client.XLen(ctx, "taskforge:stream:default").Result()
	if err != nil {
		t.Fatalf("XLen() error = %v", err)
	}
	if streamLen != 1 {
		t.Fatalf("stream length = %d, want 1", streamLen)
	}
	if totalDispatched != 1 {
		t.Fatalf("total dispatched = %d, want 1", totalDispatched)
	}
	finalState := loadRecurringScheduleState(t, ctx, client, schedule.ID)
	if !finalState.NextRunAt.After(now) {
		t.Fatalf("next run = %v, want after %v", finalState.NextRunAt, now)
	}
}

func TestSchedulerFailoverCoalescesMissedRecurringRuns(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	startAt := time.Now().UTC().Add(-20 * time.Millisecond)
	schedules := []schedulerpkg.ScheduleDefinition{{
		ID:            "integration-recurring-failover",
		Interval:      150 * time.Millisecond,
		Queue:         "default",
		TaskName:      "integration.recurring",
		Payload:       json.RawMessage(`{"hello":"failover"}`),
		Enabled:       true,
		MisfirePolicy: schedulerpkg.MisfirePolicyCoalesce,
		StartAt:       &startAt,
	}}

	schedulerA := newIntegrationScheduler(t, client, "scheduler-a", schedules)

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()

	errChA := runScheduler(ctxA, schedulerA)

	waitForStreamLength(t, client, "taskforge:stream:default", 1)
	cancelA()
	waitForSchedulerStop(t, errChA)

	time.Sleep(450 * time.Millisecond)

	schedulerB := newIntegrationScheduler(t, client, "scheduler-b", schedules)
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	errChB := runScheduler(ctxB, schedulerB)
	waitForStreamLength(t, client, "taskforge:stream:default", 2)
	cancelB()
	waitForSchedulerStop(t, errChB)

	messages := loadStreamTaskMessages(t, ctx, client, "taskforge:stream:default")
	if len(messages) != 2 {
		t.Fatalf("stream messages = %d, want 2", len(messages))
	}
	if messages[0].ID == messages[1].ID {
		t.Fatalf("recurring task ids are identical: %q", messages[0].ID)
	}
	if messages[1].Headers[schedulerpkg.HeaderScheduleID] != "integration-recurring-failover" {
		t.Fatalf("schedule_id header = %q, want %q", messages[1].Headers[schedulerpkg.HeaderScheduleID], "integration-recurring-failover")
	}
	missedRuns, err := strconv.Atoi(messages[1].Headers[schedulerpkg.HeaderScheduleMissedRuns])
	if err != nil {
		t.Fatalf("parse missed runs = %v", err)
	}
	if missedRuns < 1 {
		t.Fatalf("missed runs = %d, want >= 1", missedRuns)
	}
	secondNominalAt := mustParseRFC3339Time(t, messages[1].Headers[schedulerpkg.HeaderScheduleNominalAt])
	secondDispatchedAt := mustParseRFC3339Time(t, messages[1].Headers[schedulerpkg.HeaderScheduleDispatchedAt])
	if secondDispatchedAt.Sub(secondNominalAt) < 150*time.Millisecond {
		t.Fatalf("dispatch lag = %v, want at least one interval", secondDispatchedAt.Sub(secondNominalAt))
	}
}

func TestSchedulerRecurringDueIndexDispatchesSmallDueSet(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	now := time.Now().UTC()
	schedules := make([]schedulerpkg.ScheduleDefinition, 0, 253)
	for i := 0; i < 3; i++ {
		startAt := now.Add(-time.Second)
		schedules = append(schedules, schedulerpkg.ScheduleDefinition{
			ID:            "integration-recurring-due-" + strconv.Itoa(i),
			Interval:      10 * time.Second,
			Queue:         "default",
			TaskName:      "integration.recurring",
			Payload:       json.RawMessage(`{"hello":"due"}`),
			Enabled:       true,
			MisfirePolicy: schedulerpkg.MisfirePolicyCoalesce,
			StartAt:       &startAt,
		})
	}
	for i := 0; i < 250; i++ {
		startAt := now.Add(24*time.Hour + time.Duration(i)*time.Minute)
		schedules = append(schedules, schedulerpkg.ScheduleDefinition{
			ID:            "integration-recurring-future-" + strconv.Itoa(i),
			Interval:      10 * time.Second,
			Queue:         "default",
			TaskName:      "integration.recurring",
			Payload:       json.RawMessage(`{"hello":"future"}`),
			Enabled:       true,
			MisfirePolicy: schedulerpkg.MisfirePolicyCoalesce,
			StartAt:       &startAt,
		})
	}

	scheduler := newIntegrationScheduler(t, client, "scheduler-a", schedules)
	ctxScheduler, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := runScheduler(ctxScheduler, scheduler)
	waitForStreamLength(t, client, "taskforge:stream:default", 3)
	cancel()
	waitForSchedulerStop(t, errCh)

	messages := loadStreamTaskMessages(t, ctx, client, "taskforge:stream:default")
	if len(messages) != 3 {
		t.Fatalf("stream messages = %d, want 3", len(messages))
	}
	for _, msg := range messages {
		if !strings.HasPrefix(msg.Headers[schedulerpkg.HeaderScheduleID], "integration-recurring-due-") {
			t.Fatalf("unexpected schedule_id %q, want due schedule", msg.Headers[schedulerpkg.HeaderScheduleID])
		}
	}

	indexCount, err := client.ZCard(ctx, "taskforge:scheduler:recurring:due").Result()
	if err != nil {
		t.Fatalf("ZCard() recurring due index error = %v", err)
	}
	if indexCount != 253 {
		t.Fatalf("recurring due index size = %d, want 253", indexCount)
	}
}

func TestSchedulerRecurringRescheduleUpdatesDueIndex(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, 30*time.Second)

	startAt := time.Now().UTC().Add(-time.Second)
	scheduleID := "integration-recurring-reindex"
	interval := 750 * time.Millisecond
	scheduler := newIntegrationScheduler(t, client, "scheduler-a", []schedulerpkg.ScheduleDefinition{{
		ID:            scheduleID,
		Interval:      interval,
		Queue:         "default",
		TaskName:      "integration.recurring",
		Payload:       json.RawMessage(`{"hello":"reindex"}`),
		Enabled:       true,
		MisfirePolicy: schedulerpkg.MisfirePolicyCoalesce,
		StartAt:       &startAt,
	}})

	ctxScheduler, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := runScheduler(ctxScheduler, scheduler)

	waitForStreamLength(t, client, "taskforge:stream:default", 1)
	state := loadRecurringScheduleState(t, ctx, client, scheduleID)
	if state.LastDispatchedAt.IsZero() {
		t.Fatal("LastDispatchedAt is zero after recurring dispatch")
	}
	if !state.NextRunAt.After(state.LastDispatchedAt) {
		t.Fatalf("NextRunAt = %v, want after LastDispatchedAt %v", state.NextRunAt, state.LastDispatchedAt)
	}
	if state.MisfirePolicy != schedulerpkg.MisfirePolicyCoalesce {
		t.Fatalf("MisfirePolicy = %q, want %q", state.MisfirePolicy, schedulerpkg.MisfirePolicyCoalesce)
	}

	score, err := client.ZScore(ctx, "taskforge:scheduler:recurring:due", scheduleID).Result()
	if err != nil {
		t.Fatalf("ZScore() recurring due index error = %v", err)
	}
	if int64(score) != state.NextRunAt.UnixMilli() {
		t.Fatalf("due index score = %d, want %d", int64(score), state.NextRunAt.UnixMilli())
	}

	time.Sleep(150 * time.Millisecond)
	streamLen, err := client.XLen(ctx, "taskforge:stream:default").Result()
	if err != nil {
		t.Fatalf("XLen() error = %v", err)
	}
	if streamLen != 1 {
		t.Fatalf("stream length after short wait = %d, want 1", streamLen)
	}

	cancel()
	waitForSchedulerStop(t, errCh)
}

func TestWorkerPermanentErrorGoesDirectlyToDeadLetter(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)
	deadLetters := dlq.NewService(client, brokerInstance, slog.Default())
	worker := newIntegrationWorker(brokerInstance, deadLetters, runtimepkg.HandlerFunc(func(context.Context, broker.TaskMessage) error {
		return runtimepkg.Permanent(errors.New("bad payload"))
	}), tasks.DefaultRetryPolicy(3))

	message := broker.TaskMessage{
		ID:        "integration-permanent-worker",
		Name:      "integration.permanent",
		Queue:     "default",
		Payload:   []byte(`{"hello":"permanent"}`),
		CreatedAt: time.Now().UTC(),
	}
	if _, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	runWorkerUntil(t, worker, func() (bool, error) {
		entries, err := deadLetters.List(ctx, "default", 10)
		if err != nil {
			return false, err
		}
		return len(entries) == 1, nil
	})

	entries, err := deadLetters.List(ctx, "default", 10)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if entries[0].Envelope.FailureClass != dlq.FailureClassPermanent {
		t.Fatalf("failure class = %q, want %q", entries[0].Envelope.FailureClass, dlq.FailureClassPermanent)
	}
}

func TestWorkerMaxDeliveryExhaustionMovesTaskToDeadLetter(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)
	deadLetters := dlq.NewService(client, brokerInstance, slog.Default())
	worker := newIntegrationWorker(brokerInstance, deadLetters, runtimepkg.HandlerFunc(func(context.Context, broker.TaskMessage) error {
		return runtimepkg.Retryable(errors.New("retry exhausted"))
	}), tasks.DefaultRetryPolicy(3))

	message := broker.TaskMessage{
		ID:          "integration-retry-exhausted",
		Name:        "integration.exhausted",
		Queue:       "default",
		Payload:     []byte(`{"hello":"exhausted"}`),
		MaxAttempts: 1,
		CreatedAt:   time.Now().UTC(),
	}
	if _, err := brokerInstance.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	runWorkerUntil(t, worker, func() (bool, error) {
		entries, err := deadLetters.List(ctx, "default", 10)
		if err != nil {
			return false, err
		}
		return len(entries) == 1, nil
	})

	entries, err := deadLetters.List(ctx, "default", 10)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if entries[0].Envelope.OriginalTask.ID != message.ID {
		t.Fatalf("dead-letter task id = %q, want %q", entries[0].Envelope.OriginalTask.ID, message.ID)
	}
}

func TestDeadLetterServiceReplayOneEntry(t *testing.T) {
	ctx, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)
	deadLetters := dlq.NewService(client, brokerInstance, slog.Default())

	original := broker.TaskMessage{
		ID:        "integration-replay",
		Name:      "integration.replay",
		Queue:     "default",
		Payload:   []byte(`{"hello":"replay"}`),
		CreatedAt: time.Now().UTC(),
	}
	envelope := dlq.Envelope{
		OriginalTask:     original,
		FailureClass:     dlq.FailureClassPermanent,
		LastError:        "failed permanently",
		DeliveryCount:    1,
		FirstEnqueuedAt:  original.CreatedAt,
		LastFailureAt:    time.Now().UTC(),
		WorkerIdentity:   "worker-1",
		DeliveryID:       "delivery-1",
		OriginalQueue:    "default",
		OriginalTaskName: original.Name,
	}

	if err := deadLetters.PublishDeadLetter(ctx, envelope); err != nil {
		t.Fatalf("PublishDeadLetter() error = %v", err)
	}

	entries, err := deadLetters.List(ctx, "default", 10)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("dead-letter entries = %d, want 1", len(entries))
	}

	if err := deadLetters.Replay(ctx, "default", entries[0].ID); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	delivery, err := brokerInstance.Reserve(ctx, "default", "replay-consumer")
	if err != nil {
		t.Fatalf("Reserve() replayed task error = %v", err)
	}
	if delivery.Message.ID != original.ID {
		t.Fatalf("replayed task id = %q, want %q", delivery.Message.ID, original.ID)
	}
}

func TestIntegrationWorkerTraceContextSurvivesPublishToExecute(t *testing.T) {
	_, brokerInstance, client := newIntegrationBroker(t, 30*time.Second)
	deadLetters := dlq.NewService(client, brokerInstance, slog.Default())

	provider := sdktrace.NewTracerProvider()
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	rootCtx, rootSpan := provider.Tracer("integration").Start(context.Background(), "publish")
	rootTraceID := rootSpan.SpanContext().TraceID()
	message := broker.TaskMessage{
		ID:        "integration-trace-context",
		Name:      "integration.trace",
		Queue:     "default",
		Payload:   []byte(`{"hello":"trace"}`),
		Headers:   observability.InjectTraceContext(rootCtx, nil),
		CreatedAt: time.Now().UTC(),
	}
	rootSpan.End()

	if _, err := brokerInstance.Publish(rootCtx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	var (
		mu          sync.Mutex
		handlerSpan trace.SpanContext
	)
	worker := newIntegrationWorker(brokerInstance, deadLetters, runtimepkg.HandlerFunc(func(ctx context.Context, msg broker.TaskMessage) error {
		mu.Lock()
		handlerSpan = trace.SpanContextFromContext(ctx)
		mu.Unlock()
		return nil
	}), tasks.DefaultRetryPolicy(1))

	runWorkerUntil(t, worker, func() (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		return handlerSpan.IsValid(), nil
	})

	mu.Lock()
	defer mu.Unlock()
	if handlerSpan.TraceID() != rootTraceID {
		t.Fatalf("handler trace id = %s, want %s", handlerSpan.TraceID(), rootTraceID)
	}
}

func TestIntegrationReclaimAndDeadLetterMetrics(t *testing.T) {
	ctx, _, client := newIntegrationBroker(t, ciLeaseTTL)

	metrics := observability.NewMetrics()
	metricBroker := brokerredis.NewWithOptions(client, slog.Default(), ciLeaseTTL, metrics, brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	})

	message := broker.TaskMessage{
		ID:        "integration-metrics-reclaim",
		Name:      "integration.metrics.reclaim",
		Queue:     "default",
		Payload:   []byte(`{"hello":"reclaim-metrics"}`),
		CreatedAt: time.Now().UTC(),
	}
	if _, err := metricBroker.Publish(ctx, message, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	firstDelivery, err := metricBroker.Reserve(ctx, "default", "consumer-a")
	if err != nil {
		t.Fatalf("Reserve() first consumer error = %v", err)
	}
	time.Sleep(ciWaitForExpiry)
	reclaimedDelivery, err := metricBroker.Reserve(ctx, "default", "consumer-b")
	if err != nil {
		t.Fatalf("Reserve() reclaimed consumer error = %v", err)
	}
	if err := metricBroker.Ack(ctx, reclaimedDelivery); err != nil {
		t.Fatalf("Ack() reclaimed delivery error = %v", err)
	}
	if firstDelivery.Execution.DeliveryID == reclaimedDelivery.Execution.DeliveryID && reclaimedDelivery.Execution.LeaseOwner == firstDelivery.Execution.LeaseOwner {
		t.Fatalf("reclaim did not transfer ownership")
	}

	deadLetterMetrics := observability.NewMetrics()
	deadLetterBroker := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, deadLetterMetrics, brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	})
	deadLetters := dlq.NewService(client, deadLetterBroker, slog.Default())
	worker := &runtimepkg.Worker{
		Broker:      deadLetterBroker,
		DeadLetter:  deadLetters,
		Handler:     runtimepkg.HandlerFunc(func(context.Context, broker.TaskMessage) error { return runtimepkg.Permanent(errors.New("boom")) }),
		Logger:      slog.Default(),
		Metrics:     deadLetterMetrics,
		Clock:       clock.RealClock{},
		RetryPolicy: tasks.DefaultRetryPolicy(1),
		PoolName:    "default",
		Queue:       "default",
		ConsumerID:  "integration-worker",
		LeaseTTL:    30 * time.Second,
		Concurrency: 1,
		Prefetch:    1,
	}

	deadLetterMessage := broker.TaskMessage{
		ID:        "integration-metrics-dead-letter",
		Name:      "integration.metrics.dead_letter",
		Queue:     "default",
		Payload:   []byte(`{"hello":"dead-letter-metrics"}`),
		CreatedAt: time.Now().UTC(),
	}
	if _, err := deadLetterBroker.Publish(ctx, deadLetterMessage, broker.PublishOptions{Source: broker.PublishSourceNew}); err != nil {
		t.Fatalf("Publish() dead-letter message error = %v", err)
	}

	runWorkerUntil(t, worker, func() (bool, error) {
		entries, err := deadLetters.List(ctx, "default", 10)
		if err != nil {
			return false, err
		}
		return len(entries) == 1, nil
	})

	if got := metricCounterValue(t, metrics.Registry, "taskforge_tasks_reclaimed_total", map[string]string{"queue": "default"}); got != 1 {
		t.Fatalf("reclaim counter = %v, want 1", got)
	}
	if got := metricCounterValue(t, deadLetterMetrics.Registry, "taskforge_task_dead_letter_results_total", map[string]string{
		"queue":        "default",
		"task_name":    "integration.metrics.dead_letter",
		"result_class": string(dlq.FailureClassPermanent),
	}); got != 1 {
		t.Fatalf("dead-letter result counter = %v, want 1", got)
	}
}

func newIntegrationBroker(t *testing.T, leaseTTL time.Duration) (context.Context, *brokerredis.RedisBroker, *redis.Client) {
	t.Helper()

	if os.Getenv("TASKFORGE_RUN_INTEGRATION") != "1" {
		t.Skip("set TASKFORGE_RUN_INTEGRATION=1 to run Redis integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	t.Cleanup(func() {
		_ = client.Close()
	})

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB() error = %v", err)
	}

	return ctx, brokerredis.NewWithOptions(client, slog.Default(), leaseTTL, nil, brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	}), client
}

func mustFairnessPolicy(t *testing.T, defaultRule fairness.Rule, rules []fairness.Rule) *fairness.Policy {
	t.Helper()

	policy, err := fairness.NewPolicy(defaultRule, rules)
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	return policy
}

func newIntegrationWorker(b broker.Broker, deadLetters dlq.Publisher, handler runtimepkg.Handler, policy tasks.RetryPolicy) *runtimepkg.Worker {
	return newIntegrationWorkerWithQueue(b, deadLetters, "default", handler, policy)
}

func newIntegrationWorkerWithQueue(b broker.Broker, deadLetters dlq.Publisher, queue string, handler runtimepkg.Handler, policy tasks.RetryPolicy) *runtimepkg.Worker {
	return &runtimepkg.Worker{
		Broker:      b,
		DeadLetter:  deadLetters,
		Handler:     handler,
		Logger:      slog.Default(),
		Metrics:     observability.NewMetrics(),
		Clock:       clock.RealClock{},
		RetryPolicy: policy,
		PoolName:    queue,
		Queue:       queue,
		ConsumerID:  "integration-worker",
		LeaseTTL:    30 * time.Second,
		Concurrency: 1,
		Prefetch:    1,
	}
}

func newIntegrationScheduler(t *testing.T, client *redis.Client, owner string, schedules []schedulerpkg.ScheduleDefinition) *schedulerpkg.Scheduler {
	t.Helper()

	brokerInstance := brokerredis.NewWithOptions(client, slog.Default(), 30*time.Second, nil, brokerredis.Options{
		ReserveTimeout: ciReserveTimeout,
	})
	elector := schedulerpkg.NewRedisLeaderElector(
		client,
		clock.RealClock{},
		slog.Default(),
		owner,
		100*time.Millisecond,
		25*time.Millisecond,
	)
	recurring := schedulerpkg.NewRecurringService(
		brokerInstance,
		schedulerpkg.NewRedisScheduleStateStore(client),
		schedules,
		slog.Default(),
	)
	return schedulerpkg.New(
		brokerInstance,
		recurring,
		elector,
		clock.RealClock{},
		slog.Default(),
		25*time.Millisecond,
		25*time.Millisecond,
	)
}

func runScheduler(ctx context.Context, scheduler *schedulerpkg.Scheduler) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- scheduler.Run(ctx)
	}()
	return errCh
}

func waitForSchedulerStop(t *testing.T, errCh <-chan error) {
	t.Helper()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("scheduler.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop before timeout")
	}
}

func waitForTaskStart(t *testing.T, started <-chan string) string {
	t.Helper()

	select {
	case id := <-started:
		return id
	case <-time.After(time.Second):
		t.Fatal("task did not start before timeout")
		return ""
	}
}

func waitForSchedulerStopIfRunning(t *testing.T, errCh <-chan error) {
	t.Helper()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("scheduler.Run() error = %v", err)
		}
	case <-time.After(100 * time.Millisecond):
	}
}

func waitForStreamLength(t *testing.T, client *redis.Client, streamKey string, expected int64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		count, err := client.XLen(context.Background(), streamKey).Result()
		if err != nil {
			t.Fatalf("XLen() error = %v", err)
		}
		if count >= expected {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("stream %s did not reach length %d before timeout", streamKey, expected)
}

func waitForSchedulerLeaderOwner(t *testing.T, client *redis.Client) string {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		value, err := client.Get(context.Background(), "taskforge:scheduler:leader").Result()
		if err == nil && value != "" {
			return value
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("scheduler leader owner was not observed before timeout")
	return ""
}

func loadStreamTaskMessages(t *testing.T, ctx context.Context, client *redis.Client, streamKey string) []broker.TaskMessage {
	t.Helper()

	entries, err := client.XRange(ctx, streamKey, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange() error = %v", err)
	}

	messages := make([]broker.TaskMessage, 0, len(entries))
	for _, entry := range entries {
		raw, ok := entry.Values["message"]
		if !ok {
			t.Fatalf("stream entry missing message field: %+v", entry.Values)
		}
		payload, ok := raw.(string)
		if !ok {
			t.Fatalf("stream payload type = %T, want string", raw)
		}
		var msg broker.TaskMessage
		if err := json.Unmarshal([]byte(payload), &msg); err != nil {
			t.Fatalf("unmarshal stream task: %v", err)
		}
		messages = append(messages, msg)
	}
	return messages
}

func loadRecurringScheduleState(t *testing.T, ctx context.Context, client *redis.Client, scheduleID string) schedulerpkg.ScheduleState {
	t.Helper()

	payload, err := client.Get(ctx, "taskforge:schedule:state:"+scheduleID).Bytes()
	if err != nil {
		t.Fatalf("Get() recurring schedule state error = %v", err)
	}

	var state schedulerpkg.ScheduleState
	if err := json.Unmarshal(payload, &state); err != nil {
		t.Fatalf("unmarshal recurring schedule state: %v", err)
	}
	return state
}

func mustParseRFC3339Time(t *testing.T, value string) time.Time {
	t.Helper()

	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("parse RFC3339 time %q: %v", value, err)
	}
	return parsed
}

func metricCounterValue(t *testing.T, registry interface {
	Gather() ([]*dto.MetricFamily, error)
}, familyName string, labels map[string]string) float64 {
	t.Helper()

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	for _, family := range families {
		if family.GetName() != familyName {
			continue
		}
		for _, metric := range family.GetMetric() {
			if hasLabels(metric, labels) {
				return metric.GetCounter().GetValue()
			}
		}
	}

	t.Fatalf("metric %s with labels %v not found", familyName, labels)
	return 0
}

func hasLabels(metric *dto.Metric, labels map[string]string) bool {
	if len(metric.GetLabel()) != len(labels) {
		return false
	}
	for _, label := range metric.GetLabel() {
		if labels[label.GetName()] != label.GetValue() {
			return false
		}
	}
	return true
}

func runWorkerUntil(t *testing.T, worker *runtimepkg.Worker, condition func() (bool, error)) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok, err := condition()
		if err != nil {
			t.Fatalf("condition error = %v", err)
		}
		if ok {
			cancel()
			select {
			case err := <-errCh:
				if err != nil {
					t.Fatalf("worker.Run() error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatalf("worker did not stop after cancel")
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("worker.Run() error = %v", err)
		}
	default:
	}
	t.Fatalf("condition was not met before timeout")
}

func runManagerUntil(t *testing.T, manager *runtimepkg.Manager, condition func() (bool, error)) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Run(ctx)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok, err := condition()
		if err != nil {
			t.Fatalf("condition error = %v", err)
		}
		if ok {
			cancel()
			select {
			case err := <-errCh:
				if err != nil {
					t.Fatalf("manager.Run() error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatalf("manager did not stop after cancel")
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("manager.Run() error = %v", err)
		}
	default:
	}
	t.Fatalf("condition was not met before timeout")
}
