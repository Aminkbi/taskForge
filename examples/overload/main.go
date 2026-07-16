// The overload demo is the supported TaskForge adoption path. It uses a local
// Redis instance, but its handler has no network or third-party dependency.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aminkbi/taskforge"
	taskforgeredis "github.com/aminkbi/taskforge/redis"
	"github.com/aminkbi/taskforge/worker"
)

const (
	demoQueue       = "demo-overload"
	demoTaskName    = "demo.overload.process"
	protectedTenant = "protected-tenant"
	noisyTenant     = "noisy-tenant"
)

type options struct {
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	Timeout       time.Duration
}

type result struct {
	CompletedTaskIDs              []string                            `json:"completed_task_ids"`
	MaxNoisyTenantConcurrency     int                                 `json:"max_noisy_tenant_concurrency"`
	ProtectedStartedWithNoisyWork bool                                `json:"protected_started_with_noisy_work"`
	QueueMetricsWhileBlocked      taskforge.QueueMetricsSnapshot      `json:"queue_metrics_while_blocked"`
	FairnessMetricsWhileBlocked   []taskforge.FairnessMetricsSnapshot `json:"fairness_metrics_while_blocked"`
	TaskStates                    map[string]taskforge.State          `json:"task_states"`
	PrometheusMetricsExposed      bool                                `json:"prometheus_metrics_exposed"`
}

func main() {
	if err := run(context.Background(), os.Stdout, loadOptions()); err != nil {
		fmt.Fprintf(os.Stderr, "overload demo: %v\n", err)
		os.Exit(1)
	}
}

func loadOptions() options {
	redisDB := 15
	if value := os.Getenv("TASKFORGE_DEMO_REDIS_DB"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			redisDB = parsed
		}
	}
	addr := os.Getenv("TASKFORGE_DEMO_REDIS_ADDR")
	if addr == "" {
		addr = os.Getenv("TASKFORGE_REDIS_ADDR")
	}
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	password := os.Getenv("TASKFORGE_DEMO_REDIS_PASSWORD")
	if password == "" {
		password = os.Getenv("TASKFORGE_REDIS_PASSWORD")
	}
	return options{
		RedisAddr:     addr,
		RedisPassword: password,
		RedisDB:       redisDB,
		Timeout:       10 * time.Second,
	}
}

func run(parent context.Context, output io.Writer, options options) (runErr error) {
	if options.Timeout <= 0 {
		options.Timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, options.Timeout)
	defer cancel()

	policy, err := taskforgeredis.NewFairnessPolicy(
		taskforgeredis.FairnessRule{Name: "shared", Weight: 1},
		[]taskforgeredis.FairnessRule{
			{Name: "protected", Keys: []string{protectedTenant}, ReservedConcurrency: 1, HardQuota: 1},
			{Name: "noisy", Keys: []string{noisyTenant}, HardQuota: 1},
		},
	)
	if err != nil {
		return fmt.Errorf("build fairness policy: %w", err)
	}
	broker := taskforgeredis.New(taskforgeredis.Options{
		Addr:           options.RedisAddr,
		Password:       options.RedisPassword,
		DB:             options.RedisDB,
		LeaseTTL:       time.Second,
		ReserveTimeout: 20 * time.Millisecond,
		FairnessPolicies: map[string]*taskforgeredis.FairnessPolicy{
			demoQueue: policy,
		},
		Retention: taskforge.RetentionPolicy{SucceededState: time.Minute},
	})
	defer broker.Close()
	if err := broker.Ping(ctx); err != nil {
		return fmt.Errorf("connect to Redis at %s: %w", options.RedisAddr, err)
	}

	handler := newGateHandler()
	runtime, err := worker.New(worker.Options{
		Broker:      broker,
		Handler:     handler,
		Queue:       demoQueue,
		PoolName:    "overload-demo",
		ConsumerID:  "overload-demo",
		Concurrency: 2,
		Prefetch:    2,
		LeaseTTL:    time.Second,
	})
	if err != nil {
		return fmt.Errorf("build worker: %w", err)
	}

	runCtx, stopWorker := context.WithCancel(ctx)
	workerErrors := make(chan error, 1)
	go func() { workerErrors <- runtime.Run(runCtx) }()
	defer func() {
		stopWorker()
		if err := <-workerErrors; err != nil && runErr == nil {
			runErr = fmt.Errorf("run worker: %w", err)
		}
	}()

	tasks := []taskforge.Task{
		taskforge.NewTask(demoTaskName, nil,
			taskforge.WithID("demo-noisy-1"), taskforge.WithQueue(demoQueue), taskforge.WithFairnessKey(noisyTenant)),
		taskforge.NewTask(demoTaskName, nil,
			taskforge.WithID("demo-noisy-2"), taskforge.WithQueue(demoQueue), taskforge.WithFairnessKey(noisyTenant)),
		taskforge.NewTask(demoTaskName, nil,
			taskforge.WithID("demo-protected-1"), taskforge.WithQueue(demoQueue), taskforge.WithFairnessKey(protectedTenant)),
	}
	for _, task := range tasks {
		if _, err := broker.Publish(ctx, task, taskforge.PublishOptions{}); err != nil {
			return fmt.Errorf("publish %s: %w", task.ID, err)
		}
	}

	firstWave, err := handler.waitForStarts(ctx, 2)
	if err != nil {
		return err
	}
	protectedStartedWithNoisyWork := containsTenant(firstWave, protectedTenant) && containsTenant(firstWave, noisyTenant)
	if !protectedStartedWithNoisyWork {
		return fmt.Errorf("fairness protection failed: first wave = %v, want protected and noisy tenants", firstWave)
	}
	queueMetrics, err := broker.QueueMetricsSnapshot(ctx, demoQueue)
	if err != nil {
		return fmt.Errorf("read queue metrics: %w", err)
	}
	fairnessMetrics, err := broker.FairnessMetricsSnapshot(ctx, demoQueue, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("read fairness metrics: %w", err)
	}
	if !hasBucketDepth(fairnessMetrics, "noisy", 1) {
		return fmt.Errorf("fairness protection failed: noisy tenant did not retain one queued task: %v", fairnessMetrics)
	}

	handler.releaseFirstWave()
	completed := taskIDs(tasks)
	if err := waitForSucceeded(ctx, broker, completed); err != nil {
		return err
	}
	maxNoisyConcurrency := handler.maxNoisyConcurrency()
	if maxNoisyConcurrency != 1 {
		return fmt.Errorf("fairness protection failed: noisy tenant concurrency = %d, want 1", maxNoisyConcurrency)
	}

	states := make(map[string]taskforge.State, len(tasks))
	for _, task := range tasks {
		record, err := broker.Get(ctx, task.ID)
		if err != nil {
			return fmt.Errorf("read task state for %s: %w", task.ID, err)
		}
		states[task.ID] = record.State
	}
	metricsResponse := httptest.NewRecorder()
	broker.MetricsHandler().ServeHTTP(metricsResponse, httptest.NewRequest("GET", "/metrics", nil))
	if metricsResponse.Code != 200 || metricsResponse.Body.Len() == 0 {
		return fmt.Errorf("expose Prometheus metrics: status=%d bytes=%d", metricsResponse.Code, metricsResponse.Body.Len())
	}

	if err := json.NewEncoder(output).Encode(result{
		CompletedTaskIDs:              completed,
		MaxNoisyTenantConcurrency:     maxNoisyConcurrency,
		ProtectedStartedWithNoisyWork: protectedStartedWithNoisyWork,
		QueueMetricsWhileBlocked:      queueMetrics,
		FairnessMetricsWhileBlocked:   fairnessMetrics,
		TaskStates:                    states,
		PrometheusMetricsExposed:      true,
	}); err != nil {
		return fmt.Errorf("write result: %w", err)
	}
	return nil
}

type taskStart struct {
	TaskID string
	Tenant string
}

type gateHandler struct {
	mu             sync.Mutex
	starts         chan taskStart
	release        chan struct{}
	firstWaveCount int
	noisyActive    int
	maxNoisyActive int
}

func newGateHandler() *gateHandler {
	return &gateHandler{
		starts:  make(chan taskStart, 3),
		release: make(chan struct{}),
	}
}

func (h *gateHandler) HandleTask(ctx context.Context, task taskforge.Task) error {
	tenant := task.FairnessKey
	h.mu.Lock()
	if tenant == noisyTenant {
		h.noisyActive++
		if h.noisyActive > h.maxNoisyActive {
			h.maxNoisyActive = h.noisyActive
		}
	}
	block := h.firstWaveCount < 2
	if block {
		h.firstWaveCount++
	}
	h.mu.Unlock()
	defer func() {
		if tenant == noisyTenant {
			h.mu.Lock()
			h.noisyActive--
			h.mu.Unlock()
		}
	}()

	h.starts <- taskStart{TaskID: task.ID, Tenant: tenant}
	if !block {
		return nil
	}
	select {
	case <-h.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *gateHandler) waitForStarts(ctx context.Context, count int) ([]taskStart, error) {
	starts := make([]taskStart, 0, count)
	for len(starts) < count {
		select {
		case start := <-h.starts:
			starts = append(starts, start)
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for first worker wave: %w", ctx.Err())
		}
	}
	return starts, nil
}

func (h *gateHandler) releaseFirstWave() { close(h.release) }

func (h *gateHandler) maxNoisyConcurrency() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxNoisyActive
}

func containsTenant(starts []taskStart, tenant string) bool {
	for _, start := range starts {
		if start.Tenant == tenant {
			return true
		}
	}
	return false
}

func hasBucketDepth(snapshots []taskforge.FairnessMetricsSnapshot, bucket string, depth float64) bool {
	for _, snapshot := range snapshots {
		if snapshot.Bucket == bucket && snapshot.Depth == depth {
			return true
		}
	}
	return false
}

func waitForSucceeded(ctx context.Context, broker *taskforgeredis.Broker, ids []string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		allSucceeded := true
		for _, id := range ids {
			record, err := broker.Get(ctx, id)
			if err != nil {
				return fmt.Errorf("read task state for %s: %w", id, err)
			}
			if record.State != taskforge.StateSucceeded {
				allSucceeded = false
				break
			}
		}
		if allSucceeded {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for task completion: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func taskIDs(tasks []taskforge.Task) []string {
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.ID)
	}
	return ids
}
