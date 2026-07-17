// taskforge-experiment runs a small, reproducible comparative workload. It is
// intentionally an experiment runner, not a product benchmark or an SLA tool.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	gredis "github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/experiment"
	tfredis "github.com/aminkbi/taskforge/redis"
)

type event struct {
	ID       string    `json:"id"`
	Tenant   string    `json:"tenant"`
	Enqueued time.Time `json:"enqueued"`
}
type tracker struct {
	mu       sync.Mutex
	first    map[string]time.Time
	samples  map[string]experiment.Sample
	attempts map[string]int
	crashed  map[string]time.Time
	recovery time.Duration
}

func newTracker() *tracker {
	return &tracker{first: map[string]time.Time{}, samples: map[string]experiment.Sample{}, attempts: map[string]int{}, crashed: map[string]time.Time{}}
}
func (t *tracker) start(e event) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.attempts[e.ID]++
	if crashedAt := t.crashed[e.ID]; !crashedAt.IsZero() && t.attempts[e.ID] > 1 {
		t.recovery = time.Since(crashedAt)
		delete(t.crashed, e.ID)
	}
	if t.first[e.ID].IsZero() {
		t.first[e.ID] = time.Now().UTC()
	}
	return t.attempts[e.ID]
}
func (t *tracker) crash(e event)               { t.mu.Lock(); defer t.mu.Unlock(); t.crashed[e.ID] = time.Now() }
func (t *tracker) recoveryTime() time.Duration { t.mu.Lock(); defer t.mu.Unlock(); return t.recovery }
func (t *tracker) done(e event, slo time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now().UTC()
	attempts := t.attempts[e.ID]
	started := t.first[e.ID]
	t.samples[e.ID] = experiment.Sample{TaskID: e.ID, Tenant: e.Tenant, EnqueuedAt: e.Enqueued, StartedAt: started, CompletedAt: now, Attempts: attempts, Duplicate: attempts > 1, SLOViolated: now.Sub(e.Enqueued) > slo}
}
func (t *tracker) all() []experiment.Sample {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]experiment.Sample, 0, len(t.samples))
	for _, sample := range t.samples {
		result = append(result, sample)
	}
	slices.SortFunc(result, func(a, b experiment.Sample) int { return strings.Compare(a.TaskID, b.TaskID) })
	return result
}
func (t *tracker) completed() int { t.mu.Lock(); defer t.mu.Unlock(); return len(t.samples) }

func main() {
	workloads := flag.String("workloads", "test/experiment/workloads", "directory containing JSON manifests")
	out := flag.String("output", "artifacts/experiments/raw", "raw JSON output directory")
	smoke := flag.Bool("smoke", false, "run smoke-sized manifests")
	manifestFilter := flag.String("manifest", "", "run one workload manifest by name")
	variantFilter := flag.String("variant", "", "run one variant by name")
	seed := flag.Int64("seed", 20260717, "deterministic workload seed")
	addr := flag.String("redis-addr", env("TASKFORGE_REDIS_ADDR", "localhost:6379"), "Redis address")
	db := flag.Int("redis-db", envInt("TASKFORGE_EXPERIMENT_REDIS_DB", 14), "dedicated Redis DB; must not contain production data")
	flag.Parse()
	if *db < 1 {
		fatal("refusing DB %d; choose a dedicated non-zero experiment DB", *db)
	}
	manifests, err := load(*workloads, *smoke)
	if err != nil {
		fatal("load manifests: %v", err)
	}
	manifests = filterManifests(manifests, *manifestFilter)
	variants := filterVariants(experiment.Variants(), *variantFilter)
	if len(manifests) == 0 || len(variants) == 0 {
		fatal("manifest or variant filter matched nothing")
	}
	client := gredis.NewClient(&gredis.Options{Addr: *addr, DB: *db})
	defer client.Close()
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		fatal("connect Redis: %v", err)
	}
	build := buildSHA()
	redisConfig := redisConfiguration(ctx, client, *addr, *db)
	for _, manifest := range manifests {
		for _, variant := range variants {
			if err := client.FlushDB(ctx).Err(); err != nil {
				fatal("clear dedicated experiment DB: %v", err)
			}
			started := time.Now().UTC()
			redisBefore := redisMetrics(ctx, client)
			tracker := newTracker()
			var runErr error
			if variant.System == "taskforge" {
				runErr = runTaskForge(ctx, client, manifest, variant, *seed, tracker)
			} else {
				runErr = runAsynq(manifest, *addr, *db, *seed, tracker)
			}
			finished := time.Now().UTC()
			if runErr != nil {
				fatal("%s/%s: %v", manifest.Name, variant.Name, runErr)
			}
			redis := redisDelta(redisBefore, redisMetrics(ctx, client))
			samples := tracker.all()
			if len(samples) != manifest.Tasks {
				fatal("%s/%s completed %d of %d tasks", manifest.Name, variant.Name, len(samples), manifest.Tasks)
			}
			result := experiment.Result{Schema: experiment.SchemaVersion, StartedAt: started, FinishedAt: finished, Seed: *seed, Manifest: manifest, Variant: variant, Environment: experiment.NewEnvironment(build, redisConfig), Samples: samples}
			result.Summary = experiment.Summarize(samples, manifest.Tenants, redis)
			result.Summary.RecoveryTime = tracker.recoveryTime()
			path, err := experiment.WriteResult(*out, result)
			if err != nil {
				fatal("write result: %v", err)
			}
			fmt.Println(path)
		}
	}
}

func runTaskForge(ctx context.Context, client *gredis.Client, m experiment.Manifest, v experiment.Variant, seed int64, tr *tracker) error {
	options := tfredis.Options{Client: client, LeaseTTL: 25 * time.Millisecond, ReserveTimeout: time.Second, Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))}
	if !has(v.Disabled, "fairness") && v.Name != "taskforge-fifo-static" {
		rules := make([]tfredis.FairnessRule, 0, len(m.Tenants))
		for _, tenant := range m.Tenants {
			rules = append(rules, tfredis.FairnessRule{Name: tenant.Name, Keys: []string{tenant.Name}, Weight: max(1, int(tenant.Weight))})
		}
		policy, err := tfredis.NewFairnessPolicy(tfredis.FairnessRule{}, rules)
		if err != nil {
			return err
		}
		options.FairnessPolicies = map[string]*tfredis.FairnessPolicy{"default": policy}
	}
	broker := tfredis.New(options)
	events := makeEvents(m, seed)
	for i, e := range events {
		e.Enqueued = time.Now().UTC()
		task := taskforge.Task{ID: e.ID, Name: "experiment.task", Queue: "default", FairnessKey: e.Tenant, Payload: mustJSON(e), CreatedAt: e.Enqueued}
		if m.DelayedFraction > 0 && float64(i)/float64(len(events)) < m.DelayedFraction {
			eta := time.Now().UTC().Add(12 * time.Millisecond)
			task.ETA = &eta
		}
		if _, err := broker.Publish(ctx, task, taskforge.PublishOptions{Source: taskforge.PublishSourceNew}); err != nil {
			return err
		}
	}
	fence := taskforge.LeadershipFence{Owner: "experiment", Epoch: 1, Token: "experiment|1"}
	if err := client.Set(ctx, "taskforge:v2:scheduler:leader", fence.Token, time.Minute).Err(); err != nil {
		return err
	}
	if err := client.Set(ctx, "taskforge:v2:scheduler:leader:epoch", fence.Epoch, 0).Err(); err != nil {
		return err
	}
	workCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	stopScheduler := make(chan struct{})
	var schedulerWG sync.WaitGroup
	schedulerWG.Add(1)
	go func() {
		defer schedulerWG.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopScheduler:
				return
			case <-ticker.C:
				_, _ = broker.MoveDue(ctx, fence, time.Now().UTC(), 64)
			}
		}
	}()
	var wg sync.WaitGroup
	var crash sync.Once
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for {
				select {
				case <-workCtx.Done():
					return
				default:
				}
				delivery, err := broker.Reserve(workCtx, "default", fmt.Sprintf("experiment-%d", n))
				if errors.Is(err, taskforge.ErrNoTask) {
					time.Sleep(time.Millisecond)
					continue
				}
				if err != nil {
					return
				}
				var e event
				if json.Unmarshal(delivery.Message.Payload, &e) != nil {
					return
				}
				attempt := tr.start(e)
				if m.CrashAfterStarts > 0 && attempt == 1 {
					crashed := false
					crash.Do(func() { crashed = true })
					if crashed {
						tr.crash(e)
						continue
					}
				}
				retry := m.RetryFraction > 0 && eventIndex(e.ID)%100 < int(m.RetryFraction*100) && attempt == 1
				if retry {
					_ = broker.Nack(ctx, delivery, true)
					continue
				}
				time.Sleep(m.ServiceTime)
				if broker.Ack(ctx, delivery) == nil {
					tr.done(e, m.SLO)
				}
			}
		}(n)
	}
	if err := waitFor(tr, m.Tasks, 3*time.Second); err != nil {
		cancelWorkers()
		close(stopScheduler)
		schedulerWG.Wait()
		wg.Wait()
		return err
	}
	cancelWorkers()
	close(stopScheduler)
	schedulerWG.Wait()
	wg.Wait()
	return nil
}

// runAsynq is the deliberately isolated baseline adapter. It shares only the
// manifest, event payload, and raw-sample contract with the TaskForge runner.
func runAsynq(m experiment.Manifest, addr string, db int, seed int64, tr *tracker) error {
	opt := asynq.RedisClientOpt{Addr: addr, DB: db}
	server := asynq.NewServer(opt, asynq.Config{Concurrency: 4, TaskCheckInterval: 2 * time.Millisecond, DelayedTaskCheckInterval: 5 * time.Millisecond, RetryDelayFunc: func(int, error, *asynq.Task) time.Duration { return 5 * time.Millisecond }, LogLevel: asynq.ErrorLevel})
	mux := asynq.NewServeMux()
	mux.HandleFunc("experiment.task", func(_ context.Context, task *asynq.Task) error {
		var e event
		if err := json.Unmarshal(task.Payload(), &e); err != nil {
			return err
		}
		attempt := tr.start(e)
		if m.RetryFraction > 0 && eventIndex(e.ID)%100 < int(m.RetryFraction*100) && attempt == 1 {
			return errors.New("experiment retry")
		}
		time.Sleep(m.ServiceTime)
		tr.done(e, m.SLO)
		return nil
	})
	errCh := make(chan error, 1)
	go func() { errCh <- server.Run(mux) }()
	client := asynq.NewClient(opt)
	defer client.Close()
	for i, e := range makeEvents(m, seed) {
		e.Enqueued = time.Now().UTC()
		opts := []asynq.Option{asynq.Queue("default"), asynq.MaxRetry(1)}
		if m.DelayedFraction > 0 && float64(i)/float64(m.Tasks) < m.DelayedFraction {
			opts = append(opts, asynq.ProcessIn(12*time.Millisecond))
		}
		if _, err := client.Enqueue(asynq.NewTask("experiment.task", mustJSON(e)), opts...); err != nil {
			server.Shutdown()
			return err
		}
	}
	err := waitFor(tr, m.Tasks, 3*time.Second)
	server.Shutdown()
	select {
	case runErr := <-errCh:
		if runErr != nil && err == nil {
			err = runErr
		}
	case <-time.After(time.Second):
	}
	return err
}

func makeEvents(m experiment.Manifest, seed int64) []event {
	r := rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0x9e3779b97f4a7c15))
	total := 0.0
	for _, t := range m.Tenants {
		total += t.Weight
	}
	result := make([]event, m.Tasks)
	for i := range result {
		pick := r.Float64() * total
		tenant := m.Tenants[len(m.Tenants)-1].Name
		for _, candidate := range m.Tenants {
			pick -= candidate.Weight
			if pick <= 0 {
				tenant = candidate.Name
				break
			}
		}
		result[i] = event{ID: fmt.Sprintf("%s-%d", m.Name, i), Tenant: tenant, Enqueued: time.Now().UTC()}
	}
	return result
}
func waitFor(tr *tracker, tasks int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tr.completed() == tasks {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %d tasks; completed %d", tasks, tr.completed())
}
func load(dir string, smoke bool) ([]experiment.Manifest, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	if len(paths) != 6 {
		return nil, fmt.Errorf("want six workload manifests, found %d", len(paths))
	}
	result := make([]experiment.Manifest, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var m experiment.Manifest
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		if err := m.Validate(); err != nil {
			return nil, err
		}
		if smoke && m.Tasks > 8 {
			m.Tasks = 8
		}
		result = append(result, m)
	}
	return result, nil
}
func filterManifests(manifests []experiment.Manifest, name string) []experiment.Manifest {
	if name == "" {
		return manifests
	}
	result := make([]experiment.Manifest, 0, 1)
	for _, manifest := range manifests {
		if manifest.Name == name {
			result = append(result, manifest)
		}
	}
	return result
}
func filterVariants(variants []experiment.Variant, name string) []experiment.Variant {
	if name == "" {
		return variants
	}
	result := make([]experiment.Variant, 0, 1)
	for _, variant := range variants {
		if variant.Name == name {
			result = append(result, variant)
		}
	}
	return result
}
func redisMetrics(ctx context.Context, c *gredis.Client) experiment.RedisMetrics {
	info, _ := c.Info(ctx, "memory", "stats", "cpu").Result()
	return experiment.RedisMetrics{CPUSeconds: infoFloat(info, "used_cpu_sys") + infoFloat(info, "used_cpu_user"), UsedMemoryBytes: int64(infoFloat(info, "used_memory")), TotalCommands: int64(infoFloat(info, "total_commands_processed"))}
}
func redisDelta(before, after experiment.RedisMetrics) experiment.RedisMetrics {
	return experiment.RedisMetrics{CPUSeconds: after.CPUSeconds - before.CPUSeconds, UsedMemoryBytes: after.UsedMemoryBytes, TotalCommands: after.TotalCommands - before.TotalCommands}
}
func redisConfiguration(ctx context.Context, c *gredis.Client, addr string, db int) string {
	config, err := c.ConfigGet(ctx, "*").Result()
	if err != nil {
		return fmt.Sprintf("addr=%s db=%d config_error=%q", addr, db, err)
	}
	selected := map[string]string{"addr": addr, "db": strconv.Itoa(db)}
	for _, key := range []string{"appendonly", "appendfsync", "maxmemory-policy", "save"} {
		selected[key] = config[key]
	}
	encoded, err := json.Marshal(selected)
	if err != nil {
		return fmt.Sprintf("addr=%s db=%d config_encode_error=%q", addr, db, err)
	}
	return string(encoded)
}
func infoFloat(info, key string) float64 {
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, key+":") {
			value, _ := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, key+":")), 64)
			return value
		}
	}
	return 0
}
func eventIndex(id string) int {
	at := strings.LastIndex(id, "-")
	n, _ := strconv.Atoi(id[at+1:])
	return n
}
func has(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func envInt(key string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(key)); err == nil && value > 0 {
		return value
	}
	return fallback
}
func buildSHA() string {
	if value := os.Getenv("TASKFORGE_BUILD_SHA"); value != "" {
		return value
	}
	return "unknown"
}
func fatal(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...); os.Exit(1) }
