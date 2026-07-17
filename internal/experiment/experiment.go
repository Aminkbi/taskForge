// Package experiment defines the durable input and output contract for
// comparative TaskForge experiments. It deliberately contains no benchmark
// conclusions: a run is evidence only after its raw samples are present.
package experiment

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"
)

const SchemaVersion = "taskforge-experiment/v2"

type Manifest struct {
	Name               string        `json:"name"`
	Description        string        `json:"description"`
	Tasks              int           `json:"tasks"`
	Tenants            []Tenant      `json:"tenants"`
	ServiceTime        time.Duration `json:"service_time"`
	SLO                time.Duration `json:"slo"`
	RetryFraction      float64       `json:"retry_fraction"`
	DelayedFraction    float64       `json:"delayed_fraction"`
	AbandonReservation bool          `json:"abandon_reservation"`
	// Control parameters are defined at scale 1. Admission caps scale with the
	// task count; the dependency budget models a fixed-capacity downstream and
	// deliberately does not scale with offered load.
	DependencyBudgetCapacity  int   `json:"dependency_budget_capacity,omitempty"`
	AdmissionMaxPending       int64 `json:"admission_max_pending,omitempty"`
	AdmissionMaxPendingPerKey int64 `json:"admission_max_pending_per_key,omitempty"`
}

// Scale multiplies the offered load and the load-relative admission caps by
// factor, leaving per-task timings, SLOs, and fixed downstream capacity as
// manifest-defined.
func (m Manifest) Scale(factor int) Manifest {
	if factor <= 1 {
		return m
	}
	m.Tasks *= factor
	m.AdmissionMaxPending *= int64(factor)
	m.AdmissionMaxPendingPerKey *= int64(factor)
	return m
}

type Tenant struct {
	Name   string  `json:"name"`
	Weight float64 `json:"weight"`
	// FairnessWeight separates a tenant's service entitlement from its offered
	// load when they differ (a noisy neighbor offers far more than its share).
	// Zero means the offered weight is also the entitlement.
	FairnessWeight float64 `json:"fairness_weight,omitempty"`
}

func (t Tenant) EffectiveFairnessWeight() float64 {
	if t.FairnessWeight > 0 {
		return t.FairnessWeight
	}
	return t.Weight
}

func (m Manifest) Validate() error {
	if m.Name == "" || m.Tasks < 1 || len(m.Tenants) == 0 || m.ServiceTime < 0 || m.SLO <= 0 {
		return fmt.Errorf("invalid manifest %q", m.Name)
	}
	var weight float64
	for _, tenant := range m.Tenants {
		if tenant.Name == "" || tenant.Weight <= 0 || tenant.FairnessWeight < 0 {
			return fmt.Errorf("invalid tenant in %q", m.Name)
		}
		weight += tenant.Weight
	}
	if weight == 0 || m.RetryFraction < 0 || m.RetryFraction > 1 || m.DelayedFraction < 0 || m.DelayedFraction > 1 {
		return fmt.Errorf("invalid fractions in %q", m.Name)
	}
	if m.DependencyBudgetCapacity < 0 || m.AdmissionMaxPending < 0 || m.AdmissionMaxPendingPerKey < 0 {
		return fmt.Errorf("invalid control parameters in %q", m.Name)
	}
	return nil
}

type Variant struct {
	Name        string   `json:"name"`
	System      string   `json:"system"`
	Controls    []string `json:"controls"`
	Disabled    []string `json:"disabled_controls,omitempty"`
	Comparable  bool     `json:"comparable"`
	Limitations []string `json:"limitations,omitempty"`
}

func Variants() []Variant {
	return []Variant{
		{Name: "taskforge-fifo-static", System: "taskforge", Controls: []string{"fifo", "static-concurrency"}, Comparable: true},
		{Name: "taskforge-no-fairness", System: "taskforge", Controls: []string{"admission", "adaptive-concurrency", "dependency-budget"}, Disabled: []string{"fairness"}, Comparable: true},
		{Name: "taskforge-no-admission", System: "taskforge", Controls: []string{"fairness", "adaptive-concurrency", "dependency-budget"}, Disabled: []string{"admission"}, Comparable: true},
		{Name: "taskforge-no-adaptive", System: "taskforge", Controls: []string{"fairness", "admission", "static-concurrency", "dependency-budget"}, Disabled: []string{"adaptive-concurrency"}, Comparable: true},
		{Name: "taskforge-no-dependency-budget", System: "taskforge", Controls: []string{"fairness", "admission", "adaptive-concurrency"}, Disabled: []string{"dependency-budget"}, Comparable: true},
		{Name: "taskforge-full", System: "taskforge", Controls: []string{"fairness", "admission", "adaptive-concurrency", "dependency-budget"}, Comparable: true},
		{Name: "asynq", System: "asynq", Controls: []string{"redis-backed", "static-concurrency"}, Comparable: false, Limitations: []string{"Asynq has no equivalent TaskForge tenant-fairness, admission, adaptive, or dependency-budget control; compare common delivery metrics only."}},
	}
}

type Sample struct {
	TaskID      string    `json:"task_id"`
	Tenant      string    `json:"tenant"`
	EnqueuedAt  time.Time `json:"enqueued_at"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	Attempts    int       `json:"attempts"`
	Duplicate   bool      `json:"duplicate"`
	SLOViolated bool      `json:"slo_violated"`
}

type Percentiles struct {
	P50 time.Duration `json:"p50"`
	P95 time.Duration `json:"p95"`
	P99 time.Duration `json:"p99"`
}
type RedisMetrics struct {
	CPUSeconds      float64 `json:"cpu_seconds"`
	UsedMemoryBytes int64   `json:"used_memory_bytes"`
	TotalCommands   int64   `json:"total_commands_processed"`
}
type Environment struct {
	BuildSHA     string `json:"build_sha"`
	Hostname     string `json:"hostname"`
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	GoVersion    string `json:"go_version"`
	CPUs         int    `json:"cpus"`
	RedisConfig  string `json:"redis_config"`
}
type Summary struct {
	EnqueueToStart       Percentiles   `json:"enqueue_to_start"`
	Completion           Percentiles   `json:"completion"`
	Throughput           float64       `json:"throughput_per_second"`
	JainFairness         float64       `json:"jain_fairness"`
	StarvationViolations int           `json:"starvation_slo_violations"`
	Retries              int           `json:"retries"`
	Duplicates           int           `json:"duplicates"`
	RecoveryTime         time.Duration `json:"recovery_time"`
	Redis                RedisMetrics  `json:"redis"`
}
type Result struct {
	Schema      string      `json:"schema"`
	StartedAt   time.Time   `json:"started_at"`
	FinishedAt  time.Time   `json:"finished_at"`
	Seed        int64       `json:"seed"`
	Manifest    Manifest    `json:"manifest"`
	Variant     Variant     `json:"variant"`
	Environment Environment `json:"environment"`
	Summary     Summary     `json:"summary"`
	Samples     []Sample    `json:"samples"`
}

func NewEnvironment(buildSHA, redisConfig string) Environment {
	hostname, _ := os.Hostname()
	return Environment{BuildSHA: buildSHA, Hostname: hostname, OS: runtime.GOOS, Architecture: runtime.GOARCH, GoVersion: runtime.Version(), CPUs: runtime.NumCPU(), RedisConfig: redisConfig}
}

// Summarize uses only observed enqueue and completion times for throughput.
// Runner startup/shutdown must not dilute a measured rate. Jain fairness is
// based on each tenant's SLO-compliant completion ratio, rather than offered
// task counts (which would call a deliberately skewed workload unfair).
func Summarize(samples []Sample, tenants []Tenant, redis RedisMetrics) Summary {
	queue := make([]time.Duration, 0, len(samples))
	completion := make([]time.Duration, 0, len(samples))
	offered := map[string]float64{}
	withinSLO := map[string]float64{}
	var firstEnqueued, lastCompleted time.Time
	s := Summary{Redis: redis}
	for _, sample := range samples {
		queue = append(queue, sample.StartedAt.Sub(sample.EnqueuedAt))
		completion = append(completion, sample.CompletedAt.Sub(sample.EnqueuedAt))
		offered[sample.Tenant]++
		if firstEnqueued.IsZero() || sample.EnqueuedAt.Before(firstEnqueued) {
			firstEnqueued = sample.EnqueuedAt
		}
		if sample.CompletedAt.After(lastCompleted) {
			lastCompleted = sample.CompletedAt
		}
		if sample.SLOViolated {
			s.StarvationViolations++
		} else {
			withinSLO[sample.Tenant]++
		}
		s.Retries += max(sample.Attempts-1, 0)
		if sample.Duplicate {
			s.Duplicates++
		}
	}
	s.EnqueueToStart = percentiles(queue)
	s.Completion = percentiles(completion)
	if elapsed := lastCompleted.Sub(firstEnqueued).Seconds(); elapsed > 0 {
		s.Throughput = float64(len(samples)) / elapsed
	}
	var sum, squares float64
	activeTenants := 0
	for _, tenant := range tenants {
		if offered[tenant.Name] == 0 {
			continue
		}
		progress := withinSLO[tenant.Name] / offered[tenant.Name]
		sum += progress
		squares += progress * progress
		activeTenants++
	}
	if squares > 0 && activeTenants > 0 {
		s.JainFairness = sum * sum / (float64(activeTenants) * squares)
	}
	return s
}

func percentiles(values []time.Duration) Percentiles {
	if len(values) == 0 {
		return Percentiles{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	pick := func(p float64) time.Duration { return values[int(math.Ceil(p*float64(len(values))))-1] }
	return Percentiles{P50: pick(.50), P95: pick(.95), P99: pick(.99)}
}

// WriteResult stores one raw run. Compact encoding is for results that are
// committed as research evidence; the content is identical either way.
func WriteResult(dir string, result Result, compact bool) (string, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	name := fmt.Sprintf("%s--%s--%d.json", result.Manifest.Name, result.Variant.Name, result.Seed)
	path := filepath.Join(dir, name)
	var data []byte
	var err error
	if compact {
		data, err = json.Marshal(result)
	} else {
		data, err = json.MarshalIndent(result, "", "  ")
	}
	if err != nil {
		return "", err
	}
	return path, os.WriteFile(path, append(data, '\n'), 0644)
}
