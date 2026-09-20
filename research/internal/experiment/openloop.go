package experiment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	OpenLoopTraceSchema  = "taskforge-open-loop-trace/v1"
	OpenLoopResultSchema = "taskforge-open-loop-result/v1"
)

// OpenLoopProfile describes offered load independently of any queue system.
// Phases are consecutive, allowing both sustained overload and explicit bursts.
type OpenLoopProfile struct {
	Name             string            `json:"name"`
	Warmup           time.Duration     `json:"warmup"`
	SteadyState      time.Duration     `json:"steady_state"`
	Cooldown         time.Duration     `json:"cooldown"`
	Phases           []ArrivalPhase    `json:"phases"`
	Tenants          []OpenLoopTenant  `json:"tenants"`
	ServiceTimes     []ServiceTimeMix  `json:"service_times"`
	PayloadSizes     []PayloadSizeMix  `json:"payload_sizes,omitempty"`
	DelayedFraction  float64           `json:"delayed_fraction"`
	Delay            time.Duration     `json:"delay"`
	SLO              time.Duration     `json:"slo"`
	MaxAttempts      int               `json:"max_attempts"`
	RetryBackoff     time.Duration     `json:"retry_backoff"`
	Downstream       DownstreamProfile `json:"downstream"`
	Faults           []FaultEvent      `json:"faults,omitempty"`
	MinimumTailCount int               `json:"minimum_tail_count"`
}

type ArrivalPhase struct {
	Name          string        `json:"name"`
	Duration      time.Duration `json:"duration"`
	RatePerSecond float64       `json:"rate_per_second"`
}

type OpenLoopTenant struct {
	Name              string  `json:"name"`
	OfferedWeight     float64 `json:"offered_weight"`
	EntitlementWeight float64 `json:"entitlement_weight"`
}

type ServiceTimeMix struct {
	Duration time.Duration `json:"duration"`
	Weight   float64       `json:"weight"`
}

// PayloadSizeMix controls application payload bytes independently of trace
// metadata. Profiles that omit it retain the original metadata-only payload.
type PayloadSizeMix struct {
	Bytes  int     `json:"bytes"`
	Weight float64 `json:"weight"`
}

// DownstreamProfile is a capacity model, not merely handler sleep. Once
// overlap exceeds Capacity, latency and failure probability increase until
// CollapseAt is reached.
type DownstreamProfile struct {
	Name                string        `json:"name"`
	Capacity            int           `json:"capacity"`
	BaseLatency         time.Duration `json:"base_latency"`
	MaxLatency          time.Duration `json:"max_latency"`
	BaseFailureRate     float64       `json:"base_failure_rate"`
	LatencySlope        float64       `json:"latency_slope"`
	FailureSlope        float64       `json:"failure_slope"`
	CollapseAt          float64       `json:"collapse_at"`
	CollapseFailureRate float64       `json:"collapse_failure_rate"`
}

type FaultKind string

const (
	FaultWorkerCrash   FaultKind = "worker_crash"
	FaultWorkerRecover FaultKind = "worker_recover"
)

type FaultEvent struct {
	At     time.Duration `json:"at"`
	Kind   FaultKind     `json:"kind"`
	Target string        `json:"target"`
}

// OpenLoopTrace is immutable input. StartAt is a deterministic synthetic UTC
// anchor; replay preserves every offset while assigning a separate run epoch.
type OpenLoopTrace struct {
	Schema   string          `json:"schema"`
	ID       string          `json:"id"`
	Digest   string          `json:"sha256"`
	Seed     int64           `json:"seed"`
	StartAt  time.Time       `json:"start_at"`
	Profile  OpenLoopProfile `json:"profile"`
	Arrivals []TraceArrival  `json:"arrivals"`
	Faults   []TraceFault    `json:"faults,omitempty"`
}

type TraceArrival struct {
	ID                  string        `json:"id"`
	At                  time.Time     `json:"at"`
	Tenant              string        `json:"tenant"`
	ServiceTime         time.Duration `json:"service_time"`
	PayloadBytes        int           `json:"payload_bytes,omitempty"`
	NotBefore           time.Time     `json:"not_before"`
	AttemptFailureDraws []float64     `json:"attempt_failure_draws"`
}

type TraceFault struct {
	At     time.Time `json:"at"`
	Kind   FaultKind `json:"kind"`
	Target string    `json:"target"`
}

func (p OpenLoopProfile) Validate() error {
	if p.Name == "" || p.Warmup <= 0 || p.SteadyState <= 0 || p.Cooldown < 0 || p.SLO <= 0 || p.MaxAttempts < 1 || len(p.Phases) == 0 || len(p.Tenants) == 0 || len(p.ServiceTimes) == 0 {
		return fmt.Errorf("invalid open-loop profile %q", p.Name)
	}
	var phaseDuration time.Duration
	for _, phase := range p.Phases {
		if phase.Name == "" || phase.Duration <= 0 || phase.RatePerSecond <= 0 || time.Duration(float64(time.Second)/phase.RatePerSecond) <= 0 {
			return fmt.Errorf("invalid arrival phase in %q", p.Name)
		}
		phaseDuration += phase.Duration
	}
	if phaseDuration != p.Warmup+p.SteadyState+p.Cooldown {
		return fmt.Errorf("arrival phases cover %s, windows cover %s", phaseDuration, p.Warmup+p.SteadyState+p.Cooldown)
	}
	var offered, entitlement float64
	seen := map[string]bool{}
	for _, tenant := range p.Tenants {
		if tenant.Name == "" || seen[tenant.Name] || tenant.OfferedWeight <= 0 || tenant.EntitlementWeight <= 0 {
			return fmt.Errorf("invalid tenant in %q", p.Name)
		}
		seen[tenant.Name] = true
		offered += tenant.OfferedWeight
		entitlement += tenant.EntitlementWeight
	}
	var serviceWeight float64
	for _, service := range p.ServiceTimes {
		if service.Duration < time.Millisecond || service.Duration > time.Second || service.Weight <= 0 {
			return fmt.Errorf("service-time mix must stay within 1ms..1s in %q", p.Name)
		}
		serviceWeight += service.Weight
	}
	var payloadWeight float64
	for _, payload := range p.PayloadSizes {
		if payload.Bytes < 0 || payload.Bytes > 4<<20 || payload.Weight <= 0 {
			return fmt.Errorf("payload sizes must stay within 0..4MiB in %q", p.Name)
		}
		payloadWeight += payload.Weight
	}
	if len(p.PayloadSizes) > 0 && payloadWeight <= 0 {
		return fmt.Errorf("invalid payload mix in %q", p.Name)
	}
	if offered <= 0 || entitlement <= 0 || serviceWeight <= 0 || p.DelayedFraction < 0 || p.DelayedFraction > 1 || (p.DelayedFraction > 0 && p.Delay <= 0) || p.RetryBackoff < 0 {
		return fmt.Errorf("invalid load mix in %q", p.Name)
	}
	if err := p.Downstream.Validate(); err != nil {
		return err
	}
	total := p.Warmup + p.SteadyState + p.Cooldown
	for _, fault := range p.Faults {
		if fault.At < 0 || fault.At >= total || fault.Target == "" || (fault.Kind != FaultWorkerCrash && fault.Kind != FaultWorkerRecover) {
			return fmt.Errorf("invalid fault in %q", p.Name)
		}
	}
	return nil
}

func (p DownstreamProfile) Validate() error {
	if p.Name == "" || p.Capacity < 1 || p.BaseLatency < 0 || p.MaxLatency < p.BaseLatency || p.BaseFailureRate < 0 || p.BaseFailureRate > 1 || p.LatencySlope < 0 || p.FailureSlope < 0 || p.CollapseAt <= 1 || p.CollapseFailureRate < p.BaseFailureRate || p.CollapseFailureRate > 1 {
		return fmt.Errorf("invalid downstream profile %q", p.Name)
	}
	return nil
}

// GenerateOpenLoopTrace is deterministic and performs no SUT operation. The
// caller should persist the returned bytes before starting any adapter.
func GenerateOpenLoopTrace(profile OpenLoopProfile, seed int64) (OpenLoopTrace, error) {
	if err := profile.Validate(); err != nil {
		return OpenLoopTrace{}, err
	}
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0x9e3779b97f4a7c15))
	anchor := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(seed%86400) * time.Second)
	trace := OpenLoopTrace{Schema: OpenLoopTraceSchema, ID: fmt.Sprintf("%s-%d", profile.Name, seed), Seed: seed, StartAt: anchor, Profile: profile}
	offset := time.Duration(0)
	index := 0
	for _, phase := range profile.Phases {
		interval := time.Duration(float64(time.Second) / phase.RatePerSecond)
		phaseEnd := offset + phase.Duration
		for at := offset; at < phaseEnd; at += interval {
			tenant := weightedTenant(rng.Float64(), profile.Tenants)
			service := weightedService(rng.Float64(), profile.ServiceTimes)
			payloadBytes := weightedPayload(rng.Float64(), profile.PayloadSizes)
			notBefore := anchor.Add(at)
			if rng.Float64() < profile.DelayedFraction {
				notBefore = notBefore.Add(profile.Delay)
			}
			draws := make([]float64, profile.MaxAttempts)
			for i := range draws {
				draws[i] = rng.Float64()
			}
			trace.Arrivals = append(trace.Arrivals, TraceArrival{ID: fmt.Sprintf("%s-%09d", trace.ID, index), At: anchor.Add(at), Tenant: tenant, ServiceTime: service, PayloadBytes: payloadBytes, NotBefore: notBefore, AttemptFailureDraws: draws})
			index++
		}
		offset = phaseEnd
	}
	for _, fault := range profile.Faults {
		trace.Faults = append(trace.Faults, TraceFault{At: anchor.Add(fault.At), Kind: fault.Kind, Target: fault.Target})
	}
	slices.SortFunc(trace.Faults, func(a, b TraceFault) int { return a.At.Compare(b.At) })
	trace.Digest = traceDigest(trace)
	if err := trace.Validate(); err != nil {
		return OpenLoopTrace{}, err
	}
	return trace, nil
}

func weightedTenant(draw float64, tenants []OpenLoopTenant) string {
	var total float64
	for _, tenant := range tenants {
		total += tenant.OfferedWeight
	}
	pick := draw * total
	for _, tenant := range tenants {
		pick -= tenant.OfferedWeight
		if pick <= 0 {
			return tenant.Name
		}
	}
	return tenants[len(tenants)-1].Name
}

func weightedService(draw float64, services []ServiceTimeMix) time.Duration {
	var total float64
	for _, service := range services {
		total += service.Weight
	}
	pick := draw * total
	for _, service := range services {
		pick -= service.Weight
		if pick <= 0 {
			return service.Duration
		}
	}
	return services[len(services)-1].Duration
}

func weightedPayload(draw float64, payloads []PayloadSizeMix) int {
	if len(payloads) == 0 {
		return 0
	}
	var total float64
	for _, payload := range payloads {
		total += payload.Weight
	}
	pick := draw * total
	for _, payload := range payloads {
		pick -= payload.Weight
		if pick <= 0 {
			return payload.Bytes
		}
	}
	return payloads[len(payloads)-1].Bytes
}

func (t OpenLoopTrace) Validate() error {
	if t.Schema != OpenLoopTraceSchema || t.ID == "" || t.StartAt.IsZero() || len(t.Arrivals) == 0 {
		return errors.New("invalid open-loop trace identity")
	}
	if err := t.Profile.Validate(); err != nil {
		return err
	}
	if t.Digest == "" || traceDigest(t) != t.Digest {
		return errors.New("open-loop trace digest mismatch")
	}
	seen := map[string]bool{}
	last := t.StartAt
	steadyCount := 0
	steadyStart := t.StartAt.Add(t.Profile.Warmup)
	steadyEnd := steadyStart.Add(t.Profile.SteadyState)
	for _, arrival := range t.Arrivals {
		if arrival.ID == "" || seen[arrival.ID] || arrival.At.Before(last) || arrival.At.Before(t.StartAt) || arrival.NotBefore.Before(arrival.At) || arrival.Tenant == "" || arrival.ServiceTime < time.Millisecond || arrival.ServiceTime > time.Second || arrival.PayloadBytes < 0 || arrival.PayloadBytes > 4<<20 || len(arrival.AttemptFailureDraws) != t.Profile.MaxAttempts {
			return fmt.Errorf("invalid arrival %q", arrival.ID)
		}
		seen[arrival.ID] = true
		last = arrival.At
		if !arrival.At.Before(steadyStart) && arrival.At.Before(steadyEnd) {
			steadyCount++
		}
	}
	if steadyCount < t.Profile.MinimumTailCount {
		return fmt.Errorf("steady-state has %d arrivals, want at least %d for tail estimates", steadyCount, t.Profile.MinimumTailCount)
	}
	for _, fault := range t.Faults {
		if fault.At.Before(t.StartAt) || !fault.At.Before(t.StartAt.Add(t.Profile.Warmup+t.Profile.SteadyState+t.Profile.Cooldown)) || fault.Target == "" || (fault.Kind != FaultWorkerCrash && fault.Kind != FaultWorkerRecover) {
			return errors.New("invalid trace fault")
		}
	}
	faultState := map[string]bool{}
	var lastFault time.Time
	for _, fault := range t.Faults {
		if !lastFault.IsZero() && fault.At.Before(lastFault) {
			return errors.New("trace faults are not timestamp ordered")
		}
		switch fault.Kind {
		case FaultWorkerCrash:
			if faultState[fault.Target] {
				return fmt.Errorf("duplicate crash for %q", fault.Target)
			}
			faultState[fault.Target] = true
		case FaultWorkerRecover:
			if !faultState[fault.Target] {
				return fmt.Errorf("recovery without crash for %q", fault.Target)
			}
			faultState[fault.Target] = false
		}
		lastFault = fault.At
	}
	for target, crashed := range faultState {
		if crashed {
			return fmt.Errorf("crash for %q has no recovery", target)
		}
	}
	return nil
}

type arrivalPayload struct {
	Arrival TraceArrival `json:"arrival"`
	Body    string       `json:"body,omitempty"`
}

// MarshalArrivalPayload gives every adapter the same application payload.
func MarshalArrivalPayload(arrival TraceArrival) ([]byte, error) {
	return json.Marshal(arrivalPayload{Arrival: arrival, Body: strings.Repeat("p", arrival.PayloadBytes)})
}

func UnmarshalArrivalPayload(data []byte) (TraceArrival, error) {
	var payload arrivalPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return TraceArrival{}, err
	}
	if len(payload.Body) != payload.Arrival.PayloadBytes {
		return TraceArrival{}, fmt.Errorf("payload body has %d bytes, want %d", len(payload.Body), payload.Arrival.PayloadBytes)
	}
	return payload.Arrival, nil
}

func traceDigest(trace OpenLoopTrace) string {
	trace.Digest = ""
	data, _ := json.Marshal(trace)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func LoadOpenLoopTrace(path string) (OpenLoopTrace, error) {
	file, err := os.Open(path)
	if err != nil {
		return OpenLoopTrace{}, fmt.Errorf("open trace: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var trace OpenLoopTrace
	if err := decoder.Decode(&trace); err != nil {
		return OpenLoopTrace{}, fmt.Errorf("decode trace: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return OpenLoopTrace{}, errors.New("trace contains trailing JSON")
	}
	if err := trace.Validate(); err != nil {
		return OpenLoopTrace{}, err
	}
	return trace, nil
}

// WriteOpenLoopTrace uses O_EXCL so a timestamp/fault input cannot be
// accidentally regenerated in place after any system has consumed it.
func WriteOpenLoopTrace(path string, trace OpenLoopTrace) error {
	if err := trace.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(trace, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0444)
	if err != nil {
		return fmt.Errorf("create immutable trace: %w", err)
	}
	_, writeErr := file.Write(append(data, '\n'))
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

func WriteOpenLoopResult(path string, result OpenLoopResult) error {
	if result.Schema != OpenLoopResultSchema || result.System == "" || result.TraceSHA256 == "" {
		return errors.New("invalid open-loop result")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("create result: %w", err)
	}
	_, writeErr := file.Write(append(data, '\n'))
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

type EnqueueDisposition string

const (
	EnqueueAccepted EnqueueDisposition = "accepted"
	EnqueueDeferred EnqueueDisposition = "deferred"
	EnqueueRejected EnqueueDisposition = "rejected"
)

type EnqueueResult struct {
	Disposition EnqueueDisposition
	Reason      string
}

type AdapterCapabilities struct {
	CrashRecovery       bool              `json:"crash_recovery"`
	DeliveryEquivalent  bool              `json:"delivery_equivalent"`
	BacklogKinds        []string          `json:"backlog_kinds"`
	ControllerTelemetry bool              `json:"controller_telemetry"`
	RedisTelemetry      bool              `json:"redis_telemetry"`
	Tuning              map[string]string `json:"tuning"`
	SemanticLimitations []string          `json:"semantic_limitations,omitempty"`
}

// OpenLoopAdapter is the complete isolation boundary for a system under test.
// It receives a pre-existing trace and a fresh downstream model.
type OpenLoopAdapter interface {
	Name() string
	Capabilities() AdapterCapabilities
	Start(context.Context, AdapterRuntime) error
	Enqueue(context.Context, TraceArrival) (EnqueueResult, error)
	ApplyFault(context.Context, TraceFault) error
	Snapshot(context.Context, time.Duration) (TelemetryPoint, error)
	Stop(context.Context) error
}

type AdapterRuntime struct {
	Trace      OpenLoopTrace
	RunEpoch   time.Time
	Recorder   *OpenLoopRecorder
	Downstream *Downstream
}

type BacklogPoint struct {
	At       time.Duration `json:"at"`
	Ready    int64         `json:"ready"`
	Deferred int64         `json:"deferred"`
	Retry    int64         `json:"retry"`
	DLQ      int64         `json:"dlq"`
}

type ControllerPoint struct {
	At                   time.Duration `json:"at"`
	EffectiveConcurrency float64       `json:"effective_concurrency"`
	Decision             string        `json:"decision"`
	Reason               string        `json:"reason"`
}

type RedisPoint struct {
	At              time.Duration `json:"at"`
	CPUSeconds      float64       `json:"cpu_seconds"`
	UsedMemoryBytes int64         `json:"used_memory_bytes"`
	Commands        int64         `json:"commands"`
	NetInputBytes   int64         `json:"net_input_bytes"`
	NetOutputBytes  int64         `json:"net_output_bytes"`
}

type TelemetryPoint struct {
	At           time.Duration   `json:"at"`
	Backlog      BacklogPoint    `json:"backlog"`
	Controller   ControllerPoint `json:"controller"`
	Redis        RedisPoint      `json:"redis"`
	SchedulerLag time.Duration   `json:"scheduler_lag"`
}

type EnqueueObservation struct {
	TaskID       string             `json:"task_id"`
	TraceAt      time.Time          `json:"trace_at"`
	ScheduledAt  time.Time          `json:"scheduled_at"`
	DispatchedAt time.Time          `json:"dispatched_at"`
	ReturnedAt   time.Time          `json:"returned_at"`
	Disposition  EnqueueDisposition `json:"disposition"`
	Reason       string             `json:"reason,omitempty"`
	Error        string             `json:"error,omitempty"`
}

type TaskObservation struct {
	TaskID                string        `json:"task_id"`
	Tenant                string        `json:"tenant"`
	Attempt               int           `json:"attempt"`
	StartedAt             time.Time     `json:"started_at"`
	CompletedAt           time.Time     `json:"completed_at"`
	Outcome               string        `json:"outcome"`
	SLO                   time.Duration `json:"slo"`
	TraceAt               time.Time     `json:"trace_at"`
	EligibleAt            time.Time     `json:"eligible_at"`
	EligibilityToStartLag time.Duration `json:"eligibility_to_start_lag"`
}

type DownstreamObservation struct {
	TaskID   string        `json:"task_id"`
	Attempt  int           `json:"attempt"`
	At       time.Time     `json:"at"`
	Overlap  int           `json:"overlap"`
	Capacity int           `json:"capacity"`
	Latency  time.Duration `json:"latency"`
	Failed   bool          `json:"failed"`
}

type TenantOutcome struct {
	Tenant            string  `json:"tenant"`
	Offered           int     `json:"offered"`
	Accepted          int     `json:"accepted"`
	Completed         int     `json:"completed"`
	SLOCompliant      int     `json:"slo_compliant"`
	EntitlementShare  float64 `json:"entitlement_share"`
	ServiceShare      float64 `json:"service_share"`
	ServiceDeficit    float64 `json:"service_deficit"`
	NormalizedDeficit float64 `json:"entitlement_normalized_service_deficit"`
	SLOAttainment     float64 `json:"slo_attainment"`
}

type HarnessTiming struct {
	DispatchLag Percentiles `json:"dispatch_lag"`
	EnqueueTime Percentiles `json:"enqueue_time"`
}

type CostModel struct {
	CPUSecond      float64 `json:"cpu_second"`
	NetworkGB      float64 `json:"network_gb"`
	MemoryGBSecond float64 `json:"memory_gb_second"`
}

type CostOutcome struct {
	Rates                   CostModel `json:"rates"`
	Total                   float64   `json:"total"`
	SLOCompliantCompletions int       `json:"slo_compliant_completions"`
	PerSLOCompletion        float64   `json:"per_slo_completion"`
}

type MeasurementWindows struct {
	Warmup      time.Duration `json:"warmup"`
	SteadyState time.Duration `json:"steady_state"`
	Cooldown    time.Duration `json:"cooldown"`
}

type OpenLoopResult struct {
	Schema        string                  `json:"schema"`
	System        string                  `json:"system"`
	SystemOrder   int                     `json:"system_order"`
	TraceID       string                  `json:"trace_id"`
	TraceSHA256   string                  `json:"trace_sha256"`
	RunEpoch      time.Time               `json:"run_epoch"`
	Windows       MeasurementWindows      `json:"windows"`
	Capabilities  AdapterCapabilities     `json:"capabilities"`
	Excluded      bool                    `json:"excluded"`
	ExcludeReason string                  `json:"exclude_reason,omitempty"`
	Enqueues      []EnqueueObservation    `json:"enqueues,omitempty"`
	Tasks         []TaskObservation       `json:"tasks,omitempty"`
	Downstream    []DownstreamObservation `json:"downstream,omitempty"`
	Telemetry     []TelemetryPoint        `json:"telemetry,omitempty"`
	Tenants       []TenantOutcome         `json:"tenants,omitempty"`
	Harness       HarnessTiming           `json:"harness"`
	Cost          CostOutcome             `json:"cost"`
}

// OpenLoopRecorder is shared only between one adapter and its dependency.
type OpenLoopRecorder struct {
	mu         sync.Mutex
	trace      OpenLoopTrace
	runEpoch   time.Time
	enqueues   []EnqueueObservation
	tasks      []TaskObservation
	downstream []DownstreamObservation
	telemetry  []TelemetryPoint
}

func NewOpenLoopRecorder(trace OpenLoopTrace) *OpenLoopRecorder {
	return &OpenLoopRecorder{trace: trace}
}

func (r *OpenLoopRecorder) RecordEnqueue(value EnqueueObservation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enqueues = append(r.enqueues, value)
}

func (r *OpenLoopRecorder) TaskStarted(task TraceArrival, attempt int, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	eligible := r.runEpoch.Add(task.NotBefore.Sub(r.trace.StartAt))
	lag := max(at.Sub(eligible), 0)
	r.tasks = append(r.tasks, TaskObservation{TaskID: task.ID, Tenant: task.Tenant, Attempt: attempt, StartedAt: at, SLO: r.trace.Profile.SLO, TraceAt: task.At, EligibleAt: eligible, EligibilityToStartLag: lag})
}

func (r *OpenLoopRecorder) TaskFinished(task TraceArrival, attempt int, at time.Time, outcome string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.tasks) - 1; i >= 0; i-- {
		if r.tasks[i].TaskID == task.ID && r.tasks[i].Attempt == attempt && r.tasks[i].CompletedAt.IsZero() {
			r.tasks[i].CompletedAt = at
			r.tasks[i].Outcome = outcome
			return
		}
	}
	r.tasks = append(r.tasks, TaskObservation{TaskID: task.ID, Tenant: task.Tenant, Attempt: attempt, CompletedAt: at, Outcome: outcome, SLO: r.trace.Profile.SLO, TraceAt: task.At})
}

func (r *OpenLoopRecorder) RecordDownstream(value DownstreamObservation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.downstream = append(r.downstream, value)
}

func (r *OpenLoopRecorder) RecordTelemetry(value TelemetryPoint) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.telemetry = append(r.telemetry, value)
}

// Downstream is deliberately shared by all workers of one cell so concurrent
// overlap creates degradation. Separate cells receive fresh instances.
type Downstream struct {
	profile  DownstreamProfile
	recorder *OpenLoopRecorder
	inFlight atomic.Int64
}

func NewDownstream(profile DownstreamProfile, recorder *OpenLoopRecorder) *Downstream {
	return &Downstream{profile: profile, recorder: recorder}
}

func (d *Downstream) Parameters(overlap int) (time.Duration, float64) {
	ratio := float64(overlap) / float64(d.profile.Capacity)
	latency := d.profile.BaseLatency
	failure := d.profile.BaseFailureRate
	if ratio > 1 {
		latency = time.Duration(float64(d.profile.BaseLatency) * (1 + d.profile.LatencySlope*math.Pow(ratio-1, 2)))
		failure += d.profile.FailureSlope * (ratio - 1)
	}
	if ratio >= d.profile.CollapseAt {
		failure = max(failure, d.profile.CollapseFailureRate)
	}
	return min(latency, d.profile.MaxLatency), min(failure, 1)
}

func (d *Downstream) Call(ctx context.Context, task TraceArrival, attempt int) error {
	overlap := int(d.inFlight.Add(1))
	defer d.inFlight.Add(-1)
	latency, failureRate := d.Parameters(overlap)
	latency += task.ServiceTime
	failed := attempt < 1 || attempt > len(task.AttemptFailureDraws) || task.AttemptFailureDraws[attempt-1] < failureRate
	observation := DownstreamObservation{TaskID: task.ID, Attempt: attempt, At: time.Now().UTC(), Overlap: overlap, Capacity: d.profile.Capacity, Latency: latency, Failed: failed}
	timer := time.NewTimer(latency)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		observation.Failed = true
		d.recorder.RecordDownstream(observation)
		return ctx.Err()
	case <-timer.C:
		d.recorder.RecordDownstream(observation)
		if failed {
			return errors.New("modeled downstream failure")
		}
		return nil
	}
}

type ReplayOptions struct {
	StartLead      time.Duration
	EnqueueTimeout time.Duration
	SnapshotPeriod time.Duration
	DrainTimeout   time.Duration
	Cost           CostModel
}

func (o ReplayOptions) normalized() ReplayOptions {
	if o.StartLead <= 0 {
		o.StartLead = 100 * time.Millisecond
	}
	if o.EnqueueTimeout <= 0 {
		o.EnqueueTimeout = time.Second
	}
	if o.SnapshotPeriod <= 0 {
		o.SnapshotPeriod = 100 * time.Millisecond
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = 30 * time.Second
	}
	return o
}

// ReplayOpenLoop never derives pacing from enqueue completion. Each due
// arrival is dispatched independently; late dispatch and blocking enqueue are
// retained as harness observations.
func ReplayOpenLoop(ctx context.Context, trace OpenLoopTrace, adapter OpenLoopAdapter, order int, opts ReplayOptions) (OpenLoopResult, error) {
	if err := trace.Validate(); err != nil {
		return OpenLoopResult{}, err
	}
	opts = opts.normalized()
	result := OpenLoopResult{
		Schema: OpenLoopResultSchema, System: adapter.Name(), SystemOrder: order, TraceID: trace.ID, TraceSHA256: trace.Digest,
		Windows:      MeasurementWindows{Warmup: trace.Profile.Warmup, SteadyState: trace.Profile.SteadyState, Cooldown: trace.Profile.Cooldown},
		Capabilities: adapter.Capabilities(),
	}
	if len(trace.Faults) > 0 && (!result.Capabilities.CrashRecovery || !result.Capabilities.DeliveryEquivalent) {
		result.Excluded = true
		result.ExcludeReason = "trace contains worker faults without equivalent crash/recovery and delivery semantics"
		return result, nil
	}
	runEpoch := time.Now().UTC().Add(opts.StartLead)
	result.RunEpoch = runEpoch
	recorder := NewOpenLoopRecorder(trace)
	recorder.runEpoch = runEpoch
	runtime := AdapterRuntime{Trace: trace, RunEpoch: runEpoch, Recorder: recorder, Downstream: NewDownstream(trace.Profile.Downstream, recorder)}
	if err := adapter.Start(ctx, runtime); err != nil {
		return result, fmt.Errorf("start %s: %w", adapter.Name(), err)
	}
	stopped := false
	defer func() {
		if !stopped {
			_ = adapter.Stop(context.Background())
		}
	}()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	asyncErr := make(chan error, 2)
	var background sync.WaitGroup
	background.Add(1)
	go func() {
		defer background.Done()
		for _, fault := range trace.Faults {
			due := runEpoch.Add(fault.At.Sub(trace.StartAt))
			if err := waitUntil(runCtx, due); err != nil {
				return
			}
			if err := adapter.ApplyFault(runCtx, fault); err != nil {
				select {
				case asyncErr <- fmt.Errorf("apply %s: %w", fault.Kind, err):
				default:
				}
				return
			}
		}
	}()
	end := runEpoch.Add(trace.Profile.Warmup + trace.Profile.SteadyState + trace.Profile.Cooldown)
	background.Add(1)
	go func() {
		defer background.Done()
		first := runEpoch.Add(opts.SnapshotPeriod)
		for due := first; !due.After(end); due = due.Add(opts.SnapshotPeriod) {
			if err := waitUntil(runCtx, due); err != nil {
				return
			}
			point, err := adapter.Snapshot(runCtx, due.Sub(runEpoch))
			if err != nil {
				select {
				case asyncErr <- fmt.Errorf("snapshot %s: %w", adapter.Name(), err):
				default:
				}
				return
			}
			recorder.RecordTelemetry(point)
		}
	}()
	var enqueueWG sync.WaitGroup
	for _, arrival := range trace.Arrivals {
		arrival := arrival
		due := runEpoch.Add(arrival.At.Sub(trace.StartAt))
		if err := waitUntil(runCtx, due); err != nil {
			return result, err
		}
		dispatched := time.Now().UTC()
		enqueueWG.Add(1)
		go func() {
			defer enqueueWG.Done()
			enqueueCtx, cancelEnqueue := context.WithTimeout(runCtx, opts.EnqueueTimeout)
			defer cancelEnqueue()
			value, err := adapter.Enqueue(enqueueCtx, arrival)
			observation := EnqueueObservation{TaskID: arrival.ID, TraceAt: arrival.At, ScheduledAt: due, DispatchedAt: dispatched, ReturnedAt: time.Now().UTC(), Disposition: value.Disposition, Reason: value.Reason}
			if err != nil {
				observation.Error = err.Error()
				if observation.Disposition == "" {
					observation.Disposition = EnqueueRejected
				}
			}
			recorder.RecordEnqueue(observation)
		}()
		select {
		case err := <-asyncErr:
			return result, err
		default:
		}
	}
	enqueueWG.Wait()
	if err := waitUntil(runCtx, end); err != nil {
		return result, err
	}
	background.Wait()
	select {
	case err := <-asyncErr:
		return result, err
	default:
	}
	deadline := time.Now().Add(opts.DrainTimeout)
	for time.Now().Before(deadline) && !recorder.allAcceptedTerminal() {
		point, err := adapter.Snapshot(runCtx, time.Since(runEpoch))
		if err == nil {
			recorder.RecordTelemetry(point)
		}
		timer := time.NewTimer(min(opts.SnapshotPeriod, time.Until(deadline)))
		select {
		case <-runCtx.Done():
			timer.Stop()
			return result, runCtx.Err()
		case <-timer.C:
		}
	}
	if err := adapter.Stop(ctx); err != nil {
		return result, fmt.Errorf("stop %s: %w", adapter.Name(), err)
	}
	stopped = true
	return recorder.result(result, opts.Cost), nil
}

func waitUntil(ctx context.Context, at time.Time) error {
	delay := time.Until(at)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *OpenLoopRecorder) allAcceptedTerminal() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	accepted := map[string]bool{}
	terminal := map[string]bool{}
	for _, enqueue := range r.enqueues {
		if enqueue.Disposition != EnqueueRejected {
			accepted[enqueue.TaskID] = true
		}
	}
	for _, task := range r.tasks {
		if task.Outcome == "completed" || task.Outcome == "dlq" {
			terminal[task.TaskID] = true
		}
	}
	return len(accepted) > 0 && len(terminal) >= len(accepted)
}

func (r *OpenLoopRecorder) result(base OpenLoopResult, cost CostModel) OpenLoopResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	base.Enqueues = slices.Clone(r.enqueues)
	base.Tasks = slices.Clone(r.tasks)
	base.Downstream = slices.Clone(r.downstream)
	base.Telemetry = slices.Clone(r.telemetry)
	sort.Slice(base.Enqueues, func(i, j int) bool { return base.Enqueues[i].TaskID < base.Enqueues[j].TaskID })
	dispatch := make([]time.Duration, 0, len(base.Enqueues))
	enqueueTime := make([]time.Duration, 0, len(base.Enqueues))
	accepted := map[string]bool{}
	for _, value := range base.Enqueues {
		dispatch = append(dispatch, max(value.DispatchedAt.Sub(value.ScheduledAt), 0))
		enqueueTime = append(enqueueTime, value.ReturnedAt.Sub(value.DispatchedAt))
		if value.Disposition != EnqueueRejected {
			accepted[value.TaskID] = true
		}
	}
	base.Harness = HarnessTiming{DispatchLag: percentiles(dispatch), EnqueueTime: percentiles(enqueueTime)}
	base.Tenants = summarizeTenantOutcomes(r.trace, accepted, base.Tasks, base.RunEpoch)
	base.Cost = summarizeCost(base.Telemetry, base.Tenants, cost)
	return base
}

func summarizeTenantOutcomes(trace OpenLoopTrace, accepted map[string]bool, tasks []TaskObservation, epoch time.Time) []TenantOutcome {
	values := make(map[string]*TenantOutcome, len(trace.Profile.Tenants))
	var entitlementTotal float64
	for _, tenant := range trace.Profile.Tenants {
		entitlementTotal += tenant.EntitlementWeight
	}
	steadyStart := trace.StartAt.Add(trace.Profile.Warmup)
	steadyEnd := steadyStart.Add(trace.Profile.SteadyState)
	for _, tenant := range trace.Profile.Tenants {
		values[tenant.Name] = &TenantOutcome{Tenant: tenant.Name, EntitlementShare: tenant.EntitlementWeight / entitlementTotal}
	}
	byID := map[string]TraceArrival{}
	for _, arrival := range trace.Arrivals {
		byID[arrival.ID] = arrival
		if !arrival.At.Before(steadyStart) && arrival.At.Before(steadyEnd) {
			values[arrival.Tenant].Offered++
			if accepted[arrival.ID] {
				values[arrival.Tenant].Accepted++
			}
		}
	}
	completedIDs := map[string]bool{}
	for _, task := range tasks {
		arrival := byID[task.TaskID]
		if task.Outcome != "completed" || completedIDs[task.TaskID] || arrival.At.Before(steadyStart) || !arrival.At.Before(steadyEnd) {
			continue
		}
		completedIDs[task.TaskID] = true
		value := values[task.Tenant]
		value.Completed++
		scheduled := epoch.Add(arrival.At.Sub(trace.StartAt))
		if !task.CompletedAt.IsZero() && task.CompletedAt.Sub(scheduled) <= trace.Profile.SLO {
			value.SLOCompliant++
		}
	}
	var compliant float64
	for _, value := range values {
		compliant += float64(value.SLOCompliant)
	}
	result := make([]TenantOutcome, 0, len(values))
	for _, tenant := range trace.Profile.Tenants {
		value := values[tenant.Name]
		if compliant > 0 {
			value.ServiceShare = float64(value.SLOCompliant) / compliant
		}
		value.ServiceDeficit = max(value.EntitlementShare-value.ServiceShare, 0)
		if value.EntitlementShare > 0 {
			value.NormalizedDeficit = value.ServiceDeficit / value.EntitlementShare
		}
		if value.Accepted > 0 {
			value.SLOAttainment = float64(value.SLOCompliant) / float64(value.Accepted)
		}
		result = append(result, *value)
	}
	return result
}

func summarizeCost(points []TelemetryPoint, tenants []TenantOutcome, cost CostModel) CostOutcome {
	var compliant int
	for _, tenant := range tenants {
		compliant += tenant.SLOCompliant
	}
	result := CostOutcome{Rates: cost, SLOCompliantCompletions: compliant}
	if len(points) > 0 {
		first, last := points[0].Redis, points[len(points)-1].Redis
		cpu := max(last.CPUSeconds-first.CPUSeconds, 0)
		networkGB := float64(max((last.NetInputBytes+last.NetOutputBytes)-(first.NetInputBytes+first.NetOutputBytes), 0)) / 1e9
		var memoryGBSeconds float64
		for i := 1; i < len(points); i++ {
			seconds := max((points[i].At - points[i-1].At).Seconds(), 0)
			averageBytes := float64(points[i-1].Redis.UsedMemoryBytes+points[i].Redis.UsedMemoryBytes) / 2
			memoryGBSeconds += averageBytes / 1e9 * seconds
		}
		result.Total = cpu*cost.CPUSecond + networkGB*cost.NetworkGB + memoryGBSeconds*cost.MemoryGBSecond
	}
	if compliant > 0 {
		result.PerSLOCompletion = result.Total / float64(compliant)
	}
	return result
}

// CounterbalancedOrder rotates a deterministic shuffled order on each
// repetition. Across a complete block every system occupies every position.
func CounterbalancedOrder(systems []string, seed int64, repetition int) []string {
	base := slices.Clone(systems)
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0xd1b54a32d192ed03))
	rng.Shuffle(len(base), func(i, j int) { base[i], base[j] = base[j], base[i] })
	if len(base) == 0 {
		return base
	}
	rotation := repetition % len(base)
	if rotation < 0 {
		rotation += len(base)
	}
	return append(slices.Clone(base[rotation:]), base[:rotation]...)
}
