package experiment

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestRegisteredOpenLoopProfilesAreValid(t *testing.T) {
	paths, err := filepath.Glob("../../test/experiment/open-loop/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 6 {
		t.Fatalf("found %d open-loop profiles, want 6", len(paths))
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var profile OpenLoopProfile
		if err := json.Unmarshal(data, &profile); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if err := profile.Validate(); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
}

func testOpenLoopProfile() OpenLoopProfile {
	return OpenLoopProfile{
		Name:        "neutral-smoke",
		Warmup:      5 * time.Millisecond,
		SteadyState: 10 * time.Millisecond,
		Cooldown:    5 * time.Millisecond,
		Phases: []ArrivalPhase{
			{Name: "warmup", Duration: 5 * time.Millisecond, RatePerSecond: 1000},
			{Name: "steady", Duration: 10 * time.Millisecond, RatePerSecond: 1000},
			{Name: "cooldown", Duration: 5 * time.Millisecond, RatePerSecond: 1000},
		},
		Tenants:         []OpenLoopTenant{{Name: "hot", OfferedWeight: 9, EntitlementWeight: 1}, {Name: "protected", OfferedWeight: 1, EntitlementWeight: 1}},
		ServiceTimes:    []ServiceTimeMix{{Duration: time.Millisecond, Weight: 1}, {Duration: time.Second, Weight: 1}},
		DelayedFraction: .25,
		Delay:           2 * time.Millisecond,
		SLO:             time.Second,
		MaxAttempts:     3,
		RetryBackoff:    time.Millisecond,
		Downstream: DownstreamProfile{
			Name: "database", Capacity: 2, BaseLatency: time.Millisecond, MaxLatency: time.Second,
			BaseFailureRate: .01, LatencySlope: 2, FailureSlope: .25, CollapseAt: 2, CollapseFailureRate: .9,
		},
		MinimumTailCount: 10,
	}
}

func TestGenerateOpenLoopTraceIsImmutableAndDeterministic(t *testing.T) {
	first, err := GenerateOpenLoopTrace(testOpenLoopProfile(), 42)
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateOpenLoopTrace(testOpenLoopProfile(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest || !slices.EqualFunc(first.Arrivals, second.Arrivals, func(a, b TraceArrival) bool {
		return a.ID == b.ID && a.At.Equal(b.At) && a.NotBefore.Equal(b.NotBefore) && slices.Equal(a.AttemptFailureDraws, b.AttemptFailureDraws)
	}) {
		t.Fatal("same profile and seed did not produce the same external trace")
	}
	first.Arrivals[0].Tenant = "mutated"
	if err := first.Validate(); err == nil {
		t.Fatal("mutated trace passed its immutable digest check")
	}
}

func TestWriteOpenLoopTraceRefusesReplacement(t *testing.T) {
	trace, err := GenerateOpenLoopTrace(testOpenLoopProfile(), 43)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/trace.json"
	if err := WriteOpenLoopTrace(path, trace); err != nil {
		t.Fatal(err)
	}
	if err := WriteOpenLoopTrace(path, trace); err == nil {
		t.Fatal("immutable trace was replaced")
	}
	loaded, err := LoadOpenLoopTrace(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Digest != trace.Digest {
		t.Fatalf("loaded digest = %q, want %q", loaded.Digest, trace.Digest)
	}
}

func TestCounterbalancedOrderOccupiesEveryPosition(t *testing.T) {
	systems := []string{"taskforge", "asynq", "third"}
	positions := map[string]map[int]bool{}
	for repetition := range len(systems) {
		order := CounterbalancedOrder(systems, 99, repetition)
		for position, system := range order {
			if positions[system] == nil {
				positions[system] = map[int]bool{}
			}
			positions[system][position] = true
		}
	}
	for _, system := range systems {
		if len(positions[system]) != len(systems) {
			t.Fatalf("%s occupied %d positions, want %d", system, len(positions[system]), len(systems))
		}
	}
}

func TestDownstreamDegradesAndCollapsesAboveCapacity(t *testing.T) {
	profile := testOpenLoopProfile().Downstream
	downstream := NewDownstream(profile, NewOpenLoopRecorder(OpenLoopTrace{}))
	nominalLatency, nominalFailure := downstream.Parameters(profile.Capacity)
	overloadLatency, overloadFailure := downstream.Parameters(profile.Capacity + 1)
	_, collapseFailure := downstream.Parameters(int(float64(profile.Capacity) * profile.CollapseAt))
	if overloadLatency <= nominalLatency || overloadFailure <= nominalFailure {
		t.Fatalf("overload did not degrade dependency: nominal=(%s,%f), overload=(%s,%f)", nominalLatency, nominalFailure, overloadLatency, overloadFailure)
	}
	if collapseFailure < profile.CollapseFailureRate {
		t.Fatalf("collapse failure rate = %f, want >= %f", collapseFailure, profile.CollapseFailureRate)
	}
}

type recordingAdapter struct {
	mu      sync.Mutex
	runtime AdapterRuntime
	faults  []TraceFault
}

func (*recordingAdapter) Name() string { return "recording" }
func (*recordingAdapter) Capabilities() AdapterCapabilities {
	return AdapterCapabilities{CrashRecovery: true, DeliveryEquivalent: true}
}
func (a *recordingAdapter) Start(_ context.Context, runtime AdapterRuntime) error {
	a.runtime = runtime
	return nil
}
func (a *recordingAdapter) Enqueue(_ context.Context, arrival TraceArrival) (EnqueueResult, error) {
	now := time.Now().UTC()
	a.runtime.Recorder.TaskStarted(arrival, 1, now)
	a.runtime.Recorder.TaskFinished(arrival, 1, now, "completed")
	return EnqueueResult{Disposition: EnqueueAccepted}, nil
}
func (a *recordingAdapter) ApplyFault(_ context.Context, fault TraceFault) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.faults = append(a.faults, fault)
	return nil
}
func (*recordingAdapter) Snapshot(_ context.Context, at time.Duration) (TelemetryPoint, error) {
	return TelemetryPoint{At: at}, nil
}
func (*recordingAdapter) Stop(context.Context) error { return nil }

func TestReplayUsesTraceTimestampsAndEquivalentFaults(t *testing.T) {
	profile := testOpenLoopProfile()
	profile.Faults = []FaultEvent{{At: 3 * time.Millisecond, Kind: FaultWorkerCrash, Target: "workers"}, {At: 7 * time.Millisecond, Kind: FaultWorkerRecover, Target: "workers"}}
	trace, err := GenerateOpenLoopTrace(profile, 7)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &recordingAdapter{}
	result, err := ReplayOpenLoop(context.Background(), trace, adapter, 1, ReplayOptions{StartLead: time.Millisecond, SnapshotPeriod: 2 * time.Millisecond, DrainTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if result.TraceSHA256 != trace.Digest || len(result.Enqueues) != len(trace.Arrivals) || len(adapter.faults) != len(trace.Faults) {
		t.Fatalf("replay did not retain common trace/fault input: trace=%q enqueue=%d faults=%d", result.TraceSHA256, len(result.Enqueues), len(adapter.faults))
	}
	byID := map[string]TraceArrival{}
	for _, arrival := range trace.Arrivals {
		byID[arrival.ID] = arrival
	}
	for _, enqueue := range result.Enqueues {
		want := result.RunEpoch.Add(byID[enqueue.TaskID].At.Sub(trace.StartAt))
		if !enqueue.ScheduledAt.Equal(want) {
			t.Fatalf("%s scheduled at %s, want %s", enqueue.TaskID, enqueue.ScheduledAt, want)
		}
	}
}

func TestReplayExcludesUnsupportedFaultCell(t *testing.T) {
	profile := testOpenLoopProfile()
	profile.Faults = []FaultEvent{{At: time.Millisecond, Kind: FaultWorkerCrash, Target: "workers"}, {At: 2 * time.Millisecond, Kind: FaultWorkerRecover, Target: "workers"}}
	trace, err := GenerateOpenLoopTrace(profile, 8)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &recordingAdapter{}
	resultCapabilities := adapter.Capabilities()
	resultCapabilities.CrashRecovery = false
	unsupported := capabilityAdapter{recordingAdapter: adapter, capabilities: resultCapabilities}
	result, err := ReplayOpenLoop(context.Background(), trace, unsupported, 0, ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Excluded || result.ExcludeReason == "" {
		t.Fatalf("unsupported fault cell was not explicitly excluded: %+v", result)
	}
}

type capabilityAdapter struct {
	*recordingAdapter
	capabilities AdapterCapabilities
}

func (a capabilityAdapter) Capabilities() AdapterCapabilities { return a.capabilities }
