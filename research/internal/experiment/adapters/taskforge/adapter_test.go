package taskforge

import (
	"slices"
	"testing"
	"time"

	"github.com/aminkbi/taskforge/research/internal/experiment"
)

func TestFairnessWeightsUseShortReservationCycles(t *testing.T) {
	for _, tc := range []struct {
		entitlements []float64
		want         []int
	}{
		{[]float64{1, 1}, []int{1, 1}},
		{[]float64{1, 8, 1}, []int{1, 8, 1}},
		{[]float64{0.5, 1.5}, []int{1, 3}},
		{[]float64{2, 2}, []int{1, 1}},
	} {
		tenants := make([]experiment.OpenLoopTenant, len(tc.entitlements))
		for i, weight := range tc.entitlements {
			tenants[i].EntitlementWeight = weight
		}
		if got := fairnessWeights(tenants); !slices.Equal(got, tc.want) {
			t.Fatalf("weights %v produce %v, want %v", tc.entitlements, got, tc.want)
		}
	}
}

func TestNeedsSchedulerOnlyForDeferredWork(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	ready := experiment.TraceArrival{At: now, NotBefore: now}
	delayed := ready
	delayed.NotBefore = now.Add(time.Second)

	tests := []struct {
		name    string
		adapter *Adapter
		trace   experiment.OpenLoopTrace
		want    bool
	}{
		{name: "fifo static ready only", adapter: New(Config{DisableFairness: true, DisableAdaptive: true, DisableDependencyBudget: true}), trace: experiment.OpenLoopTrace{Profile: experiment.OpenLoopProfile{MaxAttempts: 1}, Arrivals: []experiment.TraceArrival{ready}}},
		{name: "delayed arrival", adapter: New(Config{}), trace: experiment.OpenLoopTrace{Profile: experiment.OpenLoopProfile{MaxAttempts: 1}, Arrivals: []experiment.TraceArrival{delayed}}, want: true},
		{name: "retry release", adapter: New(Config{}), trace: experiment.OpenLoopTrace{Profile: experiment.OpenLoopProfile{MaxAttempts: 2}, Arrivals: []experiment.TraceArrival{ready}}, want: true},
		{name: "admission deferral", adapter: New(Config{AdmissionMaxPending: 1}), trace: experiment.OpenLoopTrace{Profile: experiment.OpenLoopProfile{MaxAttempts: 1}, Arrivals: []experiment.TraceArrival{ready}}, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.adapter.needsScheduler(test.trace); got != test.want {
				t.Fatalf("needsScheduler() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestFIFOStaticCapabilitiesDeclareDisabledControls(t *testing.T) {
	t.Parallel()

	capabilities := New(Config{DisableFairness: true, DisableAdaptive: true, DisableDependencyBudget: true}).Capabilities()
	if capabilities.Tuning["fairness_enabled"] != "false" || capabilities.Tuning["adaptive_enabled"] != "false" || capabilities.Tuning["dependency_budget_enabled"] != "false" {
		t.Fatalf("FIFO/static tuning = %#v", capabilities.Tuning)
	}
}
