package taskforge

import (
	"testing"
	"time"

	"github.com/aminkbi/taskforge/internal/experiment"
)

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
