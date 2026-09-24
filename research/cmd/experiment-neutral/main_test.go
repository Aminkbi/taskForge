package main

import "testing"

func TestSingleControlArms(t *testing.T) {
	for _, control := range []string{"fairness", "admission", "budget", "adaptive"} {
		c := controlConfig("taskforge-"+control+"-only", 8, 32, 4)
		if c.Concurrency != 8 || c.MaxConcurrency != 16 || c.DisableFairness != (control != "fairness") || c.DisableAdaptive != (control != "adaptive") || c.DisableDependencyBudget != (control != "budget") || (c.AdmissionMaxPending > 0) != (control == "admission") {
			t.Fatalf("%s enables unexpected controls: %+v", control, c)
		}
	}
}

func TestAdaptiveAblationAndCapacityBaseline(t *testing.T) {
	c := controlConfig("taskforge-no-adaptive", 8, 32, 4)
	if !c.DisableAdaptive || c.DisableFairness || c.DisableDependencyBudget || c.AdmissionMaxPending != 32 {
		t.Fatalf("adaptive ablation changes other controls: %+v", c)
	}
	c = controlConfig("taskforge-static-capacity", 8, 32, 4)
	if c.Concurrency != 4 || c.MaxConcurrency != 16 || !c.DisableAdaptive || !c.DisableFairness || !c.DisableDependencyBudget || c.AdmissionMaxPending != 0 {
		t.Fatalf("capacity baseline: %+v", c)
	}
}
