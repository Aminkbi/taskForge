package experiment

import (
	"testing"
	"time"
)

func TestManifestValidateAndSummary(t *testing.T) {
	m := Manifest{Name: "tenant-skew", Tasks: 2, Tenants: []Tenant{{Name: "a", Weight: 1}}, SLO: time.Second}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	summary := Summarize([]Sample{{Tenant: "a", EnqueuedAt: now, StartedAt: now.Add(time.Millisecond), CompletedAt: now.Add(2 * time.Millisecond)}, {Tenant: "a", EnqueuedAt: now, StartedAt: now.Add(2 * time.Millisecond), CompletedAt: now.Add(3 * time.Millisecond)}}, m.Tenants, RedisMetrics{})
	if summary.EnqueueToStart.P50 != time.Millisecond || summary.Throughput != 2/0.003 || summary.JainFairness != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
}

func TestVariantsIncludeSeparatedBaselineAndAblations(t *testing.T) {
	seen := map[string]bool{}
	for _, variant := range Variants() {
		seen[variant.Name] = true
	}
	for _, name := range []string{"taskforge-fifo-static", "taskforge-no-fairness", "taskforge-no-admission", "taskforge-no-adaptive", "taskforge-no-dependency-budget", "taskforge-full", "asynq"} {
		if !seen[name] {
			t.Errorf("missing %s", name)
		}
	}
}
