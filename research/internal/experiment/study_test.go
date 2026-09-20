package experiment

import (
	"math"
	"testing"
)

func TestPairedStudyMetricUsesWithinBlockDifferences(t *testing.T) {
	left := []float64{101, 1001, 11, 201}
	right := []float64{100, 1000, 10, 200}
	metric := pairedStudyMetric("throughput_per_second", left, right, .95, 42, 2000)
	if metric.Estimate != 1 || metric.Lower != 1 || metric.Upper != 1 || metric.Pairs != 4 {
		t.Fatalf("paired metric = %+v, want constant within-block effect 1", metric)
	}
	if metric.RelativePercent == nil || math.Abs(*metric.RelativePercent-.75) > .001 {
		t.Fatalf("relative paired effect = %v", metric.RelativePercent)
	}
}

func TestFindEnvironmentReversalsReportsSignChange(t *testing.T) {
	results := []StudyContrastResult{
		{Name: "common", Environment: "native", Metrics: []StudyMetric{{Name: "throughput_per_second", Estimate: 10}}},
		{Name: "common", Environment: "constrained", Metrics: []StudyMetric{{Name: "throughput_per_second", Estimate: -3}}},
	}
	reversals := findEnvironmentReversals(results)
	if len(reversals) != 1 || reversals[0].Metric != "throughput_per_second" {
		t.Fatalf("reversals = %+v", reversals)
	}
}

func TestStudyPlanRejectsUnpairedContrast(t *testing.T) {
	plan := StudyPlan{
		Schema: StudyPlanSchema, FrozenAt: "2026-07-18T00:00:00Z", BootstrapSeed: 1, BootstrapIterations: 1000, Multiplicity: "fixed",
		Environments: []StudyEnvironment{{Name: "one", GOMAXPROCS: 1, RedisTopology: "local", RedisNetwork: "tcp", Description: "one"}, {Name: "two", GOMAXPROCS: 2, RedisTopology: "network", RedisNetwork: "tcp", Description: "two"}},
		Profiles:     []StudyProfile{{Name: "profile", File: "profile.json", Seeds: []int64{1, 2}, Repetitions: 1, Systems: []string{"left", "right"}, DurationClass: "standard"}},
		Contrasts:    []RegisteredContrast{{Name: "bad", Family: "primary", Profiles: []string{"profile"}, Left: "left", Right: "missing", Metrics: []string{"throughput_per_second"}, Primary: true, Confidence: .95}},
	}
	if err := plan.Validate(); err == nil {
		t.Fatal("unpaired contrast passed study-plan validation")
	}
}
