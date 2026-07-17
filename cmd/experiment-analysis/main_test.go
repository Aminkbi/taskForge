package main

import (
	"strings"
	"testing"

	"github.com/aminkbi/taskforge/internal/experiment"
)

func TestMarkdownHasCompleteTablesAndMarksUnsupportedCrashCell(t *testing.T) {
	metrics := map[string]experiment.MetricSummary{}
	for _, name := range []string{
		"completion_p99_ms", "enqueue_to_start_p99_ms", "throughput_per_second",
		"jain_fairness", "slo_violations", "nondominant_slo_violations",
		"peak_concurrency", "retries", "duplicates", "redis_commands", "recovery_ms",
	} {
		metrics[name] = experiment.MetricSummary{N: 1, Median: 1, Lo: 1, Hi: 1}
	}
	output := markdown(experiment.Analysis{
		Runs:          1,
		BootstrapSeed: 1,
		Resamples:     1,
		Cells: []experiment.Cell{{
			Manifest: "worker-crash",
			Variant:  "asynq",
			Status:   "not_measured",
			Metrics:  metrics,
		}},
	})

	separator := "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
	if !strings.Contains(output, separator) {
		t.Fatalf("markdown is missing the 11-column separator:\n%s", output)
	}
	if !strings.Contains(output, "| asynq | not measured | not measured |") {
		t.Fatalf("unsupported crash cell is not marked as unmeasured:\n%s", output)
	}
	if strings.Contains(output, "asynq 1 [1, 1] ms") {
		t.Fatalf("unsupported crash recovery was reported numerically:\n%s", output)
	}
}
