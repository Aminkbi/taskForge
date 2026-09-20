package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aminkbi/taskforge/research/internal/experiment"
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

func TestMarkdownReportsFamilyWiseStatusAndPointMasses(t *testing.T) {
	analysis := experiment.Analysis{
		Runs:      2,
		Resamples: 10000,
		Multiplicity: experiment.MultiplicitySummary{
			FamilySize: 2, Alpha: 0.05, Confidence: 1 - 0.025,
			Detections: 2, Survivors: 1, Degenerate: 1, DegenerateDetections: 1,
		},
		Contrasts: []experiment.Contrast{
			{
				Manifest: "wl", Metric: "throughput_per_second", Base: "taskforge-full", Against: "taskforge-fifo-static",
				Difference: -1, Lo: -2, Hi: -0.5, Detected: true,
				FamilyWise: experiment.FamilyWise{FamilySize: 2, Confidence: 1 - 0.025, Lo: -2.5, Hi: -0.1, Survives: true},
			},
			{
				Manifest: "wl", Metric: "peak_concurrency", Base: "taskforge-full", Against: "taskforge-fifo-static",
				Difference: -1, Lo: -1, Hi: -1, Detected: true,
				FamilyWise: experiment.FamilyWise{FamilySize: 2, Confidence: 1 - 0.025, Lo: -1, Hi: -1, Degenerate: true},
			},
		},
	}
	output := markdown(analysis)
	for _, want := range []string{
		"## Multiplicity of the confirmatory set",
		"| Family-wise interval | Family-wise |",
		"| survives |",
		"| degenerate |",
		"Of the 2 marginal detections, 1 survive the family-wise criterion.",
		"1 contrasts are point-mass, 1 of them marginal detections.",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("markdown is missing %q:\n%s", want, output)
		}
	}
}

func TestRenderPaperUsesOnlyGeneratedEvidenceTokens(t *testing.T) {
	dir := t.TempDir()
	template := filepath.Join(dir, "paper.template.md")
	output := filepath.Join(dir, "paper.md")
	content := "runs={{RUNS}} measured={{MEASURED_RUNS}} omitted={{NOT_MEASURED_RUNS}} workloads={{WORKLOADS}} variants={{VARIANTS}} resamples={{RESAMPLES}} source={{SOURCE_COMMIT}} binary={{BINARY_SHA256}} family={{FAMILY_SIZE}} confidence={{FAMILY_CONFIDENCE}} detected={{DETECTIONS}} survivors={{FAMILY_WISE_SURVIVORS}} point_mass={{DEGENERATE_CONTRASTS}} point_mass_detected={{DEGENERATE_DETECTIONS}}\n{{GENERATED_EVIDENCE}}\n"
	if err := os.WriteFile(template, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	analysis := experiment.Analysis{
		Runs: 504, MeasuredRuns: 492, NotMeasuredRuns: 12, Resamples: 10000,
		SourceCommit: "source", BinarySHA256: "binary", Workloads: []experiment.Manifest{{Name: "worker-crash"}},
		Cells: []experiment.Cell{{Manifest: "worker-crash", Variant: "asynq", Status: "not_measured"}},
		Multiplicity: experiment.MultiplicitySummary{
			FamilySize: 210, Confidence: 1 - 0.05/210,
			Detections: 79, Survivors: 52, Degenerate: 19, DegenerateDetections: 5,
		},
	}
	evidence := paperEvidence(analysis)
	if err := renderPaper(template, output, analysis, evidence); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	rendered := string(data)
	if strings.Contains(rendered, "{{") || !strings.Contains(rendered, "not measured | not measured") {
		t.Fatalf("paper was not fully generated:\n%s", rendered)
	}
	for _, want := range []string{"family=210", "detected=79", "survivors=52", "point_mass=19", "point_mass_detected=5"} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("paper is missing generated multiplicity %q:\n%s", want, rendered)
		}
	}
}
