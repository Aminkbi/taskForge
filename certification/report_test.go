package certification

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testManifest() Manifest {
	return Manifest{
		SchemaVersion: ManifestSchemaVersion,
		Checks: []Check{
			{ID: "unit", Command: "make test", Required: true},
			{ID: "artifact", Command: "make release-validate", Required: true},
			{ID: "optional", Command: "make docs-check"},
		},
		Assumptions: []Assumption{{ID: "idempotency", Statement: "handlers are idempotent"}},
	}
}

func TestBuildReportRecordsProvenanceAndRequiredSkips(t *testing.T) {
	dir := t.TempDir()
	artifact := filepath.Join(dir, "artifact.txt")
	if err := os.WriteFile(artifact, []byte("evidence"), 0o644); err != nil {
		t.Fatal(err)
	}
	report, err := BuildReport(testManifest(), Provenance{Commit: "abc123", SourceDateEpoch: "1700000000"}, []CheckResult{{ID: "unit", Status: Passed}}, json.RawMessage(`[{"metric":"throughput","delta_percent":2.5}]`), []string{artifact})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "incomplete" {
		t.Fatalf("status = %q, want incomplete", report.Status)
	}
	if report.Provenance.Commit != "abc123" || report.Provenance.SourceDateEpoch != "1700000000" {
		t.Fatalf("provenance = %+v", report.Provenance)
	}
	if got, want := strings.Join(report.SkippedChecks, ","), "artifact,optional"; got != want {
		t.Fatalf("skipped checks = %q, want %q", got, want)
	}
	for _, check := range report.Checks {
		if check.ID == "artifact" && check.Status != Skipped {
			t.Fatalf("required skipped check status = %q, want skipped", check.Status)
		}
	}
	if len(report.Artifacts) != 1 || report.Artifacts[0].SHA256 == "" {
		t.Fatalf("artifacts = %+v, want hash", report.Artifacts)
	}

	jsonPath := filepath.Join(dir, "report.json")
	markdownPath := filepath.Join(dir, "report.md")
	if err := Write(report, jsonPath, markdownPath); err != nil {
		t.Fatal(err)
	}
	markdown, err := os.ReadFile(markdownPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"incomplete", "abc123", "idempotency", "throughput", report.Artifacts[0].SHA256} {
		if !strings.Contains(string(markdown), want) {
			t.Errorf("markdown missing %q:\n%s", want, markdown)
		}
	}
}

func TestBuildReportFailsForFailingCheck(t *testing.T) {
	report, err := BuildReport(testManifest(), Provenance{Commit: "abc123", SourceDateEpoch: "1700000000"}, []CheckResult{{ID: "unit", Status: Failed, Detail: "deliberate failure"}, {ID: "artifact", Status: Passed}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "failed" {
		t.Fatalf("status = %q, want failed", report.Status)
	}
}

func TestBuildReportIsReproducible(t *testing.T) {
	manifest := testManifest()
	results := []CheckResult{{ID: "unit", Status: Passed}, {ID: "artifact", Status: Passed}}
	first, err := BuildReport(manifest, Provenance{Commit: "abc123", SourceDateEpoch: "1700000000"}, results, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildReport(manifest, Provenance{Commit: "abc123", SourceDateEpoch: "1700000000"}, results, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	left, _ := json.Marshal(first)
	right, _ := json.Marshal(second)
	if string(left) != string(right) {
		t.Fatalf("same inputs produced different reports\n%s\n%s", left, right)
	}
}

func TestLoadInputsConsumesVersionedResults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.json")
	data := []byte(`{"schema_version":"taskforge-certification-results/v1","checks":[{"id":"benchmark-regression","status":"passed"}],"benchmark_deltas":[{"metric":"p99","delta_percent":-4.2}]}`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	inputs, err := LoadInputs(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs.Checks) != 1 || inputs.Checks[0].ID != "benchmark-regression" || !strings.Contains(string(inputs.BenchmarkDeltas), "p99") {
		t.Fatalf("inputs = %+v", inputs)
	}
}
