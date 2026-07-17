// Package certification builds reproducible release reliability reports.
package certification

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

const (
	ManifestSchemaVersion = "taskforge-certification-manifest/v2"
	ReportSchemaVersion   = "taskforge-reliability-certification/v1"
)

// Manifest is the executable check inventory. It intentionally contains only
// the fields a release report needs; the manifest test validates the rest.
type Manifest struct {
	SchemaVersion string       `json:"schema_version"`
	Checks        []Check      `json:"checks"`
	Assumptions   []Assumption `json:"assumptions"`
}

type Check struct {
	ID       string `json:"id"`
	Command  string `json:"command"`
	Required bool   `json:"required"`
}

type Assumption struct {
	ID        string `json:"id"`
	Statement string `json:"statement"`
}

type Status string

const (
	Passed  Status = "passed"
	Failed  Status = "failed"
	Skipped Status = "skipped"
)

func (s Status) valid() bool { return s == Passed || s == Failed || s == Skipped }

type CheckResult struct {
	ID     string `json:"id"`
	Status Status `json:"status"`
	Detail string `json:"detail,omitempty"`
}

// Inputs may be committed by CI or supplied by a previous check runner.
type Inputs struct {
	SchemaVersion   string          `json:"schema_version"`
	Checks          []CheckResult   `json:"checks"`
	BenchmarkDeltas json.RawMessage `json:"benchmark_deltas,omitempty"`
}

type Provenance struct {
	Commit          string `json:"commit"`
	SourceDateEpoch string `json:"source_date_epoch"`
}

type Environment struct {
	GoVersion string `json:"go_version"`
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
}

type Artifact struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type ReportCheck struct {
	ID       string `json:"id"`
	Required bool   `json:"required"`
	Command  string `json:"command"`
	Status   Status `json:"status"`
	Detail   string `json:"detail,omitempty"`
}

type Report struct {
	SchemaVersion   string          `json:"schema_version"`
	Provenance      Provenance      `json:"provenance"`
	Environment     Environment     `json:"environment"`
	Status          string          `json:"status"`
	Checks          []ReportCheck   `json:"checks"`
	SkippedChecks   []string        `json:"skipped_checks"`
	Assumptions     []Assumption    `json:"assumptions"`
	BenchmarkDeltas json.RawMessage `json:"benchmark_deltas"`
	Artifacts       []Artifact      `json:"artifacts"`
}

// LoadManifest reads the versioned manifest without accepting unknown fields.
func LoadManifest(path string) (Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	if manifest.SchemaVersion != ManifestSchemaVersion {
		return Manifest{}, fmt.Errorf("unsupported manifest schema %q", manifest.SchemaVersion)
	}
	return manifest, nil
}

// LoadInputs reads pre-recorded results. A missing schema version is rejected
// so results cannot silently drift from the report contract.
func LoadInputs(path string) (Inputs, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Inputs{}, err
	}
	var inputs Inputs
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&inputs); err != nil {
		return Inputs{}, fmt.Errorf("decode inputs: %w", err)
	}
	if inputs.SchemaVersion != "taskforge-certification-results/v1" {
		return Inputs{}, fmt.Errorf("unsupported results schema %q", inputs.SchemaVersion)
	}
	return inputs, nil
}

// BuildReport creates a deterministic report for the supplied immutable inputs.
// Missing checks are deliberately skipped; they are never upgraded to pass.
func BuildReport(manifest Manifest, provenance Provenance, results []CheckResult, deltas json.RawMessage, artifactPaths []string) (Report, error) {
	if manifest.SchemaVersion != ManifestSchemaVersion {
		return Report{}, fmt.Errorf("unsupported manifest schema %q", manifest.SchemaVersion)
	}
	if provenance.Commit == "" || provenance.SourceDateEpoch == "" {
		return Report{}, fmt.Errorf("commit and source date epoch are required for reproducible certification")
	}
	byID := make(map[string]CheckResult, len(results))
	for _, result := range results {
		if result.ID == "" || !result.Status.valid() {
			return Report{}, fmt.Errorf("invalid check result %q with status %q", result.ID, result.Status)
		}
		if _, exists := byID[result.ID]; exists {
			return Report{}, fmt.Errorf("duplicate result for check %q", result.ID)
		}
		byID[result.ID] = result
	}

	report := Report{
		SchemaVersion: ReportSchemaVersion,
		Provenance:    provenance,
		Environment:   Environment{GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH},
		Assumptions:   append([]Assumption(nil), manifest.Assumptions...),
		Artifacts:     make([]Artifact, 0, len(artifactPaths)),
	}
	if len(deltas) == 0 {
		report.BenchmarkDeltas = json.RawMessage("[]")
	} else if !json.Valid(deltas) {
		return Report{}, fmt.Errorf("benchmark deltas are not valid JSON")
	} else {
		report.BenchmarkDeltas = append(json.RawMessage(nil), deltas...)
	}

	known := make(map[string]struct{}, len(manifest.Checks))
	hasFailure, hasRequiredSkip := false, false
	for _, check := range manifest.Checks {
		if check.ID == "" || check.Command == "" {
			return Report{}, fmt.Errorf("manifest check must have id and command")
		}
		if _, exists := known[check.ID]; exists {
			return Report{}, fmt.Errorf("duplicate manifest check %q", check.ID)
		}
		known[check.ID] = struct{}{}
		result, exists := byID[check.ID]
		if !exists {
			result = CheckResult{ID: check.ID, Status: Skipped, Detail: "no result supplied"}
		}
		report.Checks = append(report.Checks, ReportCheck{ID: check.ID, Required: check.Required, Command: check.Command, Status: result.Status, Detail: result.Detail})
		if result.Status == Failed {
			hasFailure = true
		}
		if result.Status == Skipped {
			report.SkippedChecks = append(report.SkippedChecks, check.ID)
			if check.Required {
				hasRequiredSkip = true
			}
		}
	}
	for id := range byID {
		if _, exists := known[id]; !exists {
			return Report{}, fmt.Errorf("result supplied for unknown check %q", id)
		}
	}
	if hasFailure {
		report.Status = "failed"
	} else if hasRequiredSkip {
		report.Status = "incomplete"
	} else {
		report.Status = "passed"
	}

	paths := append([]string(nil), artifactPaths...)
	sort.Strings(paths)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return Report{}, fmt.Errorf("read artifact %q: %w", path, err)
		}
		digest := sha256.Sum256(data)
		report.Artifacts = append(report.Artifacts, Artifact{Path: filepath.ToSlash(path), SHA256: hex.EncodeToString(digest[:])})
	}
	return report, nil
}

// Write writes stable JSON and a concise, attachable Markdown rendering.
func Write(report Report, jsonPath, markdownPath string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.WriteFile(jsonPath, data, 0o644); err != nil {
		return err
	}
	var markdown strings.Builder
	fmt.Fprintf(&markdown, "# TaskForge reliability certification\n\nStatus: **%s**  \nCommit: `%s`  \nSource date epoch: `%s`  \nEnvironment: %s / %s / %s\n\n", report.Status, report.Provenance.Commit, report.Provenance.SourceDateEpoch, report.Environment.GoVersion, report.Environment.GOOS, report.Environment.GOARCH)
	markdown.WriteString("| Check | Required | Status |\n| --- | --- | --- |\n")
	for _, check := range report.Checks {
		fmt.Fprintf(&markdown, "| %s | %t | %s |\n", check.ID, check.Required, check.Status)
	}
	if len(report.SkippedChecks) > 0 {
		fmt.Fprintf(&markdown, "\nSkipped checks: %s.\n", strings.Join(report.SkippedChecks, ", "))
	}
	if len(report.Artifacts) > 0 {
		markdown.WriteString("\n## Artifact hashes\n\n")
		for _, artifact := range report.Artifacts {
			fmt.Fprintf(&markdown, "- `%s`: `%s`\n", artifact.Path, artifact.SHA256)
		}
	}
	assumptionIDs := make([]string, 0, len(report.Assumptions))
	for _, assumption := range report.Assumptions {
		assumptionIDs = append(assumptionIDs, assumption.ID)
	}
	fmt.Fprintf(&markdown, "\nAssumptions: %s.\n\nBenchmark deltas: `%s`.\n", strings.Join(assumptionIDs, ", "), string(report.BenchmarkDeltas))
	return os.WriteFile(markdownPath, []byte(markdown.String()), 0o644)
}
