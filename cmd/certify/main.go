// certify generates a versioned, non-publishing reliability certification.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/aminkbi/taskforge/certification"
)

type stringsFlag []string

func (values *stringsFlag) String() string { return strings.Join(*values, ",") }
func (values *stringsFlag) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func main() {
	manifestPath := flag.String("manifest", "certification/manifest.json", "versioned certification manifest")
	inputPath := flag.String("input", "", "pre-recorded results JSON")
	run := flag.String("run", "", "comma-separated manifest check IDs to execute")
	jsonPath := flag.String("json", "dist/reliability-certification.json", "JSON report path")
	markdownPath := flag.String("markdown", "dist/reliability-certification.md", "Markdown report path")
	commit := flag.String("commit", env("TASKFORGE_COMMIT", git("rev-parse", "HEAD")), "source commit")
	epoch := flag.String("source-date-epoch", env("SOURCE_DATE_EPOCH", git("show", "-s", "--format=%ct", "HEAD")), "SOURCE_DATE_EPOCH for reproducible reports")
	allowIncomplete := flag.Bool("allow-incomplete", false, "write incomplete reports without failing")
	var results stringsFlag
	var artifacts stringsFlag
	flag.Var(&results, "result", "check result id=passed|failed|skipped[:detail] (repeatable)")
	flag.Var(&artifacts, "artifact", "artifact to hash (repeatable)")
	flag.Parse()

	manifest, err := certification.LoadManifest(*manifestPath)
	if err != nil {
		fatal(err)
	}
	allResults := make([]certification.CheckResult, 0)
	var deltas json.RawMessage
	if *inputPath != "" {
		inputs, err := certification.LoadInputs(*inputPath)
		if err != nil {
			fatal(err)
		}
		allResults = append(allResults, inputs.Checks...)
		deltas = inputs.BenchmarkDeltas
	}
	provided, err := parseResults(results)
	if err != nil {
		fatal(err)
	}
	allResults = append(allResults, provided...)
	if *run != "" {
		allResults, err = runChecks(manifest, allResults, *run)
		if err != nil {
			fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Dir(*jsonPath), 0o755); err != nil {
		fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(*markdownPath), 0o755); err != nil {
		fatal(err)
	}
	report, err := certification.BuildReport(manifest, certification.Provenance{Commit: *commit, SourceDateEpoch: *epoch}, allResults, deltas, artifacts)
	if err != nil {
		fatal(err)
	}
	if err := certification.Write(report, *jsonPath, *markdownPath); err != nil {
		fatal(err)
	}
	fmt.Printf("certification status=%s json=%s markdown=%s\n", report.Status, *jsonPath, *markdownPath)
	if report.Status == "failed" || (report.Status == "incomplete" && !*allowIncomplete) {
		os.Exit(1)
	}
}

func runChecks(manifest certification.Manifest, existing []certification.CheckResult, names string) ([]certification.CheckResult, error) {
	seen := make(map[string]struct{}, len(existing))
	for _, result := range existing {
		seen[result.ID] = struct{}{}
	}
	byID := make(map[string]certification.Check, len(manifest.Checks))
	for _, check := range manifest.Checks {
		byID[check.ID] = check
	}
	for _, id := range strings.Split(names, ",") {
		id = strings.TrimSpace(id)
		check, ok := byID[id]
		if !ok || id == "" {
			return nil, fmt.Errorf("unknown check %q", id)
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("result already supplied for check %q", id)
		}
		command := exec.Command("/bin/bash", "-lc", check.Command)
		output, err := command.CombinedOutput()
		result := certification.CheckResult{ID: id, Status: certification.Passed}
		if err != nil {
			result.Status = certification.Failed
			result.Detail = strings.TrimSpace(string(output))
			if result.Detail == "" {
				result.Detail = err.Error()
			}
		}
		existing = append(existing, result)
		seen[id] = struct{}{}
	}
	return existing, nil
}

func parseResults(values []string) ([]certification.CheckResult, error) {
	results := make([]certification.CheckResult, 0, len(values))
	for _, value := range values {
		parts := strings.SplitN(value, "=", 2)
		if len(parts) != 2 || parts[0] == "" {
			return nil, fmt.Errorf("invalid result %q", value)
		}
		statusAndDetail := strings.SplitN(parts[1], ":", 2)
		result := certification.CheckResult{ID: parts[0], Status: certification.Status(statusAndDetail[0])}
		if len(statusAndDetail) == 2 {
			result.Detail = statusAndDetail[1]
		}
		results = append(results, result)
	}
	return results, nil
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func git(args ...string) string {
	output, err := exec.Command("git", args...).Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(output))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "certify:", err)
	os.Exit(2)
}
