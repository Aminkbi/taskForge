package main

import (
	"testing"

	"github.com/aminkbi/taskforge/certification"
)

func TestRunChecksRecordsDeliberateFailure(t *testing.T) {
	manifest := certification.Manifest{
		SchemaVersion: certification.ManifestSchemaVersion,
		Checks:        []certification.Check{{ID: "deliberate-failure", Command: "false", Required: true}},
	}
	results, err := runChecks(manifest, nil, "deliberate-failure")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Status != certification.Failed {
		t.Fatalf("results = %+v, want failed check", results)
	}
	report, err := certification.BuildReport(manifest, certification.Provenance{Commit: "test", SourceDateEpoch: "1700000000"}, results, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "failed" {
		t.Fatalf("report status = %q, want failed", report.Status)
	}
}
