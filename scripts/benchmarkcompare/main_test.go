package main

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

func fixture(name string, samples int, latency int) string {
	var output strings.Builder
	output.WriteString("goos: linux\ngoarch: amd64\ncpu: test\npkg: test/benchmark\n")
	for range samples {
		fmt.Fprintf(&output, "%s 100 %d ns/op 40 B/op 2 allocs/op\n", name, latency)
	}
	output.WriteString("PASS\n")
	return output.String()
}

func TestComparisonValidatesMeasuredRuns(t *testing.T) {
	baseline, err := readRun(strings.NewReader(fixture("BenchmarkPublish-8", 5, 100)))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, input string
		wantError   bool
	}{
		{"improved", fixture("BenchmarkPublish-8", 5, 80), false},
		{"regressed", fixture("BenchmarkPublish-8", 5, 130), true},
		{"too_few", fixture("BenchmarkPublish-8", 1, 80), true},
		{"unequal_counts", fixture("BenchmarkPublish-8", 6, 80), true},
		{"different_benchmark", fixture("BenchmarkOther-8", 5, 80), true},
		{"different_host", strings.ReplaceAll(fixture("BenchmarkPublish-8", 5, 80), "cpu: test", "cpu: other"), true},
		{"different_iterations", strings.ReplaceAll(fixture("BenchmarkPublish-8", 5, 80), " 100 ", " 200 "), true},
		{"missing_metric", strings.ReplaceAll(fixture("BenchmarkPublish-8", 5, 80), " 2 allocs/op", ""), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate, err := readRun(strings.NewReader(test.input))
			if err != nil {
				t.Fatal(err)
			}
			if err := compare(io.Discard, baseline, candidate); (err != nil) != test.wantError {
				t.Fatalf("compare error = %v, want error %t", err, test.wantError)
			}
		})
	}
}

func TestRejectsMissingOrInvalidMeasurements(t *testing.T) {
	for _, input := range []string{
		"PASS\n", "FAIL\n", "--- SKIP: BenchmarkPublish\nPASS\n",
		"BenchmarkPublish 0 10 ns/op\nPASS\n",
		"BenchmarkPublish 100 NaN ns/op\nPASS\n",
		"BenchmarkPublish 100 10 ns/op\n",
	} {
		if _, err := readRun(strings.NewReader(input)); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
}
