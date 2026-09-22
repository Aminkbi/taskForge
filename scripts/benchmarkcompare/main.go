// Command benchmarkcompare compares repeated, same-host Go benchmark runs.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
)

type run struct {
	metadata   map[string]string
	samples    map[string]map[string][]float64
	iterations map[string][]int
}

func readRun(r io.Reader) (run, error) {
	result := run{metadata: make(map[string]string), samples: make(map[string]map[string][]float64), iterations: make(map[string][]int)}
	scanner := bufio.NewScanner(r)
	passed := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "PASS" {
			passed = true
		}
		if strings.HasPrefix(line, "FAIL") || strings.Contains(line, "--- SKIP:") {
			return run{}, fmt.Errorf("failed or skipped benchmark: %s", line)
		}
		for _, key := range []string{"goos:", "goarch:", "cpu:", "pkg:"} {
			if value, ok := strings.CutPrefix(line, key); ok {
				value = strings.TrimSpace(value)
				if previous, exists := result.metadata[key]; exists && previous != value {
					return run{}, fmt.Errorf("mixed %s metadata", key)
				}
				result.metadata[key] = value
			}
		}
		fields := strings.Fields(line)
		if len(fields) == 0 || !strings.HasPrefix(fields[0], "Benchmark") {
			continue
		}
		if len(fields) < 4 || len(fields)%2 != 0 {
			return run{}, fmt.Errorf("invalid benchmark line: %s", line)
		}
		iterations, err := strconv.Atoi(fields[1])
		if err != nil || iterations < 1 {
			return run{}, fmt.Errorf("invalid iterations: %s", line)
		}
		result.iterations[fields[0]] = append(result.iterations[fields[0]], iterations)
		metrics := result.samples[fields[0]]
		if metrics == nil {
			metrics = make(map[string][]float64)
			result.samples[fields[0]] = metrics
		}
		for index := 2; index < len(fields); index += 2 {
			value, err := strconv.ParseFloat(fields[index], 64)
			if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
				return run{}, fmt.Errorf("invalid metric: %s", line)
			}
			unit := fields[index+1]
			metrics[unit] = append(metrics[unit], value)
		}
	}
	if err := scanner.Err(); err != nil {
		return run{}, err
	}
	if !passed || len(result.samples) == 0 {
		return run{}, errors.New("no completed benchmark measurements; enable TASKFORGE_RUN_BENCHMARKS=1")
	}
	return result, nil
}

func median(values []float64) float64 {
	values = slices.Sorted(slices.Values(values))
	mid := len(values) / 2
	if len(values)%2 != 0 {
		return values[mid]
	}
	return (values[mid-1] + values[mid]) / 2
}

func compare(w io.Writer, before, after run) error {
	for _, key := range []string{"goos:", "goarch:", "cpu:", "pkg:"} {
		if before.metadata[key] == "" || before.metadata[key] != after.metadata[key] {
			return fmt.Errorf("missing or mismatched %s metadata", key)
		}
	}
	if len(before.samples) != len(after.samples) {
		return errors.New("benchmark sets differ")
	}
	names := make([]string, 0, len(before.samples))
	for name := range before.samples {
		names = append(names, name)
	}
	slices.Sort(names)
	var failures []error
	for _, name := range names {
		left, right := before.samples[name], after.samples[name]
		if right == nil {
			return fmt.Errorf("missing candidate benchmark %s", name)
		}
		if len(left) != len(right) {
			return fmt.Errorf("metric sets differ for %s", name)
		}
		if len(left["ns/op"]) < 5 || len(left["ns/op"]) != len(right["ns/op"]) {
			return fmt.Errorf("%s requires equal sample counts of at least five", name)
		}
		if !slices.Equal(before.iterations[name], after.iterations[name]) {
			return fmt.Errorf("%s requires matching iteration counts; use fixed -benchtime=Nx", name)
		}
		units := make([]string, 0, len(left))
		for unit := range left {
			units = append(units, unit)
		}
		slices.Sort(units)
		for _, unit := range units {
			values := left[unit]
			if len(values) != len(left["ns/op"]) || len(right[unit]) != len(values) {
				return fmt.Errorf("incomplete %s samples for %s", unit, name)
			}
			baseline, candidate := median(values), median(right[unit])
			change := float64(0)
			if baseline != 0 {
				change = 100 * (candidate/baseline - 1)
			} else if candidate > 0 {
				change = math.Inf(1)
			}
			fmt.Fprintf(w, "%s %s: %.3f -> %.3f (%+.1f%%)\n", name, unit, baseline, candidate, change)
			// Latency is noisy on shared hosts. Structural costs receive the same
			// tolerance for warm-up effects; network counters are server-wide and
			// therefore reported without enforcing a threshold.
			switch unit {
			case "ns/op", "B/op", "allocs/op", "redis_commands/op", "redis_round_trips/op":
				if change > 15 {
					failures = append(failures, fmt.Errorf("%s %s regressed %.1f%% (limit 15%%)", name, unit, change))
				}
			}
		}
	}
	return errors.Join(failures...)
}

func load(path string) (run, error) {
	file, err := os.Open(path)
	if err != nil {
		return run{}, err
	}
	defer file.Close()
	return readRun(file)
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: benchmarkcompare BEFORE AFTER")
		os.Exit(2)
	}
	before, err := load(os.Args[1])
	if err == nil {
		var after run
		after, err = load(os.Args[2])
		if err == nil {
			err = compare(os.Stdout, before, after)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Benchmark medians passed the 15% regression limit; this is a host-local check, not a statistical significance test.")
}
