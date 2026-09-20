package experiment

import (
	"compress/gzip"
	"encoding/json"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func sampleAt(base time.Time, tenant string, startMS, endMS int, violated bool) Sample {
	return Sample{
		Tenant:      tenant,
		EnqueuedAt:  base,
		StartedAt:   base.Add(time.Duration(startMS) * time.Millisecond),
		CompletedAt: base.Add(time.Duration(endMS) * time.Millisecond),
		SLOViolated: violated,
	}
}

func TestPeakConcurrencyCountsOverlapsNotTouches(t *testing.T) {
	base := time.Now()
	samples := []Sample{
		sampleAt(base, "a", 0, 10, false),
		sampleAt(base, "a", 5, 15, false),
		sampleAt(base, "b", 15, 20, false), // starts exactly when one ends
		sampleAt(base, "b", 30, 40, false),
	}
	if peak := PeakConcurrency(samples); peak != 2 {
		t.Fatalf("PeakConcurrency() = %d, want 2", peak)
	}
}

func TestRunMetricsNondominantViolations(t *testing.T) {
	base := time.Now()
	result := Result{
		Manifest: Manifest{Tenants: []Tenant{{Name: "hot", Weight: 8}, {Name: "cold", Weight: 1}}},
		Samples: []Sample{
			sampleAt(base, "hot", 0, 10, true),
			sampleAt(base, "cold", 0, 10, true),
			sampleAt(base, "cold", 0, 10, false),
		},
	}
	metrics := RunMetrics(result)
	if metrics["nondominant_slo_violations"] != 1 {
		t.Fatalf("nondominant violations = %v, want 1", metrics["nondominant_slo_violations"])
	}
	if metrics["peak_concurrency"] != 3 {
		t.Fatalf("peak concurrency = %v, want 3", metrics["peak_concurrency"])
	}
}

func testResult(manifest, variant string, seed int64, p99 time.Duration) Result {
	base := time.Unix(1700000000, 0).UTC()
	return Result{
		Schema:   SchemaVersion,
		Seed:     seed,
		Manifest: Manifest{Name: manifest, Tenants: []Tenant{{Name: "a", Weight: 1}}},
		Variant:  Variant{Name: variant},
		Summary:  Summary{Completion: Percentiles{P99: p99}},
		Samples:  []Sample{sampleAt(base, "a", 0, int(p99/time.Millisecond), false)},
	}
}

func TestAnalyzeIsDeterministicAndDetectsSeparatedArms(t *testing.T) {
	var results []Result
	for seed := int64(1); seed <= 12; seed++ {
		// The full arm is consistently ~40ms faster with disjoint ranges, so
		// the pre-registered interval must exclude zero.
		results = append(results,
			testResult("wl", "taskforge-full", seed, time.Duration(10+seed)*time.Millisecond),
			testResult("wl", "taskforge-fifo-static", seed, time.Duration(50+seed)*time.Millisecond),
		)
	}
	first := Analyze(results, 20260717, 2000)
	second := Analyze(results, 20260717, 2000)

	var contrast *Contrast
	for i := range first.Contrasts {
		if first.Contrasts[i].Metric == "completion_p99_ms" && first.Contrasts[i].Against == "taskforge-fifo-static" {
			contrast = &first.Contrasts[i]
		}
	}
	if contrast == nil {
		t.Fatal("missing pre-registered contrast")
	}
	if !contrast.Detected || contrast.Difference != -40 || contrast.Hi >= 0 {
		t.Fatalf("contrast = %+v, want detected difference of -40 with negative interval", *contrast)
	}
	for i := range first.Contrasts {
		if !reflect.DeepEqual(first.Contrasts[i], second.Contrasts[i]) {
			t.Fatalf("bootstrap not deterministic: %+v != %+v", first.Contrasts[i], second.Contrasts[i])
		}
	}
	if len(first.Cells) != 2 || first.Cells[0].Metrics["completion_p99_ms"].N != 12 {
		t.Fatalf("unexpected cells: %+v", first.Cells)
	}
}

func TestAnalyzeAppliesRegisteredThroughputMaterialityBound(t *testing.T) {
	var results []Result
	for seed := int64(1); seed <= 12; seed++ {
		full := testResult("wl", "taskforge-full", seed, 10*time.Millisecond)
		full.Summary.Throughput = 80
		fifo := testResult("wl", "taskforge-fifo-static", seed, 10*time.Millisecond)
		fifo.Summary.Throughput = 100
		results = append(results, full, fifo)
	}
	analysis := Analyze(results, 20260717, 2000)
	for _, contrast := range analysis.Contrasts {
		if contrast.Metric != "throughput_per_second" || contrast.Against != "taskforge-fifo-static" {
			continue
		}
		if contrast.RelativeChange == nil || math.Abs(contrast.RelativeChange.Estimate-(-20)) > 1e-9 || !contrast.RelativeChange.Material {
			t.Fatalf("relative throughput contrast = %+v, want a material -20%% reduction", contrast.RelativeChange)
		}
		return
	}
	t.Fatal("missing throughput contrast")
}

func TestAnalyzeMarksUnsupportedBaselineCrashCellNotMeasured(t *testing.T) {
	result := testResult("worker-crash", "asynq", 1, time.Millisecond)
	analysis := Analyze([]Result{result}, 1, 10)
	if len(analysis.Cells) != 1 || analysis.Cells[0].Status != "not_measured" || len(analysis.Cells[0].Metrics) != 0 {
		t.Fatalf("unsupported crash cell = %+v, want status not_measured without metrics", analysis.Cells)
	}
}

func TestBootstrapMedianCoversPointEstimate(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	lo, hi := bootstrapMedian(values, 4000, rng)
	if lo > 6.5 || hi < 6.5 || lo < 1 || hi > 12 {
		t.Fatalf("bootstrap interval [%v, %v] should cover the median 6.5 within the data range", lo, hi)
	}
}

func registeredTestGrid() []Result {
	variants := Variants()
	results := make([]Result, 0, len(registeredManifests)*len(variants)*int(registeredLastSeed-registeredFirstSeed+1))
	for _, manifest := range registeredManifests {
		for _, variant := range variants {
			for seed := registeredFirstSeed; seed <= registeredLastSeed; seed++ {
				base := time.Unix(seed, 0).UTC()
				results = append(results, Result{
					Schema:   SchemaVersion,
					Seed:     seed,
					Manifest: Manifest{Name: manifest, Tasks: 1, Tenants: []Tenant{{Name: "a", Weight: 1}}, SLO: time.Second},
					Variant:  variant,
					Environment: Environment{
						BuildSHA: "0123456789012345678901234567890123456789", Hostname: "research-host",
						OS: "linux", Architecture: "amd64", GoVersion: "go-test", CPUs: 1, RedisConfig: "test",
					},
					Samples: []Sample{sampleAt(base, "a", 0, 1, false)},
				})
			}
		}
	}
	return results
}

func TestValidateRegisteredGrid(t *testing.T) {
	results := registeredTestGrid()
	if err := ValidateRegisteredGrid(results); err != nil {
		t.Fatalf("ValidateRegisteredGrid() error = %v", err)
	}

	t.Run("missing", func(t *testing.T) {
		if err := ValidateRegisteredGrid(results[:len(results)-1]); err == nil {
			t.Fatal("ValidateRegisteredGrid() accepted a missing cell")
		}
	})
	t.Run("duplicate", func(t *testing.T) {
		duplicated := append(append([]Result(nil), results[:len(results)-1]...), results[0])
		if err := ValidateRegisteredGrid(duplicated); err == nil {
			t.Fatal("ValidateRegisteredGrid() accepted a duplicate cell")
		}
	})
	t.Run("mixed source", func(t *testing.T) {
		mixed := append([]Result(nil), results...)
		mixed[len(mixed)-1].Environment.BuildSHA = "different"
		if err := ValidateRegisteredGrid(mixed); err == nil {
			t.Fatal("ValidateRegisteredGrid() accepted mixed source revisions")
		}
	})
	t.Run("hostname", func(t *testing.T) {
		exposed := append([]Result(nil), results...)
		exposed[len(exposed)-1].Environment.Hostname = "private-host"
		if err := ValidateRegisteredGrid(exposed); err == nil {
			t.Fatal("ValidateRegisteredGrid() accepted a non-neutral hostname")
		}
	})
}

func writeRegisteredDataset(t *testing.T) (string, []Result, Dataset) {
	t.Helper()
	rawDir := filepath.Join(t.TempDir(), "raw")
	if err := os.MkdirAll(rawDir, 0755); err != nil {
		t.Fatal(err)
	}
	results := registeredTestGrid()
	redis := RedisConfiguration{
		Address: "localhost:6379", DB: 14,
		Values: map[string]string{"appendfsync": "everysec", "appendonly": "yes", "maxmemory-policy": "noeviction", "save": "60 1"},
	}
	var runs []RunProvenance
	for i := range results {
		result := &results[i]
		result.Environment.RedisConfig = encodeRedisConfiguration(redis)
		name := result.Manifest.Name + "--" + result.Variant.Name + "--" + strconv.FormatInt(result.Seed, 10) + ".json.gz"
		path := filepath.Join(rawDir, name)
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		writer := gzip.NewWriter(file)
		if err := json.NewEncoder(writer).Encode(result); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		digest, err := SHA256File(path)
		if err != nil {
			t.Fatal(err)
		}
		status := RunStatusOK
		if result.Manifest.Name == "worker-crash" && result.Variant.Name == "asynq" {
			status = RunStatusNotMeasured
		}
		runs = append(runs, RunProvenance{
			Manifest: result.Manifest.Name, Variant: result.Variant.Name, Seed: result.Seed,
			Status: status, ResultFile: name, ResultSHA256: digest, ResultSchema: SchemaVersion,
			SourceCommit: result.Environment.BuildSHA, SourceTree: strings.Repeat("1", 40), BinarySHA256: strings.Repeat("2", 64),
			BuildArguments:  []string{"go", "build", "-trimpath", "-buildvcs=false", "-o", "<temporary>/experiment", "./cmd/experiment"},
			DependencyLocks: []FileDigest{{Path: "go.mod", SHA256: strings.Repeat("3", 64)}, {Path: "go.sum", SHA256: strings.Repeat("4", 64)}},
			RunnerArguments: ExpectedRunnerArguments(result.Manifest.Name, result.Variant.Name, result.Seed),
			Redis:           redis,
			Environment: SanitizedEnvironment{
				OS: result.Environment.OS, Architecture: result.Environment.Architecture,
				GoVersion: result.Environment.GoVersion, CPUs: result.Environment.CPUs,
				CGOEnabled: false, GOMAXPROCS: 1,
			},
		})
	}
	return rawDir, results, Dataset{Schema: DatasetSchemaVersion, Publishable: true, Runs: runs}
}

func TestValidateRegisteredDatasetRejectsIntegrityDefects(t *testing.T) {
	rawDir, results, dataset := writeRegisteredDataset(t)
	if err := ValidateRegisteredDataset(rawDir, results, dataset); err != nil {
		t.Fatalf("ValidateRegisteredDataset() error = %v", err)
	}

	tests := map[string]func(*Dataset){
		"pilot":          func(d *Dataset) { d.Publishable = false },
		"failed":         func(d *Dataset) { d.Runs[0].Status = RunStatusFailed },
		"duplicate":      func(d *Dataset) { d.Runs[len(d.Runs)-1] = d.Runs[0] },
		"mixed revision": func(d *Dataset) { d.Runs[len(d.Runs)-1].SourceCommit = strings.Repeat("a", 40) },
		"mixed schema":   func(d *Dataset) { d.Runs[len(d.Runs)-1].ResultSchema = "old" },
		"privacy home": func(d *Dataset) {
			d.Runs[len(d.Runs)-1].RunnerArguments = append(d.Runs[len(d.Runs)-1].RunnerArguments, "/home/alice/result")
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			copy := dataset
			copy.Runs = append([]RunProvenance(nil), dataset.Runs...)
			mutate(&copy)
			if err := ValidateRegisteredDataset(rawDir, results, copy); err == nil {
				t.Fatal("ValidateRegisteredDataset() accepted invalid dataset")
			}
		})
	}

	t.Run("missing result bytes", func(t *testing.T) {
		path := filepath.Join(rawDir, dataset.Runs[0].ResultFile)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := ValidateRegisteredDataset(rawDir, results, dataset); err == nil {
			t.Fatal("ValidateRegisteredDataset() accepted missing raw bytes")
		}
	})
}

func TestPrivacyLeakRejectsUserIdentifiers(t *testing.T) {
	for _, value := range []string{
		`{"username":"alice"}`,
		`{"arg":"/home/alice/results"}`,
		`{"contact":"alice@example.test"}`,
	} {
		if leak := privacyLeak(value); leak == "" {
			t.Errorf("privacyLeak(%q) accepted a user identifier", value)
		}
	}
	if leak := privacyLeak(`{"hostname":"research-host","address":"localhost:6379"}`); leak != "" {
		t.Fatalf("privacyLeak() rejected neutral provenance: %s", leak)
	}
}
