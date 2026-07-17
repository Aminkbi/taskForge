package experiment

import (
	"math/rand/v2"
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
		if first.Contrasts[i] != second.Contrasts[i] {
			t.Fatalf("bootstrap not deterministic: %+v != %+v", first.Contrasts[i], second.Contrasts[i])
		}
	}
	if len(first.Cells) != 2 || first.Cells[0].Metrics["completion_p99_ms"].N != 12 {
		t.Fatalf("unexpected cells: %+v", first.Cells)
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
