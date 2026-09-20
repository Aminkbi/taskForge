package experiment

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

// nullGrid synthesizes one replication of the registered grid under a global
// null: every variant of every workload draws from the same distribution, so
// every pre-registered contrast is null by construction. It exists to measure
// what the inference rule does when no effect is present.
func nullGrid(rng *rand.Rand) []Result {
	base := time.Unix(1700000000, 0).UTC()
	var results []Result
	for _, manifest := range registeredManifests {
		for _, variant := range Variants() {
			for seed := registeredFirstSeed; seed <= registeredLastSeed; seed++ {
				samples := make([]Sample, 8)
				for i := range samples {
					start := rng.Float64() * 200
					samples[i] = Sample{
						Tenant:      []string{"hot", "cold"}[rng.IntN(2)],
						EnqueuedAt:  base,
						StartedAt:   base.Add(time.Duration(start) * time.Millisecond),
						CompletedAt: base.Add(time.Duration(start+1+rng.Float64()*20) * time.Millisecond),
						SLOViolated: rng.Float64() < 0.3,
					}
				}
				latency := 200 + 40*rng.NormFloat64()
				results = append(results, Result{
					Schema:   SchemaVersion,
					Seed:     seed,
					Manifest: Manifest{Name: manifest, Tasks: len(samples), Tenants: []Tenant{{Name: "hot", Weight: 8}, {Name: "cold", Weight: 1}}},
					Variant:  variant,
					Summary: Summary{
						Completion:           Percentiles{P99: time.Duration(latency*2) * time.Millisecond},
						EnqueueToStart:       Percentiles{P99: time.Duration(latency) * time.Millisecond},
						Throughput:           500 + 30*rng.NormFloat64(),
						JainFairness:         math.Min(1, math.Max(0, 0.97+0.02*rng.NormFloat64())),
						StarvationViolations: rng.IntN(12),
					},
					Samples: samples,
				})
			}
		}
	}
	return results
}

// TestMarginalRuleDoesNotBoundFamilyWiseErrorUnderTheNull measures the exposure
// the family-wise criterion exists to disclose: with no effect anywhere, the
// frozen marginal rule still reports at least one detection in most
// replications, while the family-wise column resolves far fewer. The criterion
// is deliberately conservative rather than a replacement rule, so this test
// asserts the direction of the correction, not nominal alpha coverage, which
// coarse discrete metrics keep out of reach for any quantile rule.
func TestMarginalRuleDoesNotBoundFamilyWiseErrorUnderTheNull(t *testing.T) {
	const (
		replications = 16
		resamples    = 200
	)
	marginalReplications := 0
	familyReplications := 0
	detections := 0
	survivors := 0
	degenerateDetections := 0

	for replication := range replications {
		rng := rand.New(rand.NewPCG(uint64(replication)+1, 99))
		analysis := Analyze(nullGrid(rng), uint64(1800000000+replication), resamples)
		if len(analysis.Contrasts) == 0 {
			t.Fatal("null replication produced no contrasts")
		}
		marginalHit := false
		familyHit := false
		for _, contrast := range analysis.Contrasts {
			if contrast.Detected {
				detections++
				marginalHit = true
			}
			if contrast.FamilyWise.Survives {
				survivors++
				familyHit = true
				if !contrast.Detected {
					t.Fatalf("replication %d: %s/%s survives without a marginal detection", replication, contrast.Metric, contrast.Against)
				}
			}
			if contrast.FamilyWise.Degenerate && contrast.Detected {
				degenerateDetections++
			}
		}
		if marginalHit {
			marginalReplications++
		}
		if familyHit {
			familyReplications++
		}
	}

	t.Logf("null calibration: marginal detections=%d survivors=%d degenerate detections=%d", detections, survivors, degenerateDetections)
	t.Logf("replications with at least one marginal detection: %d/%d; with at least one family-wise survivor: %d/%d",
		marginalReplications, replications, familyReplications, replications)

	if marginalReplications <= replications/2 {
		t.Fatalf("the marginal rule produced a spurious detection in only %d/%d null replications", marginalReplications, replications)
	}
	if survivors >= detections {
		t.Fatalf("family-wise criterion removed nothing: %d detections, %d survivors", detections, survivors)
	}
}
