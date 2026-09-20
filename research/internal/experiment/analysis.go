package experiment

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
)

// AnalysisSchemaVersion identifies the derived-analysis contract, which is
// separate from the raw result schema it consumes.
const AnalysisSchemaVersion = "taskforge-analysis/v2"

var registeredManifests = []string{
	"delayed-backlog",
	"hot-dependency",
	"noisy-neighbor",
	"retry-storm",
	"tenant-skew",
	"worker-crash",
}

const (
	registeredFirstSeed = int64(20260717)
	registeredLastSeed  = int64(20260728)
)

func RegisteredManifests() []string { return slices.Clone(registeredManifests) }

func RegisteredSeeds() []int64 {
	seeds := make([]int64, 0, registeredLastSeed-registeredFirstSeed+1)
	for seed := registeredFirstSeed; seed <= registeredLastSeed; seed++ {
		seeds = append(seeds, seed)
	}
	return seeds
}

// ContrastMetrics are the pre-registered per-run metrics that participate in
// variant contrasts. Every other extracted metric is reported descriptively.
var ContrastMetrics = []string{
	"completion_p99_ms",
	"enqueue_to_start_p99_ms",
	"throughput_per_second",
	"jain_fairness",
	"slo_violations",
	"nondominant_slo_violations",
	"peak_concurrency",
}

// DescriptiveMetrics are reported per cell but never contrasted.
var DescriptiveMetrics = []string{
	"completion_p50_ms",
	"completion_p95_ms",
	"retries",
	"duplicates",
	"recovery_ms",
	"redis_commands",
}

// ContrastBase is the arm every pre-registered contrast subtracts from.
const ContrastBase = "taskforge-full"

// ContrastAgainst lists the pre-registered comparison arms in report order.
var ContrastAgainst = []string{
	"taskforge-fifo-static",
	"taskforge-no-fairness",
	"taskforge-no-admission",
	"taskforge-no-adaptive",
	"taskforge-no-dependency-budget",
}

type MetricSummary struct {
	N      int       `json:"n"`
	Median float64   `json:"median"`
	Lo     float64   `json:"ci95_lo"`
	Hi     float64   `json:"ci95_hi"`
	Values []float64 `json:"values"`
}

type Cell struct {
	Manifest string                   `json:"manifest"`
	Variant  string                   `json:"variant"`
	Status   string                   `json:"status,omitempty"`
	Seeds    []int64                  `json:"seeds"`
	Metrics  map[string]MetricSummary `json:"metrics"`
}

type Contrast struct {
	Manifest       string          `json:"manifest"`
	Metric         string          `json:"metric"`
	Base           string          `json:"base"`
	Against        string          `json:"against"`
	Difference     float64         `json:"difference_of_medians"`
	Lo             float64         `json:"ci95_lo"`
	Hi             float64         `json:"ci95_hi"`
	Detected       bool            `json:"detected"`
	RelativeChange *RelativeChange `json:"relative_change_percent,omitempty"`
}

type RelativeChange struct {
	Estimate float64 `json:"estimate"`
	Lo       float64 `json:"ci95_lo"`
	Hi       float64 `json:"ci95_hi"`
	Material bool    `json:"material_reduction"`
}

type Analysis struct {
	Schema          string     `json:"schema"`
	DatasetSchema   string     `json:"dataset_schema,omitempty"`
	SourceCommit    string     `json:"source_commit,omitempty"`
	BinarySHA256    string     `json:"binary_sha256,omitempty"`
	ResultSchema    string     `json:"result_schema,omitempty"`
	BootstrapSeed   uint64     `json:"bootstrap_seed"`
	Resamples       int        `json:"resamples"`
	Runs            int        `json:"runs"`
	MeasuredRuns    int        `json:"measured_runs"`
	NotMeasuredRuns int        `json:"not_measured_runs"`
	Workloads       []Manifest `json:"workloads"`
	Cells           []Cell     `json:"cells"`
	Contrasts       []Contrast `json:"contrasts"`
}

func (a *Analysis) AttachDataset(dataset Dataset) {
	a.DatasetSchema = dataset.Schema
	if len(dataset.Runs) == 0 {
		return
	}
	a.SourceCommit = dataset.Runs[0].SourceCommit
	a.BinarySHA256 = dataset.Runs[0].BinarySHA256
	a.ResultSchema = dataset.Runs[0].ResultSchema
}

// LoadRawResults reads every raw run in dir, accepting plain and gzip-encoded
// JSON so committed evidence can stay compressed.
func LoadRawResults(dir string) ([]Result, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json*"))
	if err != nil {
		return nil, err
	}
	results := make([]Result, 0, len(paths))
	for _, path := range paths {
		if !strings.HasSuffix(path, ".json") && !strings.HasSuffix(path, ".json.gz") {
			continue
		}
		result, err := loadRawResult(path)
		if err != nil {
			return nil, err
		}
		if result.Schema != SchemaVersion {
			return nil, fmt.Errorf("%s: unsupported schema %q", path, result.Schema)
		}
		results = append(results, result)
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no raw results in %s", dir)
	}
	slices.SortFunc(results, func(a, b Result) int {
		if c := strings.Compare(a.Manifest.Name, b.Manifest.Name); c != 0 {
			return c
		}
		if c := strings.Compare(a.Variant.Name, b.Variant.Name); c != 0 {
			return c
		}
		return int(a.Seed - b.Seed)
	})
	return results, nil
}

func loadRawResult(path string) (Result, error) {
	file, err := os.Open(path)
	if err != nil {
		return Result{}, err
	}
	var reader io.Reader = file
	var unzipped *gzip.Reader
	if strings.HasSuffix(path, ".gz") {
		unzipped, err = gzip.NewReader(file)
		if err != nil {
			file.Close()
			return Result{}, fmt.Errorf("open %s: %w", path, err)
		}
		reader = unzipped
	}
	decoder := json.NewDecoder(reader)
	var result Result
	decodeErr := decoder.Decode(&result)
	if decodeErr == nil {
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			if err == nil {
				decodeErr = fmt.Errorf("multiple JSON values")
			} else {
				decodeErr = err
			}
		}
	}
	if unzipped != nil {
		if err := unzipped.Close(); decodeErr == nil && err != nil {
			decodeErr = err
		}
	}
	if err := file.Close(); decodeErr == nil && err != nil {
		decodeErr = err
	}
	if decodeErr != nil {
		return Result{}, fmt.Errorf("decode %s: %w", path, decodeErr)
	}
	return result, nil
}

// ValidateRegisteredGrid rejects incomplete, duplicated, mixed-source, or
// privacy-leaking evidence before the publication report is generated.
func ValidateRegisteredGrid(results []Result) error {
	variants := Variants()
	expectedRuns := len(registeredManifests) * len(variants) * int(registeredLastSeed-registeredFirstSeed+1)
	if len(results) != expectedRuns {
		return fmt.Errorf("registered grid has %d runs, want %d", len(results), expectedRuns)
	}

	variantByName := make(map[string]Variant, len(variants))
	for _, variant := range variants {
		variantByName[variant.Name] = variant
	}
	manifestNames := make(map[string]bool, len(registeredManifests))
	for _, manifest := range registeredManifests {
		manifestNames[manifest] = true
	}

	seen := make(map[string]bool, expectedRuns)
	manifestDefinitions := make(map[string]string, len(registeredManifests))
	buildSHA := ""
	environment := ""
	for _, result := range results {
		if !manifestNames[result.Manifest.Name] {
			return fmt.Errorf("unexpected registered manifest %q", result.Manifest.Name)
		}
		canonicalVariant, ok := variantByName[result.Variant.Name]
		if !ok {
			return fmt.Errorf("unexpected registered variant %q", result.Variant.Name)
		}
		variantJSON, _ := json.Marshal(result.Variant)
		canonicalVariantJSON, _ := json.Marshal(canonicalVariant)
		if string(variantJSON) != string(canonicalVariantJSON) {
			return fmt.Errorf("variant metadata differs for %q", result.Variant.Name)
		}
		if result.Seed < registeredFirstSeed || result.Seed > registeredLastSeed {
			return fmt.Errorf("unexpected registered seed %d", result.Seed)
		}
		key := fmt.Sprintf("%s/%s/%d", result.Manifest.Name, result.Variant.Name, result.Seed)
		if seen[key] {
			return fmt.Errorf("duplicate registered cell %s", key)
		}
		seen[key] = true

		manifestJSON, _ := json.Marshal(result.Manifest)
		if previous, ok := manifestDefinitions[result.Manifest.Name]; ok && previous != string(manifestJSON) {
			return fmt.Errorf("manifest metadata differs across %q cells", result.Manifest.Name)
		}
		manifestDefinitions[result.Manifest.Name] = string(manifestJSON)
		if len(result.Samples) != result.Manifest.Tasks {
			return fmt.Errorf("%s has %d samples, want %d", key, len(result.Samples), result.Manifest.Tasks)
		}
		if result.Environment.BuildSHA == "" || result.Environment.BuildSHA == "unknown" {
			return fmt.Errorf("%s has no source revision", key)
		}
		if buildSHA == "" {
			buildSHA = result.Environment.BuildSHA
		} else if result.Environment.BuildSHA != buildSHA {
			return fmt.Errorf("mixed source revisions %q and %q", buildSHA, result.Environment.BuildSHA)
		}
		if result.Environment.Hostname != "research-host" {
			return fmt.Errorf("%s exposes non-neutral hostname %q", key, result.Environment.Hostname)
		}
		environmentJSON, _ := json.Marshal(struct {
			OS           string
			Architecture string
			GoVersion    string
			CPUs         int
			RedisConfig  string
		}{result.Environment.OS, result.Environment.Architecture, result.Environment.GoVersion, result.Environment.CPUs, result.Environment.RedisConfig})
		if environment == "" {
			environment = string(environmentJSON)
		} else if string(environmentJSON) != environment {
			return fmt.Errorf("mixed execution environments in %s", key)
		}
	}

	for _, manifest := range registeredManifests {
		for _, variant := range variants {
			for seed := registeredFirstSeed; seed <= registeredLastSeed; seed++ {
				key := fmt.Sprintf("%s/%s/%d", manifest, variant.Name, seed)
				if !seen[key] {
					return fmt.Errorf("missing registered cell %s", key)
				}
			}
		}
	}
	return nil
}

// PeakConcurrency is the maximum number of overlapping handler executions
// observed in a run's samples: the downstream-overload proxy.
func PeakConcurrency(samples []Sample) int {
	type edge struct {
		at    time.Time
		delta int
	}
	edges := make([]edge, 0, 2*len(samples))
	for _, s := range samples {
		if s.StartedAt.IsZero() || s.CompletedAt.IsZero() {
			continue
		}
		edges = append(edges, edge{s.StartedAt, 1}, edge{s.CompletedAt, -1})
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].at.Equal(edges[j].at) {
			return edges[i].delta < edges[j].delta
		}
		return edges[i].at.Before(edges[j].at)
	})
	current, peak := 0, 0
	for _, e := range edges {
		current += e.delta
		if current > peak {
			peak = current
		}
	}
	return peak
}

// nondominantTenants is every tenant except the single highest offered-weight
// tenant, the pre-registered focus of fairness and admission protection.
func nondominantTenants(tenants []Tenant) map[string]bool {
	dominant := ""
	weight := -1.0
	for _, tenant := range tenants {
		if tenant.Weight > weight {
			dominant, weight = tenant.Name, tenant.Weight
		}
	}
	result := make(map[string]bool, len(tenants))
	for _, tenant := range tenants {
		if tenant.Name != dominant {
			result[tenant.Name] = true
		}
	}
	return result
}

// RunMetrics extracts the per-run observations named in the analysis plan.
func RunMetrics(result Result) map[string]float64 {
	nondominant := nondominantTenants(result.Manifest.Tenants)
	violations := 0
	for _, sample := range result.Samples {
		if sample.SLOViolated && nondominant[sample.Tenant] {
			violations++
		}
	}
	s := result.Summary
	return map[string]float64{
		"completion_p50_ms":          float64(s.Completion.P50) / 1e6,
		"completion_p95_ms":          float64(s.Completion.P95) / 1e6,
		"completion_p99_ms":          float64(s.Completion.P99) / 1e6,
		"enqueue_to_start_p99_ms":    float64(s.EnqueueToStart.P99) / 1e6,
		"throughput_per_second":      s.Throughput,
		"jain_fairness":              s.JainFairness,
		"slo_violations":             float64(s.StarvationViolations),
		"nondominant_slo_violations": float64(violations),
		"retries":                    float64(s.Retries),
		"duplicates":                 float64(s.Duplicates),
		"recovery_ms":                float64(s.RecoveryTime) / 1e6,
		"redis_commands":             float64(s.Redis.TotalCommands),
		"peak_concurrency":           float64(PeakConcurrency(result.Samples)),
	}
}

func median(values []float64) float64 {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	n := len(sorted)
	if n == 0 {
		return math.NaN()
	}
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

func percentileSorted(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	index := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[max(min(index, len(sorted)-1), 0)]
}

func resampleMedian(values []float64, rng *rand.Rand) float64 {
	sample := make([]float64, len(values))
	for i := range sample {
		sample[i] = values[rng.IntN(len(values))]
	}
	return median(sample)
}

// bootstrapMedian returns the seeded percentile-bootstrap 95% interval of the
// median. It is deterministic for a given rng state and value order.
func bootstrapMedian(values []float64, resamples int, rng *rand.Rand) (lo, hi float64) {
	medians := make([]float64, resamples)
	for i := range medians {
		medians[i] = resampleMedian(values, rng)
	}
	slices.Sort(medians)
	return percentileSorted(medians, 0.025), percentileSorted(medians, 0.975)
}

// bootstrapDifference resamples two arms independently and returns the 95%
// interval of the difference of medians (base minus against).
func bootstrapDifference(base, against []float64, resamples int, rng *rand.Rand) (lo, hi float64) {
	diffs := make([]float64, resamples)
	for i := range diffs {
		diffs[i] = resampleMedian(base, rng) - resampleMedian(against, rng)
	}
	slices.Sort(diffs)
	return percentileSorted(diffs, 0.025), percentileSorted(diffs, 0.975)
}

func relativeChange(base, against float64) float64 {
	if against == 0 {
		return math.NaN()
	}
	return 100 * (base/against - 1)
}

func bootstrapRelativeChange(base, against []float64, resamples int, rng *rand.Rand) (lo, hi float64) {
	changes := make([]float64, resamples)
	for i := range changes {
		changes[i] = relativeChange(resampleMedian(base, rng), resampleMedian(against, rng))
	}
	slices.Sort(changes)
	return percentileSorted(changes, 0.025), percentileSorted(changes, 0.975)
}

// Analyze aggregates raw runs into the pre-registered cells and contrasts.
// Iteration order and the seeded generator make the output byte-reproducible.
func Analyze(results []Result, bootstrapSeed uint64, resamples int) Analysis {
	rng := rand.New(rand.NewPCG(bootstrapSeed, bootstrapSeed^0x9e3779b97f4a7c15))
	relativeRNG := rand.New(rand.NewPCG(bootstrapSeed^0xd1b54a32d192ed03, bootstrapSeed^0x94d049bb133111eb))
	type key struct{ manifest, variant string }
	grouped := make(map[key][]Result)
	var order []key
	for _, result := range results {
		k := key{result.Manifest.Name, result.Variant.Name}
		if _, seen := grouped[k]; !seen {
			order = append(order, k)
		}
		grouped[k] = append(grouped[k], result)
	}

	metricNames := slices.Concat(ContrastMetrics, DescriptiveMetrics)
	analysis := Analysis{Schema: AnalysisSchemaVersion, BootstrapSeed: bootstrapSeed, Resamples: resamples, Runs: len(results)}
	seenWorkload := map[string]bool{}
	for _, result := range results {
		if !seenWorkload[result.Manifest.Name] {
			analysis.Workloads = append(analysis.Workloads, result.Manifest)
			seenWorkload[result.Manifest.Name] = true
		}
	}
	values := make(map[key]map[string][]float64, len(order))
	for _, k := range order {
		runs := grouped[k]
		cell := Cell{Manifest: k.manifest, Variant: k.variant, Metrics: make(map[string]MetricSummary, len(metricNames))}
		if k.manifest == "worker-crash" && k.variant == "asynq" {
			cell.Status = "not_measured"
			for _, run := range runs {
				cell.Seeds = append(cell.Seeds, run.Seed)
			}
			analysis.NotMeasuredRuns += len(runs)
			analysis.Cells = append(analysis.Cells, cell)
			continue
		}
		analysis.MeasuredRuns += len(runs)
		perMetric := make(map[string][]float64, len(metricNames))
		for _, run := range runs {
			cell.Seeds = append(cell.Seeds, run.Seed)
			metrics := RunMetrics(run)
			for _, name := range metricNames {
				perMetric[name] = append(perMetric[name], metrics[name])
			}
		}
		for _, name := range metricNames {
			observed := perMetric[name]
			lo, hi := bootstrapMedian(observed, resamples, rng)
			cell.Metrics[name] = MetricSummary{N: len(observed), Median: median(observed), Lo: lo, Hi: hi, Values: observed}
		}
		values[k] = perMetric
		analysis.Cells = append(analysis.Cells, cell)
	}

	manifests := make([]string, 0, len(order))
	for _, k := range order {
		if !slices.Contains(manifests, k.manifest) {
			manifests = append(manifests, k.manifest)
		}
	}
	for _, manifest := range manifests {
		base, ok := values[key{manifest, ContrastBase}]
		if !ok {
			continue
		}
		for _, against := range ContrastAgainst {
			arm, ok := values[key{manifest, against}]
			if !ok {
				continue
			}
			for _, metric := range ContrastMetrics {
				difference := median(base[metric]) - median(arm[metric])
				lo, hi := bootstrapDifference(base[metric], arm[metric], resamples, rng)
				contrast := Contrast{
					Manifest:   manifest,
					Metric:     metric,
					Base:       ContrastBase,
					Against:    against,
					Difference: difference,
					Lo:         lo,
					Hi:         hi,
					Detected:   (lo > 0 && hi > 0) || (lo < 0 && hi < 0),
				}
				if metric == "throughput_per_second" && median(arm[metric]) != 0 {
					relativeLo, relativeHi := bootstrapRelativeChange(base[metric], arm[metric], resamples, relativeRNG)
					contrast.RelativeChange = &RelativeChange{
						Estimate: relativeChange(median(base[metric]), median(arm[metric])),
						Lo:       relativeLo,
						Hi:       relativeHi,
						Material: relativeHi < -10,
					}
				}
				analysis.Contrasts = append(analysis.Contrasts, contrast)
			}
		}
	}
	return analysis
}
