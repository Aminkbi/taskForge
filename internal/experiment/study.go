package experiment

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

const (
	StudyPlanSchema     = "taskforge-paired-study-plan/v1"
	StudyDatasetSchema  = "taskforge-paired-study-dataset/v1"
	StudyAnalysisSchema = "taskforge-paired-study-analysis/v1"
)

type StudyPlan struct {
	Schema              string               `json:"schema"`
	FrozenAt            string               `json:"frozen_at"`
	BootstrapSeed       uint64               `json:"bootstrap_seed"`
	BootstrapIterations int                  `json:"bootstrap_iterations"`
	Environments        []StudyEnvironment   `json:"environments"`
	Profiles            []StudyProfile       `json:"profiles"`
	Contrasts           []RegisteredContrast `json:"contrasts"`
	Multiplicity        string               `json:"multiplicity"`
}

type StudyEnvironment struct {
	Name          string `json:"name"`
	GOMAXPROCS    int    `json:"gomaxprocs"`
	RedisTopology string `json:"redis_topology"`
	RedisNetwork  string `json:"redis_network"`
	Description   string `json:"description"`
}

type StudyProfile struct {
	Name            string   `json:"name"`
	File            string   `json:"file"`
	Seeds           []int64  `json:"seeds"`
	Repetitions     int      `json:"repetitions"`
	Systems         []string `json:"systems"`
	ProtectedTenant string   `json:"protected_tenant,omitempty"`
	DurationClass   string   `json:"duration_class"`
}

type RegisteredContrast struct {
	Name       string   `json:"name"`
	Family     string   `json:"family"`
	Profiles   []string `json:"profiles"`
	Left       string   `json:"left"`
	Right      string   `json:"right"`
	Metrics    []string `json:"metrics"`
	Primary    bool     `json:"primary"`
	Confidence float64  `json:"confidence"`
}

type StudyDataset struct {
	Schema       string     `json:"schema"`
	PlanSHA256   string     `json:"plan_sha256"`
	TraceLockSHA string     `json:"trace_lock_sha256"`
	BinarySHA256 string     `json:"binary_sha256"`
	SourceParent string     `json:"source_parent_commit"`
	Runs         []StudyRun `json:"runs"`
}

type StudyRun struct {
	Environment  string `json:"environment"`
	Profile      string `json:"profile"`
	TraceID      string `json:"trace_id"`
	TraceSHA256  string `json:"trace_sha256"`
	Seed         int64  `json:"seed"`
	Repetition   int    `json:"repetition"`
	System       string `json:"system"`
	SystemOrder  int    `json:"system_order"`
	Status       string `json:"status"`
	Failure      string `json:"failure,omitempty"`
	ResultFile   string `json:"result_file,omitempty"`
	ResultSHA256 string `json:"result_sha256,omitempty"`
}

type StudyMetric struct {
	Name            string   `json:"name"`
	Unit            string   `json:"unit"`
	Estimate        float64  `json:"paired_median_effect"`
	RelativePercent *float64 `json:"paired_median_relative_percent,omitempty"`
	Lower           float64  `json:"interval_lower"`
	Upper           float64  `json:"interval_upper"`
	Confidence      float64  `json:"confidence"`
	Pairs           int      `json:"pairs"`
	Standardized    float64  `json:"paired_standardized_effect"`
	LeftWinFraction float64  `json:"left_higher_fraction"`
}

type StudyContrastResult struct {
	Name        string        `json:"name"`
	Family      string        `json:"family"`
	Environment string        `json:"environment"`
	Profiles    []string      `json:"profiles"`
	Left        string        `json:"left"`
	Right       string        `json:"right"`
	Primary     bool          `json:"primary"`
	Metrics     []StudyMetric `json:"metrics"`
}

type StudyFailure struct {
	Environment string `json:"environment"`
	Profile     string `json:"profile"`
	Seed        int64  `json:"seed"`
	Repetition  int    `json:"repetition"`
	System      string `json:"system"`
	Status      string `json:"status"`
	Reason      string `json:"reason"`
}

type EnvironmentReversal struct {
	Contrast     string  `json:"contrast"`
	Metric       string  `json:"metric"`
	First        string  `json:"first_environment"`
	Second       string  `json:"second_environment"`
	FirstEffect  float64 `json:"first_effect"`
	SecondEffect float64 `json:"second_effect"`
}

type StudyAnalysis struct {
	Schema        string                `json:"schema"`
	PlanSHA256    string                `json:"plan_sha256"`
	DatasetSHA256 string                `json:"dataset_sha256"`
	Contrasts     []StudyContrastResult `json:"contrasts"`
	Failures      []StudyFailure        `json:"failures"`
	Reversals     []EnvironmentReversal `json:"environment_reversals"`
}

func LoadStudyPlan(path string) (StudyPlan, error) {
	var plan StudyPlan
	if err := decodeStrictJSON(path, &plan); err != nil {
		return plan, err
	}
	if err := plan.Validate(); err != nil {
		return plan, err
	}
	return plan, nil
}

func (p StudyPlan) Validate() error {
	if p.Schema != StudyPlanSchema || p.FrozenAt == "" || p.BootstrapIterations < 1000 || len(p.Environments) < 2 || len(p.Profiles) == 0 || len(p.Contrasts) == 0 || p.Multiplicity == "" {
		return errors.New("invalid paired study plan identity or design")
	}
	environments := map[string]bool{}
	for _, environment := range p.Environments {
		if environment.Name == "" || environments[environment.Name] || environment.GOMAXPROCS < 1 || (environment.RedisNetwork != "tcp" && environment.RedisNetwork != "unix") || environment.RedisTopology == "" || environment.Description == "" {
			return fmt.Errorf("invalid study environment %q", environment.Name)
		}
		environments[environment.Name] = true
	}
	profiles := map[string]StudyProfile{}
	for _, profile := range p.Profiles {
		if profile.Name == "" || profiles[profile.Name].Name != "" || filepath.Base(profile.File) != profile.File || len(profile.Seeds) < 2 || profile.Repetitions < 1 || len(profile.Systems) < 2 || profile.DurationClass == "" {
			return fmt.Errorf("invalid study profile %q", profile.Name)
		}
		if len(slices.Compact(slices.Clone(profile.Systems))) != len(profile.Systems) {
			return fmt.Errorf("duplicate system in %q", profile.Name)
		}
		profiles[profile.Name] = profile
	}
	for _, contrast := range p.Contrasts {
		if contrast.Name == "" || contrast.Family == "" || contrast.Left == contrast.Right || len(contrast.Profiles) == 0 || len(contrast.Metrics) == 0 || contrast.Confidence < .90 || contrast.Confidence >= 1 {
			return fmt.Errorf("invalid registered contrast %q", contrast.Name)
		}
		for _, name := range contrast.Profiles {
			profile, ok := profiles[name]
			if !ok || !slices.Contains(profile.Systems, contrast.Left) || !slices.Contains(profile.Systems, contrast.Right) {
				return fmt.Errorf("contrast %q is not supported by profile %q", contrast.Name, name)
			}
		}
	}
	return nil
}

func AnalyzeStudy(plan StudyPlan, dataset StudyDataset, dataDir, traceDir string) (StudyAnalysis, error) {
	if err := ValidateStudyDataset(plan, dataset, dataDir, traceDir); err != nil {
		return StudyAnalysis{}, err
	}
	planDigest, _ := fileSHA256(filepath.Join(filepath.Dir(traceDir), "study-plan.json"))
	datasetDigest, _ := fileSHA256(filepath.Join(dataDir, "dataset.json"))
	analysis := StudyAnalysis{Schema: StudyAnalysisSchema, PlanSHA256: planDigest, DatasetSHA256: datasetDigest}
	profiles := make(map[string]StudyProfile, len(plan.Profiles))
	traces := make(map[string]OpenLoopTrace)
	for _, profile := range plan.Profiles {
		profiles[profile.Name] = profile
		for _, seed := range profile.Seeds {
			path := filepath.Join(traceDir, fmt.Sprintf("%s-%d.json", profile.Name, seed))
			trace, err := LoadOpenLoopTrace(path)
			if err != nil {
				return analysis, err
			}
			traces[trace.ID] = trace
		}
	}
	values := map[string]map[string]float64{}
	for _, run := range dataset.Runs {
		if run.Status != RunStatusOK {
			analysis.Failures = append(analysis.Failures, StudyFailure{run.Environment, run.Profile, run.Seed, run.Repetition, run.System, run.Status, run.Failure})
			continue
		}
		result, err := loadOpenLoopResult(filepath.Join(dataDir, run.ResultFile))
		if err != nil {
			return analysis, err
		}
		key := studyBlockKey(run)
		if values[key] == nil {
			values[key] = map[string]float64{}
		}
		for metric, value := range studyMetrics(result, traces[run.TraceID], profiles[run.Profile].ProtectedTenant) {
			values[key][run.System+"\x00"+metric] = value
		}
	}
	for _, contrast := range plan.Contrasts {
		for _, environment := range plan.Environments {
			result := StudyContrastResult{Name: contrast.Name, Family: contrast.Family, Environment: environment.Name, Profiles: slices.Clone(contrast.Profiles), Left: contrast.Left, Right: contrast.Right, Primary: contrast.Primary}
			for metricIndex, metric := range contrast.Metrics {
				var left, right []float64
				keys := make([]string, 0, len(values))
				for key := range values {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				for _, key := range keys {
					parts := strings.Split(key, "\x00")
					if parts[0] != environment.Name || !slices.Contains(contrast.Profiles, parts[1]) {
						continue
					}
					l, lok := values[key][contrast.Left+"\x00"+metric]
					r, rok := values[key][contrast.Right+"\x00"+metric]
					if lok && rok {
						left, right = append(left, l), append(right, r)
					}
				}
				if len(left) == 0 {
					continue
				}
				result.Metrics = append(result.Metrics, pairedStudyMetric(metric, left, right, contrast.Confidence, plan.BootstrapSeed+uint64(metricIndex), plan.BootstrapIterations))
			}
			analysis.Contrasts = append(analysis.Contrasts, result)
		}
	}
	analysis.Reversals = findEnvironmentReversals(analysis.Contrasts)
	return analysis, nil
}

func ValidateStudyDataset(plan StudyPlan, dataset StudyDataset, dataDir, traceDir string) error {
	if dataset.Schema != StudyDatasetSchema || !fullHex(dataset.PlanSHA256, 64, 64) || !fullHex(dataset.TraceLockSHA, 64, 64) || !fullHex(dataset.BinarySHA256, 64, 64) || !fullHex(dataset.SourceParent, 40, 64) {
		return errors.New("invalid study dataset provenance")
	}
	planDigest, err := fileSHA256(filepath.Join(filepath.Dir(traceDir), "study-plan.json"))
	if err != nil || planDigest != dataset.PlanSHA256 {
		return errors.New("study plan digest differs from frozen plan")
	}
	lockDigest, err := fileSHA256(filepath.Join(filepath.Dir(traceDir), "trace-lock.json"))
	if err != nil || lockDigest != dataset.TraceLockSHA {
		return errors.New("trace lock digest differs from frozen corpus")
	}
	expected := map[string]bool{}
	for _, environment := range plan.Environments {
		for _, profile := range plan.Profiles {
			for _, seed := range profile.Seeds {
				trace, err := LoadOpenLoopTrace(filepath.Join(traceDir, fmt.Sprintf("%s-%d.json", profile.Name, seed)))
				if err != nil {
					return err
				}
				for repetition := range profile.Repetitions {
					for _, system := range profile.Systems {
						expected[studyCellKey(environment.Name, profile.Name, seed, repetition, system)] = trace.Digest == ""
					}
				}
			}
		}
	}
	seen := map[string]bool{}
	for _, run := range dataset.Runs {
		key := studyCellKey(run.Environment, run.Profile, run.Seed, run.Repetition, run.System)
		if _, ok := expected[key]; !ok || seen[key] {
			return fmt.Errorf("unexpected or duplicate study cell %s", key)
		}
		seen[key] = true
		if run.Status != RunStatusOK && run.Status != RunStatusFailed && run.Status != RunStatusNotMeasured {
			return fmt.Errorf("invalid status for %s", key)
		}
		if run.Status == RunStatusOK || run.Status == RunStatusNotMeasured {
			if run.ResultFile == "" || filepath.Clean(run.ResultFile) != run.ResultFile || strings.HasPrefix(run.ResultFile, "..") {
				return fmt.Errorf("unsafe result path for %s", key)
			}
			digest, err := fileSHA256(filepath.Join(dataDir, run.ResultFile))
			if err != nil || digest != run.ResultSHA256 {
				return fmt.Errorf("result digest mismatch for %s", key)
			}
			result, err := loadOpenLoopResult(filepath.Join(dataDir, run.ResultFile))
			if err != nil || result.System != run.System || result.SystemOrder != run.SystemOrder || result.TraceID != run.TraceID || result.TraceSHA256 != run.TraceSHA256 || result.Excluded != (run.Status == RunStatusNotMeasured) {
				return fmt.Errorf("result identity mismatch for %s", key)
			}
			if run.Status == RunStatusNotMeasured && run.Failure == "" {
				return fmt.Errorf("not-measured cell %s must retain a reason", key)
			}
		} else if run.Failure == "" || run.ResultFile != "" || run.ResultSHA256 != "" {
			return fmt.Errorf("failed cell %s must retain a reason and no result", key)
		}
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("study dataset has %d cells, want %d", len(seen), len(expected))
	}
	return nil
}

func studyMetrics(result OpenLoopResult, trace OpenLoopTrace, protected string) map[string]float64 {
	metrics := map[string]float64{}
	steadyStart := trace.StartAt.Add(trace.Profile.Warmup)
	steadyEnd := steadyStart.Add(trace.Profile.SteadyState)
	steadyIDs := make(map[string]bool)
	for _, arrival := range trace.Arrivals {
		if !arrival.At.Before(steadyStart) && arrival.At.Before(steadyEnd) {
			steadyIDs[arrival.ID] = true
		}
	}
	completed := map[string]TaskObservation{}
	for _, task := range result.Tasks {
		if task.Outcome == "completed" && steadyIDs[task.TaskID] {
			if _, ok := completed[task.TaskID]; !ok {
				completed[task.TaskID] = task
			}
		}
	}
	measurementStart := result.RunEpoch.Add(trace.Profile.Warmup)
	measurementEnd := measurementStart.Add(trace.Profile.SteadyState)
	lastCompletion := measurementEnd
	for _, task := range completed {
		if task.CompletedAt.After(lastCompletion) {
			lastCompletion = task.CompletedAt
		}
	}
	if elapsed := lastCompletion.Sub(measurementStart).Seconds(); elapsed > 0 {
		metrics["throughput_per_second"] = float64(len(completed)) / elapsed
	}
	var attainments []float64
	for _, tenant := range result.Tenants {
		attainments = append(attainments, tenant.SLOAttainment)
		if tenant.Tenant == protected {
			metrics["protected_slo_attainment"] = tenant.SLOAttainment
		}
		metrics["max_normalized_service_deficit"] = max(metrics["max_normalized_service_deficit"], tenant.NormalizedDeficit)
	}
	var sum, squares float64
	for _, value := range attainments {
		sum += value
		squares += value * value
	}
	if squares > 0 {
		metrics["jain_slo_equality"] = sum * sum / (float64(len(attainments)) * squares)
	}
	var over, failed int
	for _, observation := range result.Downstream {
		if observation.Overlap > observation.Capacity {
			over++
		}
		if observation.Failed {
			failed++
		}
	}
	if len(result.Downstream) > 0 {
		metrics["downstream_over_capacity_rate"] = float64(over) / float64(len(result.Downstream))
		metrics["downstream_failure_rate"] = float64(failed) / float64(len(result.Downstream))
	}
	metrics["cost_per_slo_completion"] = result.Cost.PerSLOCompletion
	metrics["enqueue_dispatch_p99_ms"] = float64(result.Harness.DispatchLag.P99) / 1e6
	return metrics
}

func pairedStudyMetric(name string, left, right []float64, confidence float64, seed uint64, iterations int) StudyMetric {
	differences := make([]float64, len(left))
	relatives := make([]float64, 0, len(left))
	leftHigher := 0
	for i := range left {
		differences[i] = left[i] - right[i]
		if right[i] != 0 {
			relatives = append(relatives, differences[i]/math.Abs(right[i])*100)
		}
		if left[i] > right[i] {
			leftHigher++
		}
	}
	estimate := medianFloat(differences)
	lo, hi := pairedBootstrapInterval(differences, confidence, seed, iterations)
	result := StudyMetric{Name: name, Unit: studyMetricUnit(name), Estimate: estimate, Lower: lo, Upper: hi, Confidence: confidence, Pairs: len(left), Standardized: standardizedPaired(differences), LeftWinFraction: float64(leftHigher) / float64(len(left))}
	if len(relatives) == len(left) {
		value := medianFloat(relatives)
		result.RelativePercent = &value
	}
	return result
}

func pairedBootstrapInterval(differences []float64, confidence float64, seed uint64, iterations int) (float64, float64) {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	values := make([]float64, iterations)
	sample := make([]float64, len(differences))
	for i := range iterations {
		for j := range sample {
			sample[j] = differences[rng.IntN(len(differences))]
		}
		values[i] = medianFloat(sample)
	}
	sort.Float64s(values)
	alpha := (1 - confidence) / 2
	return values[int(alpha*float64(iterations))], values[min(int((1-alpha)*float64(iterations)), iterations-1)]
}

func standardizedPaired(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	var mean float64
	for _, value := range values {
		mean += value
	}
	mean /= float64(len(values))
	var variance float64
	for _, value := range values {
		variance += (value - mean) * (value - mean)
	}
	variance /= float64(len(values) - 1)
	if variance == 0 {
		return 0
	}
	return mean / math.Sqrt(variance)
}

func findEnvironmentReversals(results []StudyContrastResult) []EnvironmentReversal {
	var reversals []EnvironmentReversal
	for i := range results {
		for j := i + 1; j < len(results); j++ {
			if results[i].Name != results[j].Name || results[i].Environment == results[j].Environment {
				continue
			}
			for _, first := range results[i].Metrics {
				for _, second := range results[j].Metrics {
					if first.Name == second.Name && first.Estimate*second.Estimate < 0 {
						reversals = append(reversals, EnvironmentReversal{results[i].Name, first.Name, results[i].Environment, results[j].Environment, first.Estimate, second.Estimate})
					}
				}
			}
		}
	}
	return reversals
}

func studyMetricUnit(name string) string {
	if strings.HasSuffix(name, "_rate") || strings.Contains(name, "attainment") || strings.Contains(name, "deficit") || strings.Contains(name, "equality") {
		return "proportion"
	}
	if strings.HasSuffix(name, "_ms") {
		return "milliseconds"
	}
	if strings.Contains(name, "cost") {
		return "normalized_cost_units"
	}
	return "tasks_per_second"
}

func studyBlockKey(run StudyRun) string {
	return fmt.Sprintf("%s\x00%s\x00%d\x00%d", run.Environment, run.Profile, run.Seed, run.Repetition)
}

func studyCellKey(environment, profile string, seed int64, repetition int, system string) string {
	return fmt.Sprintf("%s/%s/%d/%d/%s", environment, profile, seed, repetition, system)
}

func loadOpenLoopResult(path string) (OpenLoopResult, error) {
	file, err := os.Open(path)
	if err != nil {
		return OpenLoopResult{}, err
	}
	defer file.Close()
	var reader io.Reader = file
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return OpenLoopResult{}, err
		}
		defer gz.Close()
		reader = gz
	}
	var result OpenLoopResult
	if err := json.NewDecoder(reader).Decode(&result); err != nil {
		return result, err
	}
	if result.Schema != OpenLoopResultSchema {
		return result, fmt.Errorf("unsupported open-loop result schema %q", result.Schema)
	}
	return result, nil
}

func decodeStrictJSON(path string, target any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
