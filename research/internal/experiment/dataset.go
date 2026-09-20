package experiment

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const DatasetSchemaVersion = "taskforge-research-dataset/v1"

const (
	RunStatusOK          = "ok"
	RunStatusNotMeasured = "not_measured"
	RunStatusFailed      = "failed"
)

// FileDigest identifies immutable input and output bytes used by a run.
type FileDigest struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// SanitizedEnvironment is the explicit allowlist of environment facts that
// are useful for reproduction. It intentionally has no generic map: HOME,
// USER, hostname, CI, and credential variables must never enter the artifact.
type SanitizedEnvironment struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	GoVersion    string `json:"go_version"`
	CPUs         int    `json:"cpus"`
	CGOEnabled   bool   `json:"cgo_enabled"`
	GOMAXPROCS   int    `json:"gomaxprocs"`
}

type RedisConfiguration struct {
	Address string            `json:"address"`
	DB      int               `json:"db"`
	Values  map[string]string `json:"values"`
}

// RunProvenance repeats the complete provenance for each registered cell.
// Repetition is deliberate: a run remains independently interpretable if it
// is extracted from the dataset ledger.
type RunProvenance struct {
	Manifest        string               `json:"manifest"`
	Variant         string               `json:"variant"`
	Seed            int64                `json:"seed"`
	Status          string               `json:"status"`
	ResultFile      string               `json:"result_file"`
	ResultSHA256    string               `json:"result_sha256"`
	ResultSchema    string               `json:"result_schema"`
	SourceCommit    string               `json:"source_commit"`
	SourceTree      string               `json:"source_tree"`
	BinarySHA256    string               `json:"binary_sha256"`
	BuildArguments  []string             `json:"build_arguments"`
	DependencyLocks []FileDigest         `json:"dependency_locks"`
	RunnerArguments []string             `json:"runner_arguments"`
	Redis           RedisConfiguration   `json:"redis"`
	Environment     SanitizedEnvironment `json:"environment"`
}

type Dataset struct {
	Schema      string          `json:"schema"`
	Publishable bool            `json:"publishable"`
	Runs        []RunProvenance `json:"runs"`
}

func LoadRegisteredDataset(dataDir string) ([]Result, Dataset, error) {
	data, err := os.ReadFile(filepath.Join(dataDir, "dataset.json"))
	if err != nil {
		return nil, Dataset{}, fmt.Errorf("read dataset ledger: %w", err)
	}
	var dataset Dataset
	if err := json.Unmarshal(data, &dataset); err != nil {
		return nil, Dataset{}, fmt.Errorf("decode dataset ledger: %w", err)
	}
	results, err := LoadRawResults(filepath.Join(dataDir, "raw"))
	if err != nil {
		return nil, Dataset{}, err
	}
	if err := ValidateRegisteredDataset(filepath.Join(dataDir, "raw"), results, dataset); err != nil {
		return nil, Dataset{}, err
	}
	return results, dataset, nil
}

// ValidateRegisteredDataset verifies both the complete experimental grid and
// the bytes/provenance that make the grid independently reproducible.
func ValidateRegisteredDataset(rawDir string, results []Result, dataset Dataset) error {
	if dataset.Schema != DatasetSchemaVersion {
		return fmt.Errorf("unsupported dataset schema %q", dataset.Schema)
	}
	if !dataset.Publishable {
		return fmt.Errorf("dataset is a non-publishable pilot")
	}
	if err := ValidateRegisteredGrid(results); err != nil {
		return err
	}
	if len(dataset.Runs) != len(results) {
		return fmt.Errorf("dataset ledger has %d runs, want %d", len(dataset.Runs), len(results))
	}
	entries, err := os.ReadDir(rawDir)
	if err != nil {
		return fmt.Errorf("read raw directory: %w", err)
	}
	if len(entries) != len(results) {
		return fmt.Errorf("raw directory has %d entries, want exactly %d result files", len(entries), len(results))
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json.gz") {
			return fmt.Errorf("raw directory contains unexpected entry %q", entry.Name())
		}
	}

	resultsByCell := make(map[string]Result, len(results))
	for _, result := range results {
		resultsByCell[cellKey(result.Manifest.Name, result.Variant.Name, result.Seed)] = result
	}
	seen := make(map[string]bool, len(dataset.Runs))
	var canonical *RunProvenance
	for i := range dataset.Runs {
		run := &dataset.Runs[i]
		key := cellKey(run.Manifest, run.Variant, run.Seed)
		if seen[key] {
			return fmt.Errorf("duplicate provenance record %s", key)
		}
		seen[key] = true
		result, ok := resultsByCell[key]
		if !ok {
			return fmt.Errorf("provenance has no result for %s", key)
		}
		if run.Status == RunStatusFailed {
			return fmt.Errorf("%s is a failed cell", key)
		}
		expectedStatus := RunStatusOK
		if run.Manifest == "worker-crash" && run.Variant == "asynq" {
			expectedStatus = RunStatusNotMeasured
		}
		if run.Status != expectedStatus {
			return fmt.Errorf("%s status is %q, want %q", key, run.Status, expectedStatus)
		}
		if err := validateRunProvenance(rawDir, *run, result); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		if canonical == nil {
			copy := *run
			canonical = &copy
		} else if err := consistentProvenance(*canonical, *run); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
	}
	return nil
}

func validateRunProvenance(rawDir string, run RunProvenance, result Result) error {
	if run.ResultSchema != SchemaVersion || result.Schema != run.ResultSchema {
		return fmt.Errorf("mixed result schema %q / %q", run.ResultSchema, result.Schema)
	}
	if !fullHex(run.SourceCommit, 40, 64) || !fullHex(run.SourceTree, 40, 64) {
		return fmt.Errorf("source commit or tree is not a full Git object ID")
	}
	if result.Environment.BuildSHA != run.SourceCommit {
		return fmt.Errorf("result source %q differs from ledger source %q", result.Environment.BuildSHA, run.SourceCommit)
	}
	if !fullHex(run.BinarySHA256, 64) || !fullHex(run.ResultSHA256, 64) {
		return fmt.Errorf("binary or result digest is not SHA-256")
	}
	if run.ResultFile != fmt.Sprintf("%s--%s--%d.json.gz", run.Manifest, run.Variant, run.Seed) || filepath.Base(run.ResultFile) != run.ResultFile {
		return fmt.Errorf("unsafe or inconsistent result filename %q", run.ResultFile)
	}
	digest, err := SHA256File(filepath.Join(rawDir, run.ResultFile))
	if err != nil {
		return err
	}
	if digest != run.ResultSHA256 {
		return fmt.Errorf("result digest mismatch: got %s, want %s", digest, run.ResultSHA256)
	}
	fileResult, err := loadRawResult(filepath.Join(rawDir, run.ResultFile))
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(fileResult, result) {
		return fmt.Errorf("result filename does not contain its registered cell")
	}
	if len(run.DependencyLocks) != 2 || run.DependencyLocks[0].Path != "go.mod" || run.DependencyLocks[1].Path != "go.sum" {
		return fmt.Errorf("dependency locks must identify go.mod and go.sum")
	}
	for _, lock := range run.DependencyLocks {
		if !fullHex(lock.SHA256, 64) {
			return fmt.Errorf("invalid %s digest", lock.Path)
		}
	}
	if !slices.Equal(run.BuildArguments, []string{"go", "build", "-trimpath", "-buildvcs=false", "-o", "<temporary>/experiment", "./cmd/experiment"}) {
		return fmt.Errorf("unexpected binary build arguments")
	}
	if !slices.Equal(run.RunnerArguments, ExpectedRunnerArguments(run.Manifest, run.Variant, run.Seed)) {
		return fmt.Errorf("runner arguments do not match the registered cell")
	}
	if result.Environment.Hostname != "research-host" {
		return fmt.Errorf("non-neutral hostname %q", result.Environment.Hostname)
	}
	if run.Redis.DB < 1 || (run.Redis.Address != "localhost:6379" && run.Redis.Address != "127.0.0.1:6379") {
		return fmt.Errorf("unsafe Redis configuration")
	}
	if result.Environment.RedisConfig != encodeRedisConfiguration(run.Redis) {
		return fmt.Errorf("redis configuration differs between result and ledger")
	}
	if run.Environment.OS != result.Environment.OS || run.Environment.Architecture != result.Environment.Architecture || run.Environment.GoVersion != result.Environment.GoVersion || run.Environment.CPUs != result.Environment.CPUs {
		return fmt.Errorf("sanitized environment differs from result")
	}
	resultJSON, _ := json.Marshal(result)
	if leak := privacyLeak(string(resultJSON)); leak != "" {
		return fmt.Errorf("privacy-leaking result contains %s", leak)
	}
	encoded, _ := json.Marshal(run)
	if leak := privacyLeak(string(encoded)); leak != "" {
		return fmt.Errorf("privacy-leaking provenance contains %s", leak)
	}
	return nil
}

func consistentProvenance(a, b RunProvenance) error {
	if a.SourceCommit != b.SourceCommit || a.SourceTree != b.SourceTree {
		return fmt.Errorf("mixed source revisions")
	}
	if a.BinarySHA256 != b.BinarySHA256 || !slices.Equal(a.BuildArguments, b.BuildArguments) {
		return fmt.Errorf("mixed experiment binaries")
	}
	if !slices.Equal(a.DependencyLocks, b.DependencyLocks) {
		return fmt.Errorf("mixed dependency locks")
	}
	aRedis, _ := json.Marshal(a.Redis)
	bRedis, _ := json.Marshal(b.Redis)
	if string(aRedis) != string(bRedis) {
		return fmt.Errorf("mixed Redis configuration")
	}
	if a.Environment != b.Environment {
		return fmt.Errorf("mixed sanitized environments")
	}
	return nil
}

func ExpectedRunnerArguments(manifest, variant string, seed int64) []string {
	return []string{
		"-seed", strconv.FormatInt(seed, 10),
		"-scale", "8",
		"-compact",
		"-hostname-label", "research-host",
		"-manifest", manifest,
		"-variant", variant,
		"-redis-addr", "localhost:6379",
		"-redis-db", "14",
		"-output", ".taskforge-experiment-output",
	}
}

func SHA256File(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func fullHex(value string, lengths ...int) bool {
	if !slices.Contains(lengths, len(value)) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func cellKey(manifest, variant string, seed int64) string {
	return fmt.Sprintf("%s/%s/%d", manifest, variant, seed)
}

func encodeRedisConfiguration(redis RedisConfiguration) string {
	values := map[string]string{"addr": redis.Address, "db": strconv.Itoa(redis.DB)}
	for key, value := range redis.Values {
		values[key] = value
	}
	data, _ := json.Marshal(values)
	return string(data)
}

var (
	homePathPattern = regexp.MustCompile(`(?i)(/home/[^/\" ]+|/users/[^/\" ]+|[a-z]:\\users\\[^\\\" ]+)`)
	emailPattern    = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)
)

func privacyLeak(value string) string {
	lower := strings.ToLower(value)
	for _, token := range []string{"\"user\":", "\"username\":", "\"home\":", "user=", "username=", "home="} {
		if strings.Contains(lower, token) {
			return token
		}
	}
	if homePathPattern.MatchString(value) {
		return "a home-directory path"
	}
	if emailPattern.MatchString(value) {
		return "an email-like identifier"
	}
	return ""
}
