// taskforge-experiment-grid executes the registered research grid as one
// atomic artifact operation. Publication runs require a completely clean Git
// checkout; exploratory runs require the explicit -pilot flag and are marked
// non-publishable in their ledger.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/aminkbi/taskforge/internal/experiment"
)

const cellOutputDir = ".taskforge-experiment-output"

type options struct {
	output    string
	pilot     bool
	redisAddr string
	redisDB   int
	scale     int
	seeds     []int64
}

type source struct {
	root   string
	commit string
	tree   string
	dirty  bool
}

func main() {
	output := flag.String("output", "research/data", "complete dataset directory (contains dataset.json, run-log.txt, and raw/)")
	pilot := flag.Bool("pilot", false, "allow dirty or reduced exploratory input and mark it non-publishable")
	redisAddr := flag.String("redis-addr", "localhost:6379", "dedicated Redis address")
	redisDB := flag.Int("redis-db", 14, "dedicated non-production Redis DB")
	scale := flag.Int("scale", 8, "workload scale (registered value: 8)")
	seedText := flag.String("seeds", "", "comma-separated pilot seeds; publication runs always use the registered seeds")
	flag.Parse()

	seeds, err := selectedSeeds(*pilot, *seedText)
	if err != nil {
		fatal("seeds: %v", err)
	}
	opts := options{output: *output, pilot: *pilot, redisAddr: *redisAddr, redisDB: *redisDB, scale: *scale, seeds: seeds}
	if err := run(opts); err != nil {
		fatal("research grid: %v", err)
	}
}

func run(opts options) error {
	if opts.redisDB < 1 {
		return fmt.Errorf("refusing Redis DB %d; use a dedicated non-zero DB", opts.redisDB)
	}
	if opts.scale < 1 {
		return fmt.Errorf("scale must be positive")
	}
	if !opts.pilot && (opts.scale != 8 || opts.redisAddr != "localhost:6379" || opts.redisDB != 14 || !slicesEqual(opts.seeds, experiment.RegisteredSeeds())) {
		return fmt.Errorf("publication runs require the registered scale, seeds, and local Redis configuration; use -pilot for exploratory runs")
	}
	if opts.pilot && filepath.Clean(opts.output) == filepath.Clean("research/data") {
		return fmt.Errorf("pilot output must not replace research/data")
	}

	src, err := inspectSource(opts.pilot)
	if err != nil {
		return err
	}
	cleanupCellOutput := filepath.Join(src.root, cellOutputDir)
	if err := os.RemoveAll(cleanupCellOutput); err != nil {
		return fmt.Errorf("clear cell staging: %w", err)
	}
	defer os.RemoveAll(cleanupCellOutput)

	buildDir, err := os.MkdirTemp("", "taskforge-research-build-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(buildDir)
	binary := filepath.Join(buildDir, "experiment")
	buildArgs := []string{"build", "-trimpath", "-buildvcs=false", "-o", binary, "./cmd/experiment"}
	build := exec.Command("go", buildArgs...)
	build.Dir = src.root
	build.Env = setEnv(os.Environ(), "CGO_ENABLED", "0")
	if output, err := build.CombinedOutput(); err != nil {
		return fmt.Errorf("build measured binary: %w: %s", err, sanitizeDiagnostic(output))
	}
	binaryDigest, err := experiment.SHA256File(binary)
	if err != nil {
		return err
	}
	locks, err := dependencyLocks(src.root)
	if err != nil {
		return err
	}

	target, err := filepath.Abs(opts.output)
	if err != nil {
		return err
	}
	parent := filepath.Dir(target)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(parent, ".taskforge-research-data-")
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(stage)
		}
	}()
	rawDir := filepath.Join(stage, "raw")
	if err := os.MkdirAll(rawDir, 0755); err != nil {
		return err
	}

	dataset := experiment.Dataset{Schema: experiment.DatasetSchemaVersion, Publishable: !opts.pilot}
	var log bytes.Buffer
	for _, seed := range opts.seeds {
		for _, manifest := range experiment.RegisteredManifests() {
			for _, variant := range experiment.Variants() {
				run, err := executeCell(src, binary, binaryDigest, locks, opts, manifest, variant.Name, seed, rawDir)
				if err != nil {
					fmt.Fprintf(&log, "FAILED %s %s %d\n", manifest, variant.Name, seed)
					_ = os.WriteFile(filepath.Join(stage, "run-log.txt"), log.Bytes(), 0644)
					return fmt.Errorf("cell %s/%s/%d failed: %w", manifest, variant.Name, seed, err)
				}
				dataset.Runs = append(dataset.Runs, run)
				fmt.Fprintf(&log, "%s %s %s %d\n", run.Status, manifest, variant.Name, seed)
			}
		}
	}
	if err := writeJSON(filepath.Join(stage, "dataset.json"), dataset); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(stage, "run-log.txt"), log.Bytes(), 0644); err != nil {
		return err
	}
	if !opts.pilot {
		results, err := experiment.LoadRawResults(rawDir)
		if err != nil {
			return err
		}
		if err := experiment.ValidateRegisteredDataset(rawDir, results, dataset); err != nil {
			return fmt.Errorf("validate staged dataset: %w", err)
		}
	}
	if err := publishDataset(stage, target); err != nil {
		return err
	}
	published = true
	fmt.Printf("wrote %d cells to %s (publishable=%t, source=%s, binary_sha256=%s)\n", len(dataset.Runs), opts.output, dataset.Publishable, src.commit, binaryDigest)
	return nil
}

func executeCell(src source, binary, binaryDigest string, locks []experiment.FileDigest, opts options, manifest, variant string, seed int64, rawDir string) (experiment.RunProvenance, error) {
	if err := os.RemoveAll(filepath.Join(src.root, cellOutputDir)); err != nil {
		return experiment.RunProvenance{}, err
	}
	args := runnerArguments(manifest, variant, seed, opts.scale, opts.redisAddr, opts.redisDB)
	command := exec.Command(binary, args...)
	command.Dir = src.root
	command.Env = setEnv(os.Environ(), "TASKFORGE_BUILD_SHA", src.commit)
	output, err := command.CombinedOutput()
	if err != nil {
		return experiment.RunProvenance{}, fmt.Errorf("experiment process: %w: %s", err, sanitizeDiagnostic(output))
	}
	name := fmt.Sprintf("%s--%s--%d.json", manifest, variant, seed)
	plainPath := filepath.Join(src.root, cellOutputDir, name)
	data, err := os.ReadFile(plainPath)
	if err != nil {
		return experiment.RunProvenance{}, fmt.Errorf("read cell result: %w", err)
	}
	var result experiment.Result
	if err := json.Unmarshal(data, &result); err != nil {
		return experiment.RunProvenance{}, fmt.Errorf("decode cell result: %w", err)
	}
	if result.Schema != experiment.SchemaVersion || result.Manifest.Name != manifest || result.Variant.Name != variant || result.Seed != seed || result.Environment.BuildSHA != src.commit {
		return experiment.RunProvenance{}, fmt.Errorf("cell result identity or source mismatch")
	}
	gzipName := strings.TrimSuffix(name, ".json") + ".json.gz"
	gzipPath := filepath.Join(rawDir, gzipName)
	if err := writeDeterministicGzip(gzipPath, data); err != nil {
		return experiment.RunProvenance{}, err
	}
	resultDigest, err := experiment.SHA256File(gzipPath)
	if err != nil {
		return experiment.RunProvenance{}, err
	}
	redis, err := parseRedisConfiguration(result.Environment.RedisConfig)
	if err != nil {
		return experiment.RunProvenance{}, err
	}
	status := experiment.RunStatusOK
	if manifest == "worker-crash" && variant == "asynq" {
		status = experiment.RunStatusNotMeasured
	}
	return experiment.RunProvenance{
		Manifest: manifest, Variant: variant, Seed: seed, Status: status,
		ResultFile: gzipName, ResultSHA256: resultDigest, ResultSchema: result.Schema,
		SourceCommit: src.commit, SourceTree: src.tree, BinarySHA256: binaryDigest,
		BuildArguments:  []string{"go", "build", "-trimpath", "-buildvcs=false", "-o", "<temporary>/experiment", "./cmd/experiment"},
		DependencyLocks: locks,
		RunnerArguments: args,
		Redis:           redis,
		Environment: experiment.SanitizedEnvironment{
			OS: result.Environment.OS, Architecture: result.Environment.Architecture,
			GoVersion: result.Environment.GoVersion, CPUs: result.Environment.CPUs,
			CGOEnabled: false, GOMAXPROCS: runtime.GOMAXPROCS(0),
		},
	}, nil
}

func inspectSource(pilot bool) (source, error) {
	root, err := gitOutput("rev-parse", "--show-toplevel")
	if err != nil {
		return source{}, fmt.Errorf("locate source checkout: %w", err)
	}
	status, err := gitOutputAt(root, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return source{}, fmt.Errorf("inspect source checkout: %w", err)
	}
	dirty := strings.TrimSpace(status) != ""
	if err := validateSourceStatus(status, pilot); err != nil {
		return source{}, err
	}
	commit, err := gitOutputAt(root, "rev-parse", "HEAD")
	if err != nil {
		return source{}, err
	}
	tree, err := gitOutputAt(root, "rev-parse", "HEAD^{tree}")
	if err != nil {
		return source{}, err
	}
	return source{root: root, commit: commit, tree: tree, dirty: dirty}, nil
}

func validateSourceStatus(status string, pilot bool) error {
	if strings.TrimSpace(status) != "" && !pilot {
		return fmt.Errorf("source checkout is dirty; commit/stash all tracked and untracked files or use -pilot for explicitly non-publishable output")
	}
	return nil
}

func selectedSeeds(pilot bool, text string) ([]int64, error) {
	if text == "" {
		return experiment.RegisteredSeeds(), nil
	}
	if !pilot {
		return nil, fmt.Errorf("custom seeds require -pilot")
	}
	var seeds []int64
	for _, part := range strings.Split(text, ",") {
		seed, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil {
			return nil, err
		}
		seeds = append(seeds, seed)
	}
	if len(seeds) == 0 {
		return nil, errors.New("at least one seed is required")
	}
	return seeds, nil
}

func runnerArguments(manifest, variant string, seed int64, scale int, addr string, db int) []string {
	return []string{
		"-seed", strconv.FormatInt(seed, 10), "-scale", strconv.Itoa(scale), "-compact",
		"-hostname-label", "research-host", "-manifest", manifest, "-variant", variant,
		"-redis-addr", addr, "-redis-db", strconv.Itoa(db), "-output", cellOutputDir,
	}
}

func dependencyLocks(root string) ([]experiment.FileDigest, error) {
	locks := make([]experiment.FileDigest, 0, 2)
	for _, name := range []string{"go.mod", "go.sum"} {
		digest, err := experiment.SHA256File(filepath.Join(root, name))
		if err != nil {
			return nil, err
		}
		locks = append(locks, experiment.FileDigest{Path: name, SHA256: digest})
	}
	return locks, nil
}

func parseRedisConfiguration(encoded string) (experiment.RedisConfiguration, error) {
	var values map[string]string
	if err := json.Unmarshal([]byte(encoded), &values); err != nil {
		return experiment.RedisConfiguration{}, fmt.Errorf("decode Redis configuration: %w", err)
	}
	db, err := strconv.Atoi(values["db"])
	if err != nil {
		return experiment.RedisConfiguration{}, fmt.Errorf("decode Redis DB: %w", err)
	}
	redis := experiment.RedisConfiguration{Address: values["addr"], DB: db, Values: map[string]string{}}
	delete(values, "addr")
	delete(values, "db")
	for key, value := range values {
		redis.Values[key] = value
	}
	return redis, nil
}

func writeDeterministicGzip(path string, data []byte) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	writer, err := gzip.NewWriterLevel(file, gzip.BestCompression)
	if err != nil {
		_ = file.Close()
		return err
	}
	writer.Header.ModTime = time.Unix(0, 0)
	writer.Header.OS = 255
	_, writeErr := writer.Write(data)
	closeErr := writer.Close()
	fileErr := file.Close()
	return errors.Join(writeErr, closeErr, fileErr)
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

func publishDataset(stage, target string) error {
	backup := target + ".previous"
	if err := os.RemoveAll(backup); err != nil {
		return err
	}
	targetExists := false
	if _, err := os.Stat(target); err == nil {
		targetExists = true
		if err := os.Rename(target, backup); err != nil {
			return fmt.Errorf("preserve previous dataset: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(stage, target); err != nil {
		if targetExists {
			_ = os.Rename(backup, target)
		}
		return fmt.Errorf("publish replacement dataset: %w", err)
	}
	if targetExists {
		if err := os.RemoveAll(backup); err != nil {
			return fmt.Errorf("remove superseded dataset: %w", err)
		}
	}
	return nil
}

func setEnv(environment []string, key, value string) []string {
	prefix := key + "="
	result := make([]string, 0, len(environment)+1)
	for _, item := range environment {
		if !strings.HasPrefix(item, prefix) {
			result = append(result, item)
		}
	}
	return append(result, prefix+value)
}

func gitOutput(args ...string) (string, error) { return gitOutputAt("", args...) }

func gitOutputAt(dir string, args ...string) (string, error) {
	command := exec.Command("git", args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	return strings.TrimSpace(string(output)), err
}

func sanitizeDiagnostic(value []byte) string {
	if len(value) > 4000 {
		value = value[:4000]
	}
	return strings.TrimSpace(string(bytes.ReplaceAll(value, []byte("\n"), []byte(" "))))
}

func slicesEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
