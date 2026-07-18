// experiment-study-register builds the complete immutable-cell ledger. Missing
// and unsupported cells remain visible; no result is selected or dropped.
package main

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aminkbi/taskforge/internal/experiment"
)

func main() {
	root := flag.String("root", "research/second-wave", "frozen study directory")
	dataDir := flag.String("data", "research/second-wave/data", "staged study data directory")
	binary := flag.String("binary", "", "exact measured replay binary")
	sourceCommit := flag.String("source-commit", "", "immutable parent source commit")
	flag.Parse()
	if *binary == "" || *sourceCommit == "" {
		fatal("-binary and -source-commit are required")
	}
	planPath := filepath.Join(*root, "study-plan.json")
	plan, err := experiment.LoadStudyPlan(planPath)
	if err != nil {
		fatal("load plan: %v", err)
	}
	planDigest := mustDigest(planPath)
	lockDigest := mustDigest(filepath.Join(*root, "trace-lock.json"))
	dataset := experiment.StudyDataset{
		Schema: experiment.StudyDatasetSchema, PlanSHA256: planDigest, TraceLockSHA: lockDigest,
		BinarySHA256: mustDigest(*binary), SourceParent: *sourceCommit,
	}
	log := make([]byte, 0, 4096)
	for _, environment := range plan.Environments {
		for _, profile := range plan.Profiles {
			for _, seed := range profile.Seeds {
				tracePath := filepath.Join(*root, "traces", fmt.Sprintf("%s-%d.json", profile.Name, seed))
				trace, err := experiment.LoadOpenLoopTrace(tracePath)
				if err != nil {
					fatal("load trace: %v", err)
				}
				for repetition := range profile.Repetitions {
					for _, system := range profile.Systems {
						relative := filepath.ToSlash(filepath.Join("raw", environment.Name, profile.Name, fmt.Sprintf("%d", seed), fmt.Sprintf("r%d", repetition), fmt.Sprintf("%s--%s--r%d.json.gz", trace.ID, system, repetition)))
						run := experiment.StudyRun{Environment: environment.Name, Profile: profile.Name, TraceID: trace.ID, TraceSHA256: trace.Digest, Seed: seed, Repetition: repetition, System: system}
						result, err := loadResult(filepath.Join(*dataDir, filepath.FromSlash(relative)))
						if err != nil {
							run.Status = experiment.RunStatusFailed
							run.Failure = "registered runner produced no readable result"
						} else if result.System != system || result.TraceID != trace.ID || result.TraceSHA256 != trace.Digest {
							run.Status = experiment.RunStatusFailed
							run.Failure = "result identity did not match its registered cell"
						} else {
							run.SystemOrder = result.SystemOrder
							run.ResultFile = relative
							run.ResultSHA256 = mustDigest(filepath.Join(*dataDir, filepath.FromSlash(relative)))
							if result.Excluded {
								run.Status = experiment.RunStatusNotMeasured
								run.Failure = result.ExcludeReason
							} else {
								run.Status = experiment.RunStatusOK
							}
						}
						dataset.Runs = append(dataset.Runs, run)
						log = fmt.Appendf(log, "%s %s %s %d %d %s\n", run.Status, run.Environment, run.Profile, run.Seed, run.Repetition, run.System)
					}
				}
			}
		}
	}
	data, err := json.MarshalIndent(dataset, "", "  ")
	if err != nil {
		fatal("encode dataset: %v", err)
	}
	if err := os.WriteFile(filepath.Join(*dataDir, "dataset.json"), append(data, '\n'), 0644); err != nil {
		fatal("write dataset: %v", err)
	}
	if err := os.WriteFile(filepath.Join(*dataDir, "run-log.txt"), log, 0644); err != nil {
		fatal("write run log: %v", err)
	}
	if err := experiment.ValidateStudyDataset(plan, dataset, *dataDir, filepath.Join(*root, "traces")); err != nil {
		fatal("validate dataset: %v", err)
	}
	fmt.Printf("registered %d study cells\n", len(dataset.Runs))
}

func loadResult(path string) (experiment.OpenLoopResult, error) {
	file, err := os.Open(path)
	if err != nil {
		return experiment.OpenLoopResult{}, err
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return experiment.OpenLoopResult{}, err
	}
	defer gz.Close()
	var result experiment.OpenLoopResult
	if err := json.NewDecoder(gz).Decode(&result); err != nil {
		return result, err
	}
	return result, nil
}

func mustDigest(path string) string {
	file, err := os.Open(path)
	if err != nil {
		fatal("open %s: %v", path, err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		fatal("digest %s: %v", path, err)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
