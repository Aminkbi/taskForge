// experiment-study-traces freezes every registered external arrival trace and
// a byte-level lock before any system result exists.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aminkbi/taskforge/research/internal/expcli"
	"github.com/aminkbi/taskforge/research/internal/experiment"
)

type traceLock struct {
	Schema string      `json:"schema"`
	Files  []traceFile `json:"files"`
}

type traceFile struct {
	Path        string `json:"path"`
	Seed        int64  `json:"seed"`
	TraceDigest string `json:"trace_digest"`
	SHA256      string `json:"sha256"`
}

type codeLock struct {
	Schema string     `json:"schema"`
	Files  []codeFile `json:"files"`
}

type codeFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

func main() {
	root := flag.String("root", "second-wave", "frozen study directory")
	repoRoot := flag.String("repo-root", "..", "repository root walked for the code lock")
	flag.Parse()
	plan, err := experiment.LoadStudyPlan(filepath.Join(*root, "study-plan.json"))
	if err != nil {
		expcli.Fatal("load plan: %v", err)
	}
	traceDir := filepath.Join(*root, "traces")
	if _, err := os.Stat(traceDir); !os.IsNotExist(err) {
		expcli.Fatal("trace directory already exists; immutable corpus may not be replaced")
	}
	if err := os.MkdirAll(traceDir, 0755); err != nil {
		expcli.Fatal("create trace directory: %v", err)
	}
	lock := traceLock{Schema: "taskforge-paired-study-trace-lock/v1"}
	for _, registered := range plan.Profiles {
		data, err := os.ReadFile(filepath.Join(*root, "profiles", registered.File))
		if err != nil {
			expcli.Fatal("read profile %s: %v", registered.Name, err)
		}
		var profile experiment.OpenLoopProfile
		if err := json.Unmarshal(data, &profile); err != nil {
			expcli.Fatal("decode profile %s: %v", registered.Name, err)
		}
		if profile.Name != registered.Name {
			expcli.Fatal("profile identity %q differs from plan %q", profile.Name, registered.Name)
		}
		for _, seed := range registered.Seeds {
			trace, err := experiment.GenerateOpenLoopTrace(profile, seed)
			if err != nil {
				expcli.Fatal("generate %s/%d: %v", profile.Name, seed, err)
			}
			name := fmt.Sprintf("%s-%d.json", profile.Name, seed)
			path := filepath.Join(traceDir, name)
			if err := experiment.WriteOpenLoopTrace(path, trace); err != nil {
				expcli.Fatal("write %s: %v", name, err)
			}
			digest, err := sha256File(path)
			if err != nil {
				expcli.Fatal("digest %s: %v", name, err)
			}
			lock.Files = append(lock.Files, traceFile{Path: filepath.ToSlash(filepath.Join("traces", name)), Seed: seed, TraceDigest: trace.Digest, SHA256: digest})
		}
	}
	sort.Slice(lock.Files, func(i, j int) bool { return lock.Files[i].Path < lock.Files[j].Path })
	data, err := json.MarshalIndent(lock, "", "  ")
	if err != nil {
		expcli.Fatal("encode trace lock: %v", err)
	}
	lockPath := filepath.Join(*root, "trace-lock.json")
	file, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0444)
	if err != nil {
		expcli.Fatal("create trace lock: %v", err)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		expcli.Fatal("write trace lock: %v", err)
	}
	if err := file.Close(); err != nil {
		expcli.Fatal("close trace lock: %v", err)
	}
	fmt.Printf("frozen %d traces in %s\n", len(lock.Files), traceDir)
	writeCodeLock(*root, *repoRoot)
}

func writeCodeLock(studyRoot, repoRoot string) {
	lock := codeLock{Schema: "taskforge-paired-study-code-lock/v1"}
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		clean := filepath.ToSlash(relative)
		if entry.IsDir() {
			if clean != "." && (clean == ".git" || clean == "dist" || strings.HasPrefix(clean, "research/data") || strings.HasPrefix(clean, "research/second-wave/data") || strings.HasPrefix(clean, "research/second-wave/results") || strings.HasPrefix(clean, "research/second-wave/figures")) {
				return filepath.SkipDir
			}
			return nil
		}
		base := filepath.Base(clean)
		include := strings.HasSuffix(clean, ".go") || base == "go.mod" || base == "go.sum" || clean == "Makefile" || strings.HasPrefix(clean, "scripts/second-wave-") || strings.HasPrefix(clean, "research/second-wave/")
		if !include || clean == "research/second-wave/code-lock.json" || clean == "research/second-wave/trace-lock.json" || strings.HasPrefix(clean, "docs/roadmap/31-") {
			return nil
		}
		digest, err := sha256File(path)
		if err != nil {
			return err
		}
		lock.Files = append(lock.Files, codeFile{Path: clean, SHA256: digest})
		return nil
	})
	if err != nil {
		expcli.Fatal("build code lock: %v", err)
	}
	sort.Slice(lock.Files, func(i, j int) bool { return lock.Files[i].Path < lock.Files[j].Path })
	data, err := json.MarshalIndent(lock, "", "  ")
	if err != nil {
		expcli.Fatal("encode code lock: %v", err)
	}
	path := filepath.Join(studyRoot, "code-lock.json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0444)
	if err != nil {
		expcli.Fatal("create code lock: %v", err)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		expcli.Fatal("write code lock: %v", err)
	}
	if err := file.Close(); err != nil {
		expcli.Fatal("close code lock: %v", err)
	}
}

func sha256File(path string) (string, error) {
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
