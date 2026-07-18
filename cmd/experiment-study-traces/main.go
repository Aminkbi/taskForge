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

	"github.com/aminkbi/taskforge/internal/experiment"
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
	root := flag.String("root", "research/second-wave", "frozen study directory")
	flag.Parse()
	plan, err := experiment.LoadStudyPlan(filepath.Join(*root, "study-plan.json"))
	if err != nil {
		fatal("load plan: %v", err)
	}
	traceDir := filepath.Join(*root, "traces")
	if _, err := os.Stat(traceDir); !os.IsNotExist(err) {
		fatal("trace directory already exists; immutable corpus may not be replaced")
	}
	if err := os.MkdirAll(traceDir, 0755); err != nil {
		fatal("create trace directory: %v", err)
	}
	lock := traceLock{Schema: "taskforge-paired-study-trace-lock/v1"}
	for _, registered := range plan.Profiles {
		data, err := os.ReadFile(filepath.Join(*root, "profiles", registered.File))
		if err != nil {
			fatal("read profile %s: %v", registered.Name, err)
		}
		var profile experiment.OpenLoopProfile
		if err := json.Unmarshal(data, &profile); err != nil {
			fatal("decode profile %s: %v", registered.Name, err)
		}
		if profile.Name != registered.Name {
			fatal("profile identity %q differs from plan %q", profile.Name, registered.Name)
		}
		for _, seed := range registered.Seeds {
			trace, err := experiment.GenerateOpenLoopTrace(profile, seed)
			if err != nil {
				fatal("generate %s/%d: %v", profile.Name, seed, err)
			}
			name := fmt.Sprintf("%s-%d.json", profile.Name, seed)
			path := filepath.Join(traceDir, name)
			if err := experiment.WriteOpenLoopTrace(path, trace); err != nil {
				fatal("write %s: %v", name, err)
			}
			digest, err := sha256File(path)
			if err != nil {
				fatal("digest %s: %v", name, err)
			}
			lock.Files = append(lock.Files, traceFile{Path: filepath.ToSlash(filepath.Join("traces", name)), Seed: seed, TraceDigest: trace.Digest, SHA256: digest})
		}
	}
	sort.Slice(lock.Files, func(i, j int) bool { return lock.Files[i].Path < lock.Files[j].Path })
	data, err := json.MarshalIndent(lock, "", "  ")
	if err != nil {
		fatal("encode trace lock: %v", err)
	}
	lockPath := filepath.Join(*root, "trace-lock.json")
	file, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0444)
	if err != nil {
		fatal("create trace lock: %v", err)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		fatal("write trace lock: %v", err)
	}
	if err := file.Close(); err != nil {
		fatal("close trace lock: %v", err)
	}
	fmt.Printf("frozen %d traces in %s\n", len(lock.Files), traceDir)
	writeCodeLock(*root)
}

func writeCodeLock(root string) {
	lock := codeLock{Schema: "taskforge-paired-study-code-lock/v1"}
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		clean := filepath.ToSlash(strings.TrimPrefix(path, "./"))
		if entry.IsDir() {
			if clean == ".git" || clean == "dist" || strings.HasPrefix(clean, "research/data") || strings.HasPrefix(clean, "research/second-wave/data") || strings.HasPrefix(clean, "research/second-wave/results") || strings.HasPrefix(clean, "research/second-wave/figures") {
				return filepath.SkipDir
			}
			return nil
		}
		include := strings.HasSuffix(clean, ".go") || clean == "go.mod" || clean == "go.sum" || clean == "Makefile" || strings.HasPrefix(clean, "scripts/second-wave-") || strings.HasPrefix(clean, "research/second-wave/")
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
		fatal("build code lock: %v", err)
	}
	sort.Slice(lock.Files, func(i, j int) bool { return lock.Files[i].Path < lock.Files[j].Path })
	data, err := json.MarshalIndent(lock, "", "  ")
	if err != nil {
		fatal("encode code lock: %v", err)
	}
	path := filepath.Join(root, "code-lock.json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0444)
	if err != nil {
		fatal("create code lock: %v", err)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		fatal("write code lock: %v", err)
	}
	if err := file.Close(); err != nil {
		fatal("close code lock: %v", err)
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

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
