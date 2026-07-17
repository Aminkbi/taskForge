package certification_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type manifest struct {
	Schema        string       `json:"$schema"`
	SchemaVersion string       `json:"schema_version"`
	Contract      string       `json:"contract"`
	Checks        []check      `json:"checks"`
	Claims        []claim      `json:"claims"`
	Assumptions   []assumption `json:"assumptions"`
	Artifacts     []artifact   `json:"artifacts"`
}

type check struct {
	ID            string   `json:"id"`
	Command       string   `json:"command"`
	Required      *bool    `json:"required"`
	Scope         string   `json:"scope"`
	Prerequisites []string `json:"prerequisites"`
	Evidence      []string `json:"evidence"`
}

type claim struct {
	ID        string   `json:"id"`
	Statement string   `json:"statement"`
	Checks    []string `json:"checks"`
	Sources   []string `json:"sources"`
}

type assumption struct {
	ID           string `json:"id"`
	Statement    string `json:"statement"`
	DocumentedAt string `json:"documented_at"`
}

type artifact struct {
	ID          string   `json:"id"`
	Path        string   `json:"path"`
	Kind        string   `json:"kind"`
	Description string   `json:"description"`
	ProducedBy  []string `json:"produced_by"`
}

func TestManifestIsInternallyConsistent(t *testing.T) {
	repoRoot := filepath.Clean("..")
	data, err := os.ReadFile("manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var got manifest
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&got); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if got.Schema != "manifest.schema.json" || got.SchemaVersion != "taskforge-certification-manifest/v2" {
		t.Fatalf("unexpected schema identity: path=%q version=%q", got.Schema, got.SchemaVersion)
	}
	assertJSONFile(t, got.Schema)
	assertRepoPath(t, repoRoot, got.Contract)

	phony := makeTargets(t, filepath.Join(repoRoot, "Makefile"))
	checks := make(map[string]struct{}, len(got.Checks))
	for _, item := range got.Checks {
		assertUniqueID(t, "check", item.ID, checks)
		if item.Required == nil || item.Scope == "" || len(item.Evidence) == 0 {
			t.Errorf("check %q needs required state, scope, and evidence", item.ID)
		}
		fields := strings.Fields(item.Command)
		if len(fields) != 2 || fields[0] != "make" {
			t.Errorf("check %q command %q is not one documented Make target", item.ID, item.Command)
			continue
		}
		if _, ok := phony[fields[1]]; !ok {
			t.Errorf("check %q references unknown Make target %q", item.ID, fields[1])
		}
		for _, path := range item.Evidence {
			assertRepoPath(t, repoRoot, path)
		}
	}

	claims := make(map[string]struct{}, len(got.Claims))
	for _, item := range got.Claims {
		assertUniqueID(t, "claim", item.ID, claims)
		if item.Statement == "" || len(item.Checks) == 0 || len(item.Sources) == 0 {
			t.Errorf("claim %q must have a statement, executable checks, and sources", item.ID)
		}
		for _, checkID := range item.Checks {
			if _, ok := checks[checkID]; !ok {
				t.Errorf("claim %q references unknown check %q", item.ID, checkID)
			}
		}
		for _, path := range item.Sources {
			assertRepoPath(t, repoRoot, path)
		}
	}

	assumptions := make(map[string]struct{}, len(got.Assumptions))
	for _, item := range got.Assumptions {
		assertUniqueID(t, "assumption", item.ID, assumptions)
		if item.Statement == "" || item.DocumentedAt == "" {
			t.Errorf("assumption %q must have a statement and documentation link", item.ID)
			continue
		}
		assertRepoPath(t, repoRoot, strings.Split(item.DocumentedAt, "#")[0])
	}

	artifacts := make(map[string]struct{}, len(got.Artifacts))
	for _, item := range got.Artifacts {
		assertUniqueID(t, "artifact", item.ID, artifacts)
		if item.Description == "" || (item.Kind != "committed" && item.Kind != "generated") {
			t.Errorf("artifact %q has invalid description or kind %q", item.ID, item.Kind)
		}
		if item.Kind == "committed" {
			assertRepoPath(t, repoRoot, item.Path)
		}
		for _, checkID := range item.ProducedBy {
			if _, ok := checks[checkID]; !ok {
				t.Errorf("artifact %q references unknown producer %q", item.ID, checkID)
			}
		}
	}
}

func assertJSONFile(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Errorf("read JSON file %q: %v", path, err)
		return
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Errorf("parse JSON file %q: %v", path, err)
	}
}

func assertRepoPath(t *testing.T, repoRoot, path string) {
	t.Helper()
	if path == "" || filepath.IsAbs(path) || strings.Contains(path, "..") {
		t.Errorf("invalid repository-relative path %q", path)
		return
	}
	if _, err := os.Stat(filepath.Join(repoRoot, filepath.FromSlash(path))); err != nil {
		t.Errorf("repository path %q: %v", path, err)
	}
}

func assertUniqueID(t *testing.T, kind, id string, seen map[string]struct{}) {
	t.Helper()
	if id == "" {
		t.Errorf("%s has an empty id", kind)
		return
	}
	if _, exists := seen[id]; exists {
		t.Errorf("duplicate %s id %q", kind, id)
	}
	seen[id] = struct{}{}
}

func makeTargets(t *testing.T, path string) map[string]struct{} {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	targets := make(map[string]struct{})
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, ".PHONY:") {
			continue
		}
		for _, target := range strings.Fields(strings.TrimPrefix(line, ".PHONY:")) {
			targets[target] = struct{}{}
		}
	}
	return targets
}
