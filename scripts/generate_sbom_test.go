package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLinkedModulesReadsBinaryBuildMetadata(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	modules, err := linkedModules([]string{executable, executable}, "v1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	if len(modules) == 0 {
		t.Fatal("linkedModules returned no modules")
	}

	foundTaskForge := false
	for index, linked := range modules {
		if index > 0 && modules[index-1].Path >= linked.Path {
			t.Fatalf("modules are not uniquely sorted: %q then %q", modules[index-1].Path, linked.Path)
		}
		if linked.Path == "github.com/aminkbi/taskforge" {
			foundTaskForge = true
			if linked.Version != "v1.2.3" {
				t.Fatalf("TaskForge version = %q, want v1.2.3", linked.Version)
			}
			if linked.Downloadable {
				t.Fatal("TaskForge release source must not claim a module-proxy download")
			}
		}
	}
	if !foundTaskForge {
		t.Fatalf("linked modules do not include TaskForge: %#v", modules)
	}
}

func TestLinkedModulesRejectsNonBinary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-binary")
	if err := os.WriteFile(path, []byte("not a Go binary"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := linkedModules([]string{path}, "dev"); err == nil {
		t.Fatal("linkedModules accepted a non-binary file")
	}
}
