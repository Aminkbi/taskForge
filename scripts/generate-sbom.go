// Command generate-sbom emits a deterministic SPDX 2.3 inventory of the Go
// modules linked into TaskForge release binaries.
package main

import (
	"debug/buildinfo"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
)

type module struct {
	Path         string
	Version      string
	Downloadable bool
}

type pkg struct {
	SPDXID           string `json:"SPDXID"`
	Name             string `json:"name"`
	VersionInfo      string `json:"versionInfo,omitempty"`
	DownloadLocation string `json:"downloadLocation"`
}

func main() {
	output := flag.String("output", "", "output SPDX JSON file")
	version := flag.String("version", "dev", "release version")
	commit := flag.String("commit", "unknown", "source revision")
	flag.Parse()
	if *output == "" {
		fmt.Fprintln(os.Stderr, "--output is required")
		os.Exit(2)
	}
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "at least one release binary is required")
		os.Exit(2)
	}

	modules, err := linkedModules(flag.Args(), *version)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read linked modules:", err)
		os.Exit(1)
	}
	packages := make([]pkg, 0, len(modules))
	for i, m := range modules {
		location := "NOASSERTION"
		if m.Downloadable {
			location = "https://proxy.golang.org/" + m.Path + "/@v/" + m.Version + ".zip"
		}
		packages = append(packages, pkg{fmt.Sprintf("SPDXRef-Package-%d", i), m.Path, m.Version, location})
	}
	document := map[string]any{
		"spdxVersion":       "SPDX-2.3",
		"dataLicense":       "CC0-1.0",
		"SPDXID":            "SPDXRef-DOCUMENT",
		"name":              "taskforge-" + *version,
		"documentNamespace": "https://taskforge.dev/spdx/" + *version + "/" + *commit,
		"creationInfo": map[string]any{
			"creators": []string{"Tool: taskforge/scripts/generate-sbom.go"},
		},
		"packages": packages,
	}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(*output, append(encoded, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write SBOM:", err)
		os.Exit(1)
	}
}

func linkedModules(paths []string, releaseVersion string) ([]module, error) {
	unique := make(map[string]module)
	for _, path := range paths {
		info, err := buildinfo.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if info.Main.Path != "" {
			unique[info.Main.Path] = module{Path: info.Main.Path, Version: releaseVersion}
		}
		for _, dependency := range info.Deps {
			linked := dependency
			if dependency.Replace != nil {
				linked = dependency.Replace
			}
			unique[linked.Path] = module{
				Path:         linked.Path,
				Version:      linked.Version,
				Downloadable: linked.Version != "",
			}
		}
	}

	modules := make([]module, 0, len(unique))
	for _, linked := range unique {
		modules = append(modules, linked)
	}
	sort.Slice(modules, func(i, j int) bool { return modules[i].Path < modules[j].Path })
	return modules, nil
}
