// Command generate-sbom emits a deterministic SPDX 2.3 inventory of the Go
// modules linked into TaskForge release binaries.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
)

type module struct {
	Path    string
	Version string
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

	cmd := exec.Command("go", "list", "-m", "-json", "all")
	cmd.Stderr = os.Stderr
	data, err := cmd.Output()
	if err != nil {
		fmt.Fprintln(os.Stderr, "list modules:", err)
		os.Exit(1)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var modules []module
	for decoder.More() {
		var m module
		if err := decoder.Decode(&m); err != nil {
			fmt.Fprintln(os.Stderr, "decode modules:", err)
			os.Exit(1)
		}
		modules = append(modules, m)
	}
	sort.Slice(modules, func(i, j int) bool { return modules[i].Path < modules[j].Path })
	packages := make([]pkg, 0, len(modules))
	for i, m := range modules {
		location := "NOASSERTION"
		if m.Version != "" {
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
