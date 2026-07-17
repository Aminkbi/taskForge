// taskforge-experiment-report derives compact JSON and SVG reports from raw
// experiment JSON. It never changes the raw evidence it reads.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"sort"

	"github.com/aminkbi/taskforge/internal/experiment"
)

type row struct {
	Manifest      string  `json:"manifest"`
	Variant       string  `json:"variant"`
	Throughput    float64 `json:"throughput_per_second"`
	P95Completion string  `json:"p95_completion"`
	Fairness      float64 `json:"jain_fairness"`
	SLOViolations int     `json:"slo_violations"`
}

func main() {
	input := flag.String("input", "artifacts/experiments/raw", "raw result directory")
	output := flag.String("output", "artifacts/experiments/reports", "derived report directory")
	flag.Parse()
	paths, err := filepath.Glob(filepath.Join(*input, "*.json"))
	if err != nil || len(paths) == 0 {
		fatal("no raw results in %s", *input)
	}
	rows := make([]row, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			fatal("read %s: %v", path, err)
		}
		var result experiment.Result
		if err := json.Unmarshal(data, &result); err != nil {
			fatal("decode %s: %v", path, err)
		}
		rows = append(rows, row{result.Manifest.Name, result.Variant.Name, result.Summary.Throughput, result.Summary.Completion.P95.String(), result.Summary.JainFairness, result.Summary.StarvationViolations})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Manifest == rows[j].Manifest {
			return rows[i].Variant < rows[j].Variant
		}
		return rows[i].Manifest < rows[j].Manifest
	})
	if err := os.MkdirAll(*output, 0755); err != nil {
		fatal("mkdir: %v", err)
	}
	data, _ := json.MarshalIndent(rows, "", "  ")
	if err := os.WriteFile(filepath.Join(*output, "summary.json"), append(data, '\n'), 0644); err != nil {
		fatal("write summary: %v", err)
	}
	if err := os.WriteFile(filepath.Join(*output, "throughput.svg"), svg(rows), 0644); err != nil {
		fatal("write SVG: %v", err)
	}
	fmt.Printf("%s\n", filepath.Join(*output, "summary.json"))
}
func svg(rows []row) []byte {
	const w = 1200
	const h = 80
	max := 0.0
	for _, row := range rows {
		if row.Throughput > max {
			max = row.Throughput
		}
	}
	if max == 0 {
		max = 1
	}
	body := ""
	for i, row := range rows {
		x := 20 + (i%10)*118
		y := 20 + (i/10)*h
		height := int(45 * row.Throughput / max)
		body += fmt.Sprintf(`<g transform="translate(%d,%d)"><rect x="0" y="%d" width="90" height="%d" fill="#2563eb"/><text x="0" y="60" font-size="9">%s</text><title>%s / %s: %.2f tasks/s</title></g>`, x, y, 45-height, height, html.EscapeString(row.Variant), html.EscapeString(row.Manifest), html.EscapeString(row.Variant), row.Throughput)
	}
	return []byte(fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d"><style>text{font-family:monospace}</style><text x="20" y="14">Throughput (tasks/s); derived from raw experiment JSON, not a superiority claim</text>%s</svg>`, w, 40+((len(rows)+9)/10)*h, w, 40+((len(rows)+9)/10)*h, body))
}
func fatal(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...); os.Exit(1) }
