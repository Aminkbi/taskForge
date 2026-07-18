// experiment-study-analysis regenerates all second-wave derived evidence from
// the frozen plan, trace corpus, dataset ledger, and raw results.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aminkbi/taskforge/internal/experiment"
)

func main() {
	root := flag.String("root", "research/second-wave", "study artifact directory")
	output := flag.String("output", "", "derived-output root (defaults to study root)")
	flag.Parse()
	if *output == "" {
		*output = *root
	}
	plan, err := experiment.LoadStudyPlan(filepath.Join(*root, "study-plan.json"))
	if err != nil {
		fatal("load plan: %v", err)
	}
	var dataset experiment.StudyDataset
	if err := readJSON(filepath.Join(*root, "data", "dataset.json"), &dataset); err != nil {
		fatal("load dataset: %v", err)
	}
	analysis, err := experiment.AnalyzeStudy(plan, dataset, filepath.Join(*root, "data"), filepath.Join(*root, "traces"))
	if err != nil {
		fatal("analyze: %v", err)
	}
	resultsDir := filepath.Join(*output, "results")
	figuresDir := filepath.Join(*output, "figures")
	paperDir := filepath.Join(*output, "paper")
	for _, dir := range []string{resultsDir, figuresDir, paperDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fatal("create %s: %v", dir, err)
		}
	}
	data, _ := json.MarshalIndent(analysis, "", "  ")
	mustWrite(filepath.Join(resultsDir, "analysis.json"), append(data, '\n'))
	markdown := renderMarkdown(plan, analysis)
	mustWrite(filepath.Join(resultsDir, "analysis.md"), []byte(markdown))
	mustWrite(filepath.Join(figuresDir, "paired-effects.svg"), []byte(renderSVG(analysis)))
	template, err := os.ReadFile(filepath.Join(*root, "paper", "paper.template.md"))
	if err != nil {
		fatal("read paper template: %v", err)
	}
	paper := strings.ReplaceAll(string(template), "{{GENERATED_RESULTS}}", demoteMarkdown(markdown))
	mustWrite(filepath.Join(paperDir, "paper.md"), []byte(paper))
	fmt.Printf("generated %d contrasts, %d failures, %d reversals\n", len(analysis.Contrasts), len(analysis.Failures), len(analysis.Reversals))
}

func renderMarkdown(plan experiment.StudyPlan, analysis experiment.StudyAnalysis) string {
	var b strings.Builder
	b.WriteString("# Paired second-wave results\n\n")
	b.WriteString("Effects are left minus right within the registered environment/profile/seed/repetition blocks. Intervals use the pre-declared confidence shown; they are not detection counts.\n\n")
	for _, contrast := range analysis.Contrasts {
		fmt.Fprintf(&b, "## %s — %s\n\n", contrast.Name, contrast.Environment)
		fmt.Fprintf(&b, "%s versus %s; profiles: %s.\n\n", contrast.Left, contrast.Right, strings.Join(contrast.Profiles, ", "))
		b.WriteString("| Metric | Pairs | Paired median effect | Interval | Relative | Standardized | Left higher |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
		for _, metric := range contrast.Metrics {
			relative := "not defined"
			if metric.RelativePercent != nil {
				relative = fmt.Sprintf("%.2f%%", *metric.RelativePercent)
			}
			fmt.Fprintf(&b, "| %s | %d | %.6g %s | [%.6g, %.6g] (%.3g) | %s | %.3f | %.1f%% |\n", metric.Name, metric.Pairs, metric.Estimate, metric.Unit, metric.Lower, metric.Upper, metric.Confidence, relative, metric.Standardized, metric.LeftWinFraction*100)
		}
		b.WriteString("\n")
	}
	b.WriteString("## Failures and unsupported cells\n\n")
	if len(analysis.Failures) == 0 {
		b.WriteString("No registered cell failed or was unsupported.\n\n")
	} else {
		b.WriteString("| Status | Environment | Profile | Seed | Repetition | System | Reason |\n| --- | --- | --- | ---: | ---: | --- | --- |\n")
		for _, failure := range analysis.Failures {
			fmt.Fprintf(&b, "| %s | %s | %s | %d | %d | %s | %s |\n", failure.Status, failure.Environment, failure.Profile, failure.Seed, failure.Repetition, failure.System, strings.ReplaceAll(failure.Reason, "|", "\\|"))
		}
		b.WriteString("\n")
	}
	b.WriteString("## Environment-specific reversals\n\n")
	if len(analysis.Reversals) == 0 {
		b.WriteString("No paired median changed sign between the two measured environment classes.\n\n")
	} else {
		b.WriteString("| Contrast | Metric | First environment/effect | Second environment/effect |\n| --- | --- | ---: | ---: |\n")
		for _, reversal := range analysis.Reversals {
			fmt.Fprintf(&b, "| %s | %s | %s: %.6g | %s: %.6g |\n", reversal.Contrast, reversal.Metric, reversal.First, reversal.FirstEffect, reversal.Second, reversal.SecondEffect)
		}
		b.WriteString("\n")
	}
	b.WriteString("## Scope boundary\n\n")
	b.WriteString("The measurements cover one 12-logical-CPU workstation in native/direct-loopback and four-Go-processor/latency-injected-loopback classes. The network class is emulated and the physical host is shared; these results do not establish behavior on remote Redis, other hardware, or independent hosts. Recovery is reported as not measured where the adapters cannot apply an equivalent process-kill delivery fault.\n")
	_ = plan
	return b.String()
}

func renderSVG(analysis experiment.StudyAnalysis) string {
	type point struct {
		Label string
		Value float64
	}
	var points []point
	for _, contrast := range analysis.Contrasts {
		for _, metric := range contrast.Metrics {
			points = append(points, point{contrast.Name + " / " + contrast.Environment + " / " + metric.Name, metric.Standardized})
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Label < points[j].Label })
	height := 60 + 22*len(points)
	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="%d" viewBox="0 0 1200 %d" role="img" aria-label="Paired standardized effects by measured environment"><style>text{font-family:ui-sans-serif,system-ui,sans-serif;fill:#151515}.sub{fill:#555}</style><rect width="1200" height="%d" fill="#fff"/><text x="20" y="24" font-size="16" font-weight="600">Paired standardized effects by measured environment</text><text x="20" y="42" font-size="11" class="sub">positive means the registered left arm is higher; direction of benefit depends on the metric</text><line x1="850" y1="52" x2="850" y2="%d" stroke="#888"/>`, height, height, height, height-8)
	for i, point := range points {
		y := 68 + i*22
		width := min(300.0, 80*abs(point.Value))
		x := 850.0
		if point.Value < 0 {
			x -= width
		}
		color := "#2a78d6"
		if point.Value < 0 {
			color = "#eb6834"
		}
		fmt.Fprintf(&b, `<text x="20" y="%d" font-size="10">%s</text><rect x="%.1f" y="%d" width="%.1f" height="12" fill="%s"><title>%.4f</title></rect>`, y+9, html.EscapeString(point.Label), x, y, width, color, point.Value)
	}
	b.WriteString("</svg>\n")
	return b.String()
}

func demoteMarkdown(value string) string {
	lines := strings.Split(value, "\n")
	for index, line := range lines {
		if strings.HasPrefix(line, "#") {
			lines[index] = "#" + line
		}
	}
	return strings.Join(lines, "\n")
}

func readJSON(path string, target any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func mustWrite(path string, data []byte) {
	if err := os.WriteFile(path, data, 0644); err != nil {
		fatal("write %s: %v", path, err)
	}
}

func abs(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
