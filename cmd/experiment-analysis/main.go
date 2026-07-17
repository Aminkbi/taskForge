// taskforge-experiment-analysis derives the pre-registered statistical report
// and figures from committed raw experiment evidence. Its output is
// byte-reproducible for a given input directory and bootstrap seed, so a
// reviewer can regenerate every table and chart and diff the result.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/aminkbi/taskforge/internal/experiment"
)

// variantOrder fixes display order; identity is carried by axis labels, and
// the two colors encode comparability, not rank.
var variantOrder = []string{
	"taskforge-fifo-static",
	"taskforge-no-fairness",
	"taskforge-no-admission",
	"taskforge-no-adaptive",
	"taskforge-no-dependency-budget",
	"taskforge-full",
	"asynq",
}

var variantShort = map[string]string{
	"taskforge-fifo-static":          "fifo",
	"taskforge-no-fairness":          "-fair",
	"taskforge-no-admission":         "-admit",
	"taskforge-no-adaptive":          "-adapt",
	"taskforge-no-dependency-budget": "-budget",
	"taskforge-full":                 "full",
	"asynq":                          "asynq",
}

const (
	colorComparable    = "#2a78d6"
	colorNonComparable = "#eb6834"
	colorInk           = "#0b0b0b"
	colorInkSecondary  = "#52514e"
	colorGrid          = "#e4e3df"
	colorSurface       = "#fcfcfb"
)

type figureSpec struct {
	file      string
	metric    string
	title     string
	subtitle  string
	manifests []string // empty means all
	reference float64  // horizontal annotation line when > 0
	refLabel  string
}

func figures() []figureSpec {
	return []figureSpec{
		{file: "completion-p99.svg", metric: "completion_p99_ms", title: "p99 enqueue-to-completion latency (ms)", subtitle: "median of 12 seeded runs; whiskers are seeded bootstrap 95% intervals"},
		{file: "throughput.svg", metric: "throughput_per_second", title: "Throughput (tasks/s)", subtitle: "median of 12 seeded runs; whiskers are seeded bootstrap 95% intervals"},
		{file: "jain-fairness.svg", metric: "jain_fairness", title: "Jain fairness over per-tenant SLO-compliant completion ratios", subtitle: "0 is recorded when no tenant met its SLO; median of 12 runs with bootstrap 95% intervals"},
		{file: "slo-violations.svg", metric: "slo_violations", title: "SLO violations per run", subtitle: "median of 12 seeded runs; whiskers are seeded bootstrap 95% intervals"},
		{file: "nondominant-slo-violations.svg", metric: "nondominant_slo_violations", title: "SLO violations for non-dominant tenants", subtitle: "tenants other than the highest offered-load tenant; median of 12 runs, bootstrap 95% intervals", manifests: []string{"noisy-neighbor", "tenant-skew"}},
		{file: "peak-concurrency.svg", metric: "peak_concurrency", title: "Peak concurrent executions (downstream-overload proxy)", subtitle: "hot-dependency workload; the dependency budget capacity is 2", manifests: []string{"hot-dependency"}, reference: 2, refLabel: "budget capacity"},
	}
}

func main() {
	input := flag.String("input", "research/data/raw", "raw result directory (.json or .json.gz)")
	results := flag.String("results", "research/results", "derived analysis output directory")
	figuresDir := flag.String("figures", "research/figures", "derived SVG figure directory")
	bootstrapSeed := flag.Uint64("bootstrap-seed", 20260717, "seed for the deterministic bootstrap")
	resamples := flag.Int("resamples", 10000, "bootstrap resamples")
	allowPartial := flag.Bool("allow-partial", false, "allow non-registered development inputs")
	flag.Parse()

	raw, err := experiment.LoadRawResults(*input)
	if err != nil {
		fatal("load raw results: %v", err)
	}
	if !*allowPartial {
		if err := experiment.ValidateRegisteredGrid(raw); err != nil {
			fatal("validate registered grid: %v", err)
		}
	}
	analysis := experiment.Analyze(raw, *bootstrapSeed, *resamples)

	if err := os.MkdirAll(*results, 0755); err != nil {
		fatal("mkdir: %v", err)
	}
	if err := os.MkdirAll(*figuresDir, 0755); err != nil {
		fatal("mkdir: %v", err)
	}
	data, err := json.MarshalIndent(analysis, "", "  ")
	if err != nil {
		fatal("encode analysis: %v", err)
	}
	if err := os.WriteFile(filepath.Join(*results, "analysis.json"), append(data, '\n'), 0644); err != nil {
		fatal("write analysis.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(*results, "analysis.md"), []byte(markdown(analysis)), 0644); err != nil {
		fatal("write analysis.md: %v", err)
	}
	for _, spec := range figures() {
		svg, ok := renderFigure(analysis, spec)
		if !ok {
			continue
		}
		if err := os.WriteFile(filepath.Join(*figuresDir, spec.file), svg, 0644); err != nil {
			fatal("write %s: %v", spec.file, err)
		}
	}
	fmt.Printf("%s\n", filepath.Join(*results, "analysis.md"))
}

func manifestNames(analysis experiment.Analysis) []string {
	var names []string
	for _, cell := range analysis.Cells {
		if !slices.Contains(names, cell.Manifest) {
			names = append(names, cell.Manifest)
		}
	}
	slices.Sort(names)
	return names
}

func cellFor(analysis experiment.Analysis, manifest, variant string) (experiment.Cell, bool) {
	for _, cell := range analysis.Cells {
		if cell.Manifest == manifest && cell.Variant == variant {
			return cell, true
		}
	}
	return experiment.Cell{}, false
}

func markdown(analysis experiment.Analysis) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Registered experiment analysis\n\n")
	fmt.Fprintf(&b, "Generated by `taskforge-experiment-analysis`; do not edit. Derived from %d raw runs with a seeded bootstrap (seed %d, %d resamples). Regenerate with `make research-analysis`.\n\n", analysis.Runs, analysis.BootstrapSeed, analysis.Resamples)
	fmt.Fprintf(&b, "Values are medians across seeds with seeded bootstrap 95%% intervals. The `asynq` arm is a non-comparable baseline reported descriptively; it exposes none of the contrasted controls, and its `worker-crash` cell has no crash injected, so it records no recovery event. In workloads with retries, `peak_concurrency` spans first start to final completion and therefore measures in-flight logical tasks, not execution overlap; it is an execution-overlap proxy only in retry-free workloads. `duplicates` counts every re-delivered task, including intentional retries.\n")

	for _, manifest := range manifestNames(analysis) {
		fmt.Fprintf(&b, "\n## %s\n\n", manifest)
		fmt.Fprintf(&b, "| Variant | p99 completion (ms) | p99 enqueue-to-start (ms) | Throughput (tasks/s) | Jain fairness | SLO violations | Non-dominant violations | Peak concurrency | Retries | Duplicates | Redis commands |\n")
		fmt.Fprintf(&b, "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
		for _, variant := range variantOrder {
			cell, ok := cellFor(analysis, manifest, variant)
			if !ok {
				continue
			}
			if unsupportedCell(manifest, variant) {
				fmt.Fprintf(&b, "| %s | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured | not measured |\n", variant)
				continue
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				variant,
				summarize(cell, "completion_p99_ms", 1),
				summarize(cell, "enqueue_to_start_p99_ms", 1),
				summarize(cell, "throughput_per_second", 0),
				summarize(cell, "jain_fairness", 3),
				summarize(cell, "slo_violations", 0),
				summarize(cell, "nondominant_slo_violations", 0),
				summarize(cell, "peak_concurrency", 0),
				summarize(cell, "retries", 0),
				summarize(cell, "duplicates", 0),
				summarize(cell, "redis_commands", 0),
			)
		}
		if manifest == "worker-crash" {
			fmt.Fprintf(&b, "\nRecovery time after the crashed reservation (TaskForge only): ")
			for _, variant := range variantOrder {
				if unsupportedCell(manifest, variant) {
					continue
				}
				if cell, ok := cellFor(analysis, manifest, variant); ok {
					fmt.Fprintf(&b, "%s %s ms; ", variant, summarize(cell, "recovery_ms", 0))
				}
			}
			fmt.Fprintf(&b, "\n")
		}
	}

	fmt.Fprintf(&b, "\n## Pre-registered contrasts\n\n")
	fmt.Fprintf(&b, "Difference of medians, `taskforge-full` minus the listed arm; an interval excluding zero is marked detected. Every pre-registered contrast is listed, including unfavorable and inconclusive ones.\n\n")
	fmt.Fprintf(&b, "| Workload | Metric | Against | Difference | 95%% interval | Detected |\n")
	fmt.Fprintf(&b, "| --- | --- | --- | --- | --- | --- |\n")
	for _, contrast := range analysis.Contrasts {
		mark := ""
		if contrast.Detected {
			mark = "yes"
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %+.2f | [%+.2f, %+.2f] | %s |\n",
			contrast.Manifest, contrast.Metric, contrast.Against, contrast.Difference, contrast.Lo, contrast.Hi, mark)
	}
	return b.String()
}

func unsupportedCell(manifest, variant string) bool {
	return manifest == "worker-crash" && variant == "asynq"
}

func summarize(cell experiment.Cell, metric string, decimals int) string {
	summary, ok := cell.Metrics[metric]
	if !ok || summary.N == 0 || math.IsNaN(summary.Median) {
		return "-"
	}
	return fmt.Sprintf("%.*f [%.*f, %.*f]", decimals, summary.Median, decimals, summary.Lo, decimals, summary.Hi)
}

// renderFigure draws small-multiple panels (one per workload) of bars per
// variant with bootstrap-interval whiskers. Bars share one y-scale per panel.
func renderFigure(analysis experiment.Analysis, spec figureSpec) ([]byte, bool) {
	manifests := spec.manifests
	if len(manifests) == 0 {
		manifests = manifestNames(analysis)
	}
	const panelWidth, panelHeight = 380, 240
	const plotLeft, plotTop, plotBottom = 52, 34, 40
	columns := min(len(manifests), 3)
	rows := (len(manifests) + columns - 1) / columns
	width := columns*panelWidth + 24
	height := rows*panelHeight + 74

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" role="img" aria-label="%s">`, width, height, width, height, html.EscapeString(spec.title))
	fmt.Fprintf(&b, `<style>text{font-family:ui-sans-serif,system-ui,sans-serif;fill:%s}.sub{fill:%s}</style>`, colorInk, colorInkSecondary)
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="%s"/>`, width, height, colorSurface)
	fmt.Fprintf(&b, `<text x="16" y="22" font-size="15" font-weight="600">%s</text>`, html.EscapeString(spec.title))
	fmt.Fprintf(&b, `<text x="16" y="38" font-size="11" class="sub">%s</text>`, html.EscapeString(spec.subtitle))
	legendX := 16
	fmt.Fprintf(&b, `<g transform="translate(%d,%d)"><rect width="10" height="10" rx="2" fill="%s"/><text x="14" y="9" font-size="11" class="sub">TaskForge variants (comparable)</text><rect x="200" width="10" height="10" rx="2" fill="%s"/><text x="214" y="9" font-size="11" class="sub">Asynq (non-comparable baseline)</text></g>`, legendX, 46, colorComparable, colorNonComparable)

	drewAny := false
	for index, manifest := range manifests {
		type bar struct {
			label      string
			value      float64
			lo, hi     float64
			comparable bool
		}
		var bars []bar
		maxValue := 0.0
		for _, variant := range variantOrder {
			if unsupportedCell(manifest, variant) {
				continue
			}
			cell, ok := cellFor(analysis, manifest, variant)
			if !ok {
				continue
			}
			summary, ok := cell.Metrics[spec.metric]
			if !ok || summary.N == 0 {
				continue
			}
			bars = append(bars, bar{variantShort[variant], summary.Median, summary.Lo, summary.Hi, variant != "asynq"})
			maxValue = math.Max(maxValue, summary.Hi)
		}
		if len(bars) == 0 {
			continue
		}
		drewAny = true
		maxValue = math.Max(maxValue, spec.reference)
		if maxValue == 0 {
			maxValue = 1
		}
		originX := 16 + (index%columns)*panelWidth
		originY := 66 + (index/columns)*panelHeight
		plotWidth := panelWidth - plotLeft - 16
		plotHeight := panelHeight - plotTop - plotBottom
		y := func(value float64) float64 {
			return float64(originY+plotTop) + float64(plotHeight)*(1-value/maxValue)
		}
		fmt.Fprintf(&b, `<text x="%d" y="%d" font-size="12" font-weight="600">%s</text>`, originX+plotLeft, originY+22, html.EscapeString(manifest))
		for _, fraction := range []float64{0, 0.5, 1} {
			value := maxValue * fraction
			fmt.Fprintf(&b, `<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="%s" stroke-width="1"/>`, originX+plotLeft, y(value), originX+plotLeft+plotWidth, y(value), colorGrid)
			fmt.Fprintf(&b, `<text x="%d" y="%.1f" font-size="9" text-anchor="end" class="sub">%s</text>`, originX+plotLeft-6, y(value)+3, formatTick(value))
		}
		if spec.reference > 0 {
			fmt.Fprintf(&b, `<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="%s" stroke-width="1.5" stroke-dasharray="5 4"/>`, originX+plotLeft, y(spec.reference), originX+plotLeft+plotWidth, y(spec.reference), colorInkSecondary)
			fmt.Fprintf(&b, `<text x="%d" y="%.1f" font-size="9" class="sub">%s</text>`, originX+plotLeft+4, y(spec.reference)-4, html.EscapeString(spec.refLabel))
		}
		step := float64(plotWidth) / float64(len(bars))
		barWidth := math.Min(step-8, 34)
		for i, item := range bars {
			center := float64(originX+plotLeft) + step*(float64(i)+0.5)
			color := colorComparable
			if !item.comparable {
				color = colorNonComparable
			}
			top := y(item.value)
			bottom := y(0)
			if bottom-top < 1 {
				top = bottom - 1
			}
			fmt.Fprintf(&b, `<path d="M%.1f %.1f L%.1f %.1f Q%.1f %.1f %.1f %.1f L%.1f %.1f Q%.1f %.1f %.1f %.1f Z" fill="%s"><title>%s / %s: %.2f [%.2f, %.2f]</title></path>`,
				center-barWidth/2, bottom, center-barWidth/2, top+4, center-barWidth/2, top, center-barWidth/2+4, top,
				center+barWidth/2-4, top, center+barWidth/2, top, center+barWidth/2, top+4,
				color,
				html.EscapeString(manifest), html.EscapeString(item.label), item.value, item.lo, item.hi)
			fmt.Fprintf(&b, `<path d="M%.1f %.1f L%.1f %.1f Z" fill="none"/>`, center+barWidth/2, bottom, center-barWidth/2, bottom)
			fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" stroke-width="1.5"/>`, center, y(item.lo), center, y(item.hi), colorInk)
			for _, whisker := range []float64{item.lo, item.hi} {
				fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" stroke-width="1.5"/>`, center-4, y(whisker), center+4, y(whisker), colorInk)
			}
			fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="9" text-anchor="middle" class="sub">%s</text>`, center, float64(originY+plotTop+plotHeight)+14, html.EscapeString(item.label))
		}
	}
	fmt.Fprintf(&b, `</svg>`)
	return []byte(b.String()), drewAny
}

func formatTick(value float64) string {
	switch {
	case value == 0:
		return "0"
	case value >= 100:
		return fmt.Sprintf("%.0f", value)
	case value >= 1:
		return fmt.Sprintf("%.1f", value)
	default:
		return fmt.Sprintf("%.2f", value)
	}
}

func fatal(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...); os.Exit(1) }
