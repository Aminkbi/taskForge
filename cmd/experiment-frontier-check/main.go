package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aminkbi/taskforge/internal/experiment"
)

func main() {
	input := flag.String("input", "", "directory containing neutral open-loop result JSON")
	maxLoss := flag.Float64("max-throughput-loss", .15, "maximum allowed FIFO/static throughput loss versus Asynq")
	flag.Parse()
	if *input == "" {
		fatal("-input is required")
	}
	paths, err := filepath.Glob(filepath.Join(*input, "*.json"))
	if err != nil {
		fatal("list results: %v", err)
	}
	results := make([]experiment.OpenLoopResult, 0, len(paths))
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			fatal("open %s: %v", path, err)
		}
		var result experiment.OpenLoopResult
		err = json.NewDecoder(file).Decode(&result)
		_ = file.Close()
		if err != nil {
			fatal("decode %s: %v", path, err)
		}
		results = append(results, result)
	}
	target, err := experiment.EvaluateFrontierTarget(results, *maxLoss)
	if err != nil {
		fatal("evaluate frontier: %v", err)
	}
	encoded, _ := json.MarshalIndent(target, "", "  ")
	fmt.Println(string(encoded))
	if !target.TargetMet {
		os.Exit(1)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}
