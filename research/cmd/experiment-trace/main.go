// experiment-trace generates immutable open-loop input before a system under
// test is selected or started.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/aminkbi/taskforge/research/internal/expcli"
	"github.com/aminkbi/taskforge/research/internal/experiment"
)

func main() {
	profilePath := flag.String("profile", "", "open-loop profile JSON")
	output := flag.String("output", "", "new immutable trace JSON (must not exist)")
	seed := flag.Int64("seed", 20260718, "deterministic trace seed")
	flag.Parse()
	if *profilePath == "" || *output == "" {
		expcli.Fatal("-profile and -output are required")
	}
	data, err := os.ReadFile(*profilePath)
	if err != nil {
		expcli.Fatal("read profile: %v", err)
	}
	var profile experiment.OpenLoopProfile
	decoderErr := json.Unmarshal(data, &profile)
	if decoderErr != nil {
		expcli.Fatal("decode profile: %v", decoderErr)
	}
	trace, err := experiment.GenerateOpenLoopTrace(profile, *seed)
	if err != nil {
		expcli.Fatal("generate trace: %v", err)
	}
	if err := experiment.WriteOpenLoopTrace(*output, trace); err != nil {
		expcli.Fatal("write trace: %v", err)
	}
	fmt.Printf("%s %s %d arrivals %d faults\n", *output, trace.Digest, len(trace.Arrivals), len(trace.Faults))
}
