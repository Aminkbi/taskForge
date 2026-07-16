package main

import (
	"context"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
)

func TestDemoImportsOnlyPublicTaskForgePackages(t *testing.T) {
	t.Parallel()

	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("main.go"), nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse demo imports: %v", err)
	}
	for _, importSpec := range file.Imports {
		path, err := strconv.Unquote(importSpec.Path.Value)
		if err != nil {
			t.Fatalf("unquote import %q: %v", importSpec.Path.Value, err)
		}
		if path == "github.com/aminkbi/taskforge/internal" || strings.HasPrefix(path, "github.com/aminkbi/taskforge/internal/") {
			t.Fatalf("demo imports internal package %q", path)
		}
	}
}

func TestGateHandlerHoldsFirstWaveAndTracksNoisyConcurrency(t *testing.T) {
	t.Parallel()

	handler := newGateHandler()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	errors := make(chan error, 2)
	for _, task := range []taskforge.Task{
		{ID: "protected", FairnessKey: protectedTenant},
		{ID: "noisy", FairnessKey: noisyTenant},
	} {
		task := task
		go func() { errors <- handler.HandleTask(ctx, task) }()
	}

	starts, err := handler.waitForStarts(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !containsTenant(starts, protectedTenant) || !containsTenant(starts, noisyTenant) {
		t.Fatalf("first wave = %v, want protected and noisy tenants", starts)
	}
	if got := handler.maxNoisyConcurrency(); got != 1 {
		t.Fatalf("max noisy concurrency = %d, want 1", got)
	}

	handler.releaseFirstWave()
	for range 2 {
		if err := <-errors; err != nil {
			t.Fatalf("HandleTask() error = %v", err)
		}
	}
}
