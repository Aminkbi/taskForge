package worker

import (
	"context"
	"testing"
	"time"

	"github.com/aminkbi/taskforge"
)

func TestNewBuildsDirectWorkerWithDefaults(t *testing.T) {
	t.Parallel()

	b := &stubBroker{}
	w, err := New(Options{
		Broker:  b,
		Handler: taskforge.HandlerFunc(func(context.Context, taskforge.Task) error { return nil }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if w.Broker != b || w.Queue != "default" || w.PoolName != "default" {
		t.Fatalf("unexpected worker identity defaults: %+v", w)
	}
	if w.Concurrency != 1 || w.LeaseTTL != 30*time.Second || w.RetryPolicy.MaxDeliveries != 1 {
		t.Fatalf("unexpected worker runtime defaults: %+v", w)
	}
}

func TestNewRejectsMissingRequiredContracts(t *testing.T) {
	t.Parallel()

	if _, err := New(Options{}); err == nil {
		t.Fatal("New() without broker error = nil")
	}
	if _, err := New(Options{Broker: &stubBroker{}}); err == nil {
		t.Fatal("New() without handler error = nil")
	}
}
