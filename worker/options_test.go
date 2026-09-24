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
	deadLetters := &stubDeadLetter{}
	w, err := New(Options{
		Broker:     b,
		DeadLetter: deadLetters,
		Handler:    taskforge.HandlerFunc(func(context.Context, taskforge.Task) error { return nil }),
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
	if w.DeadLetter != deadLetters {
		t.Fatal("New() did not preserve the configured dead-letter publisher")
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
	if _, err := New(Options{
		Broker:  &stubBroker{},
		Handler: taskforge.HandlerFunc(func(context.Context, taskforge.Task) error { return nil }),
	}); err == nil {
		t.Fatal("New() without dead-letter publisher error = nil")
	}
}

type deliveryOnlyBroker struct{ *stubBroker }

func (deliveryOnlyBroker) StateWritesEnabled() bool { return false }

func (deliveryOnlyBroker) RecordQueued(context.Context, taskforge.Task) error { return nil }

func (deliveryOnlyBroker) RecordDelivery(context.Context, taskforge.Delivery, taskforge.State, []byte) error {
	return nil
}

func (deliveryOnlyBroker) Get(context.Context, string) (taskforge.TaskRecord, error) {
	return taskforge.TaskRecord{}, nil
}

func (b deliveryOnlyBroker) OwnsStateStore(store taskforge.StateStore) bool {
	return store == b
}

func (b deliveryOnlyBroker) AckAndRecord(ctx context.Context, delivery taskforge.Delivery, state taskforge.State) error {
	return b.Ack(ctx, delivery)
}

func TestNewAcceptsBrokerStateStoreWithDeliveryOnlyBroker(t *testing.T) {
	t.Parallel()
	broker := deliveryOnlyBroker{stubBroker: &stubBroker{}}
	if _, err := New(Options{
		Broker:     broker,
		DeadLetter: &stubDeadLetter{},
		Handler:    taskforge.HandlerFunc(func(context.Context, taskforge.Task) error { return nil }),
		StateStore: broker,
	}); err != nil {
		t.Fatalf("New() rejected broker state store: %v", err)
	}
}

func TestNewRejectsCustomStoreWithDeliveryOnlyBroker(t *testing.T) {
	t.Parallel()
	_, err := New(Options{
		Broker:     deliveryOnlyBroker{stubBroker: &stubBroker{}},
		DeadLetter: &stubDeadLetter{},
		Handler:    taskforge.HandlerFunc(func(context.Context, taskforge.Task) error { return nil }),
		StateStore: &stubStateStore{},
	})
	if err == nil {
		t.Fatal("New() accepted a custom state store with delivery-only broker state")
	}
}
