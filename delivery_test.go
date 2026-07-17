package taskforge

import "testing"

func TestDeliveryOwnershipKeyScopesRedisStreamIDs(t *testing.T) {
	t.Parallel()

	first := Delivery{
		Message:   Task{ID: "task-a", Queue: "default", FairnessKey: "tenant-a"},
		Execution: ExecutionMetadata{DeliveryID: "1-0"},
	}
	same := first
	secondStream := first
	secondStream.Message.FairnessKey = "tenant-b"
	secondTask := first
	secondTask.Message.ID = "task-b"
	ambiguousWithoutLengths := Delivery{
		Message:   Task{ID: "a:b", Queue: "default", FairnessKey: "tenant-a"},
		Execution: ExecutionMetadata{DeliveryID: "1-0"},
	}

	if first.OwnershipKey() != same.OwnershipKey() {
		t.Fatal("the same broker entry produced different ownership keys")
	}
	for name, candidate := range map[string]Delivery{
		"fairness stream": secondStream,
		"logical task":    secondTask,
		"delimited task":  ambiguousWithoutLengths,
	} {
		if first.OwnershipKey() == candidate.OwnershipKey() {
			t.Fatalf("%s collision: %q", name, first.OwnershipKey())
		}
	}
}
