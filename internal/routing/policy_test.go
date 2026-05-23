package routing

import (
	"testing"

	"github.com/aminkbi/taskforge/internal/broker"
)

func TestPolicyApplyRoutesFirstMatchingRule(t *testing.T) {
	policy, err := ParseJSON([]byte(`{
		"default_queue":"default",
		"default_shard":"shard-a",
		"rules":[
			{
				"name":"vip-critical",
				"match":{
					"task_names":["billing.charge"],
					"fairness_keys":["tenant-vip"],
					"headers":{"region":"eu"},
					"traffic_classes":["critical"]
				},
				"destination":{"queue":"critical","shard":"shard-eu"}
			}
		]
	}`))
	if err != nil {
		t.Fatalf("ParseJSON() error = %v", err)
	}

	msg, placement := policy.Apply(broker.TaskMessage{
		ID:          "task-1",
		Name:        "billing.charge",
		Queue:       "ingress",
		FairnessKey: "tenant-vip",
		Headers: map[string]string{
			"region":           "eu",
			HeaderTrafficClass: "critical",
			"existing-header":  "preserved",
		},
	})

	if placement.Queue != "critical" || placement.Shard != "shard-eu" || placement.Rule != "vip-critical" || !placement.Matched {
		t.Fatalf("unexpected placement: %+v", placement)
	}
	if msg.Queue != "critical" {
		t.Fatalf("msg.Queue = %q, want critical", msg.Queue)
	}
	if msg.Headers[HeaderRoutingRule] != "vip-critical" || msg.Headers[HeaderShard] != "shard-eu" {
		t.Fatalf("routing headers = %+v", msg.Headers)
	}
	if msg.Headers["existing-header"] != "preserved" {
		t.Fatalf("existing header not preserved: %+v", msg.Headers)
	}
}

func TestPolicyPlaceFallsBackToDefaultPlacement(t *testing.T) {
	policy, err := ParseJSON([]byte(`{"default_queue":"bulk","default_shard":"shard-a"}`))
	if err != nil {
		t.Fatalf("ParseJSON() error = %v", err)
	}

	placement := policy.Place(broker.TaskMessage{ID: "task-1", Name: "unknown"})

	if placement.Queue != "bulk" || placement.Shard != "shard-a" || placement.Matched {
		t.Fatalf("unexpected fallback placement: %+v", placement)
	}
}

func TestPolicyChoosesStableShard(t *testing.T) {
	policy, err := ParseJSON([]byte(`{
		"rules":[
			{
				"name":"tenant-spread",
				"match":{"traffic_classes":["bulk"]},
				"destination":{"queue":"bulk","shards":["shard-a","shard-b","shard-c"],"shard_by":"fairness_key"}
			}
		]
	}`))
	if err != nil {
		t.Fatalf("ParseJSON() error = %v", err)
	}

	msg := broker.TaskMessage{
		ID:          "task-1",
		FairnessKey: "tenant-42",
		Headers:     map[string]string{HeaderTrafficClass: "bulk"},
	}
	first := policy.Place(msg)
	for i := 0; i < 10; i++ {
		next := policy.Place(msg)
		if next != first {
			t.Fatalf("placement changed: first=%+v next=%+v", first, next)
		}
	}
	if first.Shard == "" {
		t.Fatalf("expected shard placement: %+v", first)
	}
}

func TestPolicyMatchesOriginalQueue(t *testing.T) {
	policy, err := ParseJSON([]byte(`{
		"rules":[
			{
				"name":"critical-ingress",
				"match":{"queues":["critical.in"]},
				"destination":{"queue":"critical.ready"}
			}
		]
	}`))
	if err != nil {
		t.Fatalf("ParseJSON() error = %v", err)
	}

	placement := policy.Place(broker.TaskMessage{ID: "task-1", Queue: "critical.in"})

	if placement.Queue != "critical.ready" || placement.Rule != "critical-ingress" {
		t.Fatalf("unexpected placement: %+v", placement)
	}
}

func TestParseJSONRejectsInvalidRules(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "missing rule name", raw: `{"rules":[{"match":{"task_names":["demo"]},"destination":{"queue":"demo"}}]}`},
		{name: "missing match", raw: `{"rules":[{"name":"all","destination":{"queue":"demo"}}]}`},
		{name: "conflicting shard forms", raw: `{"rules":[{"name":"bad","match":{"task_names":["demo"]},"destination":{"shard":"a","shards":["b"]}}]}`},
		{name: "invalid shard by", raw: `{"rules":[{"name":"bad","match":{"task_names":["demo"]},"destination":{"shards":["a","b"],"shard_by":"tenant"}}]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseJSON([]byte(tt.raw)); err == nil {
				t.Fatal("ParseJSON() error = nil, want non-nil")
			}
		})
	}
}
