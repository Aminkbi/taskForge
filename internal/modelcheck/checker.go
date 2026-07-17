// Package modelcheck exhaustively explores bounded delivery-ownership and
// scheduler-fencing protocol models. It is intentionally independent of
// Redis and worker goroutines; only canonical protocol value types are shared.
package modelcheck

import (
	"fmt"
	"sort"
	"strings"
)

// Model selects an executable transition system.
type Model string

const (
	DeliveryModel  Model = "delivery"
	SchedulerModel Model = "scheduler"
)

// Mutation selects an intentionally defective transition guard. Mutations
// are test-only evidence that the models can expose the named protocol bug.
type Mutation string

const (
	NoMutation          Mutation = ""
	DeliveryIDOnly      Mutation = "delivery-id-only"
	RetryWithoutReceipt Mutation = "retry-without-receipt"
	SchedulerOwnerOnly  Mutation = "scheduler-owner-only"
)

// Bounds are hard resource limits. The CI bounds are deliberately large
// enough to exhaust the finite models; truncation is reported as a failure.
type Bounds struct {
	MaxDepth  int
	MaxStates int
}

// SmokeBounds returns the reproducible bounds used in CI.
func SmokeBounds() Bounds {
	return Bounds{MaxDepth: 32, MaxStates: 100000}
}

// Report describes one completely explored state graph.
type Report struct {
	Model       Model
	Mutation    Mutation
	States      int
	Transitions int
	MaxDepth    int
	Actions     map[string]int
}

// Violation contains a shortest counterexample because exploration is BFS.
type Violation struct {
	Model     Model
	Mutation  Mutation
	Invariant string
	State     string
	Trace     []string
}

func (v *Violation) Error() string {
	mutation := string(v.Mutation)
	if mutation == "" {
		mutation = "correct"
	}
	return fmt.Sprintf("model check failed: model=%s mutation=%s invariant=%s\ntrace:\n  %s\nstate: %s",
		v.Model, mutation, v.Invariant, strings.Join(v.Trace, "\n  "), v.State)
}

type state interface {
	key() string
	describe() string
}

type transition struct {
	action string
	next   state
}

type protocolModel interface {
	name() Model
	initial() state
	next(state) []transition
	invariant(state) string
	terminal(state) bool
}

type node struct {
	state  state
	parent int
	action string
	depth  int
}

// Check exhaustively checks one finite model. It checks safety after every
// transition and bounded liveness by requiring the complete nonterminal graph
// to have neither a deadlock nor a cycle.
func Check(name Model, mutation Mutation, bounds Bounds) (Report, error) {
	if bounds.MaxDepth <= 0 || bounds.MaxStates <= 0 {
		return Report{}, fmt.Errorf("model check bounds must be positive")
	}

	var model protocolModel
	switch name {
	case DeliveryModel:
		if mutation != NoMutation && mutation != DeliveryIDOnly && mutation != RetryWithoutReceipt {
			return Report{}, fmt.Errorf("mutation %q does not apply to model %q", mutation, name)
		}
		model = newDeliveryModel(mutation)
	case SchedulerModel:
		if mutation != NoMutation && mutation != SchedulerOwnerOnly {
			return Report{}, fmt.Errorf("mutation %q does not apply to model %q", mutation, name)
		}
		model = newSchedulerModel(mutation)
	default:
		return Report{}, fmt.Errorf("unknown model %q", name)
	}

	nodes := []node{{state: model.initial(), parent: -1, action: "Init", depth: 0}}
	seen := map[string]int{nodes[0].state.key(): 0}
	adjacency := make([][]int, 1)
	transitions := 0
	maxDepth := 0
	actions := make(map[string]int)

	for cursor := 0; cursor < len(nodes); cursor++ {
		current := nodes[cursor]
		if invariant := model.invariant(current.state); invariant != "" {
			return Report{}, violation(model, mutation, nodes, cursor, invariant)
		}
		if model.terminal(current.state) {
			continue
		}

		next := model.next(current.state)
		sort.Slice(next, func(i, j int) bool { return next[i].action < next[j].action })
		if len(next) == 0 {
			return Report{}, violation(model, mutation, nodes, cursor, "liveness/nonterminal-deadlock")
		}
		if current.depth >= bounds.MaxDepth {
			return Report{}, violation(model, mutation, nodes, cursor, "bounds/depth-truncated")
		}

		for _, edge := range next {
			transitions++
			actions[actionClass(edge.action)]++
			key := edge.next.key()
			index, ok := seen[key]
			if !ok {
				if len(nodes) >= bounds.MaxStates {
					return Report{}, violation(model, mutation, nodes, cursor, "bounds/state-limit-exceeded")
				}
				index = len(nodes)
				seen[key] = index
				nodes = append(nodes, node{state: edge.next, parent: cursor, action: edge.action, depth: current.depth + 1})
				adjacency = append(adjacency, nil)
				if current.depth+1 > maxDepth {
					maxDepth = current.depth + 1
				}
			}
			adjacency[cursor] = append(adjacency[cursor], index)
		}
	}

	if cycleAt := nonterminalCycle(model, nodes, adjacency); cycleAt >= 0 {
		return Report{}, violation(model, mutation, nodes, cycleAt, "liveness/nonterminal-cycle")
	}

	return Report{
		Model:       name,
		Mutation:    mutation,
		States:      len(nodes),
		Transitions: transitions,
		MaxDepth:    maxDepth,
		Actions:     actions,
	}, nil
}

// CheckAll checks both correct protocol models.
func CheckAll(bounds Bounds) ([]Report, error) {
	reports := make([]Report, 0, 2)
	for _, name := range []Model{DeliveryModel, SchedulerModel} {
		report, err := Check(name, NoMutation, bounds)
		if err != nil {
			return nil, err
		}
		reports = append(reports, report)
	}
	return reports, nil
}

func violation(model protocolModel, mutation Mutation, nodes []node, at int, invariant string) *Violation {
	trace := make([]string, 0, nodes[at].depth+1)
	for index := at; index >= 0; index = nodes[index].parent {
		trace = append(trace, nodes[index].action)
	}
	for left, right := 0, len(trace)-1; left < right; left, right = left+1, right-1 {
		trace[left], trace[right] = trace[right], trace[left]
	}
	return &Violation{
		Model:     model.name(),
		Mutation:  mutation,
		Invariant: invariant,
		State:     nodes[at].state.describe(),
		Trace:     trace,
	}
}

func nonterminalCycle(model protocolModel, nodes []node, adjacency [][]int) int {
	colors := make([]uint8, len(nodes))
	var visit func(int) int
	visit = func(index int) int {
		if model.terminal(nodes[index].state) {
			return -1
		}
		colors[index] = 1
		for _, next := range adjacency[index] {
			if model.terminal(nodes[next].state) {
				continue
			}
			if colors[next] == 1 {
				return next
			}
			if colors[next] == 0 {
				if found := visit(next); found >= 0 {
					return found
				}
			}
		}
		colors[index] = 2
		return -1
	}
	for index := range nodes {
		if colors[index] == 0 && !model.terminal(nodes[index].state) {
			if found := visit(index); found >= 0 {
				return found
			}
		}
	}
	return -1
}

func actionClass(action string) string {
	if index := strings.IndexByte(action, '('); index >= 0 {
		return action[:index]
	}
	return action
}
