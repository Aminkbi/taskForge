package redis

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/aminkbi/taskforge"
)

const (
	HeaderRoutingRule  = "taskforge_routing_rule"
	HeaderShard        = "taskforge_shard"
	HeaderTrafficClass = "taskforge_traffic_class"
)

type RoutingPolicy struct {
	DefaultQueue string
	DefaultShard string
	Rules        []RoutingRule
}

type RoutingRule struct {
	Name        string
	Match       RoutingMatch
	Destination RoutingDestination
}

type RoutingMatch struct {
	TaskNames      []string          `json:"task_names"`
	Queues         []string          `json:"queues"`
	FairnessKeys   []string          `json:"fairness_keys"`
	TrafficClasses []string          `json:"traffic_classes"`
	Headers        map[string]string `json:"headers"`
}

type RoutingDestination struct {
	Queue   string
	Shard   string
	Shards  []string
	ShardBy string
}

type RoutingPlacement struct {
	Queue   string
	Shard   string
	Rule    string
	Matched bool
}

type rawPolicy struct {
	DefaultQueue string    `json:"default_queue"`
	DefaultShard string    `json:"default_shard"`
	Rules        []rawRule `json:"rules"`
}

type rawRule struct {
	Name        string         `json:"name"`
	Match       RoutingMatch   `json:"match"`
	Destination rawDestination `json:"destination"`
}

type rawDestination struct {
	Queue   string   `json:"queue"`
	Shard   string   `json:"shard"`
	Shards  []string `json:"shards"`
	ShardBy string   `json:"shard_by"`
}

func ParseRoutingPolicyJSON(data []byte) (*RoutingPolicy, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, nil
	}

	var raw rawPolicy
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse routing policy json: %w", err)
	}

	policy := &RoutingPolicy{
		DefaultQueue: normalizeRoutingQueue(raw.DefaultQueue),
		DefaultShard: strings.TrimSpace(raw.DefaultShard),
		Rules:        make([]RoutingRule, 0, len(raw.Rules)),
	}
	for i, rawRule := range raw.Rules {
		rule, err := parseRule(i, rawRule)
		if err != nil {
			return nil, err
		}
		policy.Rules = append(policy.Rules, rule)
	}
	return policy, nil
}

func (p *RoutingPolicy) Place(msg taskforge.Task) RoutingPlacement {
	queue := effectiveQueue(msg.Queue)
	placement := RoutingPlacement{
		Queue: queue,
		Shard: strings.TrimSpace(p.defaultShard()),
	}
	if p == nil {
		return placement
	}
	if p.DefaultQueue != "" {
		placement.Queue = p.DefaultQueue
	}

	for _, rule := range p.Rules {
		if !rule.Match.matches(msg) {
			continue
		}
		placement.Matched = true
		placement.Rule = rule.Name
		if rule.Destination.Queue != "" {
			placement.Queue = rule.Destination.Queue
		}
		placement.Shard = rule.Destination.resolveShard(msg, placement.Shard)
		return placement
	}

	return placement
}

func (p *RoutingPolicy) Apply(msg taskforge.Task) (taskforge.Task, RoutingPlacement) {
	placement := p.Place(msg)
	msg.Queue = placement.Queue
	if placement.Rule != "" || placement.Shard != "" {
		msg.Headers = cloneHeaders(msg.Headers)
		if placement.Rule != "" {
			msg.Headers[HeaderRoutingRule] = placement.Rule
		}
		if placement.Shard != "" {
			msg.Headers[HeaderShard] = placement.Shard
		}
	}
	return msg, placement
}

func parseRule(index int, raw rawRule) (RoutingRule, error) {
	name := strings.TrimSpace(raw.Name)
	if name == "" {
		return RoutingRule{}, fmt.Errorf("routing policy rule[%d] name is required", index)
	}

	destination, err := parseDestination(index, raw.Destination)
	if err != nil {
		return RoutingRule{}, err
	}

	rule := RoutingRule{
		Name:        name,
		Match:       normalizeMatch(raw.Match),
		Destination: destination,
	}
	if rule.Match.empty() {
		return RoutingRule{}, fmt.Errorf("routing policy rule %q must set at least one match condition", name)
	}
	return rule, nil
}

func parseDestination(index int, raw rawDestination) (RoutingDestination, error) {
	shardBy, err := normalizeShardBy(raw.ShardBy)
	if err != nil {
		return RoutingDestination{}, fmt.Errorf("routing policy rule[%d] destination.shard_by: %w", index, err)
	}
	destination := RoutingDestination{
		Queue:   normalizeRoutingQueue(raw.Queue),
		Shard:   strings.TrimSpace(raw.Shard),
		ShardBy: shardBy,
	}
	if destination.Shard != "" && len(raw.Shards) > 0 {
		return RoutingDestination{}, fmt.Errorf("routing policy rule[%d] destination cannot set both shard and shards", index)
	}
	for _, shard := range raw.Shards {
		shard = strings.TrimSpace(shard)
		if shard == "" {
			return RoutingDestination{}, fmt.Errorf("routing policy rule[%d] destination shards must be non-empty", index)
		}
		destination.Shards = append(destination.Shards, shard)
	}
	return destination, nil
}

func normalizeMatch(match RoutingMatch) RoutingMatch {
	match.TaskNames = normalizeList(match.TaskNames)
	match.Queues = normalizeQueues(match.Queues)
	match.FairnessKeys = normalizeList(match.FairnessKeys)
	match.TrafficClasses = normalizeList(match.TrafficClasses)
	if len(match.Headers) > 0 {
		headers := make(map[string]string, len(match.Headers))
		for key, value := range match.Headers {
			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)
			if key == "" || value == "" {
				continue
			}
			headers[key] = value
		}
		match.Headers = headers
	}
	return match
}

func (m RoutingMatch) empty() bool {
	return len(m.TaskNames) == 0 &&
		len(m.Queues) == 0 &&
		len(m.FairnessKeys) == 0 &&
		len(m.TrafficClasses) == 0 &&
		len(m.Headers) == 0
}

func (m RoutingMatch) matches(msg taskforge.Task) bool {
	if len(m.TaskNames) > 0 && !contains(m.TaskNames, strings.TrimSpace(msg.Name)) {
		return false
	}
	if len(m.Queues) > 0 && !contains(m.Queues, effectiveQueue(msg.Queue)) {
		return false
	}
	if len(m.FairnessKeys) > 0 && !contains(m.FairnessKeys, strings.TrimSpace(msg.FairnessKey)) {
		return false
	}
	if len(m.TrafficClasses) > 0 && !contains(m.TrafficClasses, trafficClass(msg.Headers)) {
		return false
	}
	for key, want := range m.Headers {
		if strings.TrimSpace(msg.Headers[key]) != want {
			return false
		}
	}
	return true
}

func (d RoutingDestination) resolveShard(msg taskforge.Task, fallback string) string {
	if d.Shard != "" {
		return d.Shard
	}
	if len(d.Shards) == 0 {
		return fallback
	}
	key := shardKey(msg, d.ShardBy)
	index := stableIndex(key, len(d.Shards))
	return d.Shards[index]
}

func shardKey(msg taskforge.Task, shardBy string) string {
	switch {
	case shardBy == "task_id":
		return fallbackKey(msg.ID, msg.FairnessKey, msg.Name)
	case shardBy == "task_name":
		return fallbackKey(msg.Name, msg.ID)
	case shardBy == "queue":
		return fallbackKey(effectiveQueue(msg.Queue), msg.ID)
	case strings.HasPrefix(shardBy, "header:"):
		header := strings.TrimSpace(strings.TrimPrefix(shardBy, "header:"))
		return fallbackKey(msg.Headers[header], msg.FairnessKey, msg.ID)
	default:
		return fallbackKey(msg.FairnessKey, msg.ID, msg.Name)
	}
}

func fallbackKey(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return "default"
}

func stableIndex(key string, size int) int {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum64() % uint64(size))
}

func trafficClass(headers map[string]string) string {
	if len(headers) == 0 {
		return ""
	}
	return strings.TrimSpace(headers[HeaderTrafficClass])
}

func normalizeList(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func normalizeQueues(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = normalizeRoutingQueue(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func normalizeRoutingQueue(queue string) string {
	queue = strings.TrimSpace(queue)
	if queue == "" {
		return ""
	}
	return queue
}

func effectiveQueue(queue string) string {
	queue = normalizeRoutingQueue(queue)
	if queue == "" {
		return "default"
	}
	return queue
}

func normalizeShardBy(value string) (string, error) {
	value = strings.TrimSpace(value)
	switch {
	case value == "":
		return "fairness_key", nil
	case value == "fairness_key", value == "task_id", value == "task_name", value == "queue":
		return value, nil
	case strings.HasPrefix(value, "header:") && strings.TrimSpace(strings.TrimPrefix(value, "header:")) != "":
		return value, nil
	default:
		return "", fmt.Errorf("must be one of fairness_key, task_id, task_name, queue, or header:<name>")
	}
}

func (p *RoutingPolicy) defaultShard() string {
	if p == nil {
		return ""
	}
	return p.DefaultShard
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func cloneHeaders(headers map[string]string) map[string]string {
	cloned := make(map[string]string, len(headers)+2)
	for key, value := range headers {
		cloned[key] = value
	}
	return cloned
}
