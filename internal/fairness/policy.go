package fairness

import (
	"fmt"
	"slices"
	"strings"
)

const DefaultKey = "__taskforge_default__"

type Rule struct {
	Name                string
	Keys                []string
	Weight              int
	ReservedConcurrency int
	SoftQuota           int
	HardQuota           int
	Burst               int
}

type Policy struct {
	defaultRule ResolvedRule
	byKey       map[string]ResolvedRule
	byBucket    map[string]ResolvedRule
	buckets     []string
}

type ResolvedRule struct {
	Bucket              string
	Weight              int
	ReservedConcurrency int
	SoftQuota           int
	HardQuota           int
	Burst               int
}

func NewPolicy(defaultRule Rule, rules []Rule) (*Policy, error) {
	compiledDefault, err := compileRule(defaultRule, true)
	if err != nil {
		return nil, err
	}

	byKey := make(map[string]ResolvedRule)
	byBucket := make(map[string]ResolvedRule)
	buckets := []string{compiledDefault.Bucket}
	seenBuckets := map[string]struct{}{compiledDefault.Bucket: {}}
	byBucket[compiledDefault.Bucket] = compiledDefault

	for _, rule := range rules {
		compiled, err := compileRule(rule, false)
		if err != nil {
			return nil, err
		}
		if _, exists := seenBuckets[compiled.Bucket]; exists {
			return nil, fmt.Errorf("duplicate fairness rule name %q", compiled.Bucket)
		}
		seenBuckets[compiled.Bucket] = struct{}{}
		buckets = append(buckets, compiled.Bucket)
		byBucket[compiled.Bucket] = compiled

		for _, key := range rule.Keys {
			normalized := NormalizeKey(key)
			if normalized == DefaultKey {
				return nil, fmt.Errorf("fairness rule %q cannot target the default fairness key", compiled.Bucket)
			}
			if _, exists := byKey[normalized]; exists {
				return nil, fmt.Errorf("duplicate fairness key %q", strings.TrimSpace(key))
			}
			byKey[normalized] = compiled
		}
	}

	slices.Sort(buckets)
	return &Policy{
		defaultRule: compiledDefault,
		byKey:       byKey,
		byBucket:    byBucket,
		buckets:     buckets,
	}, nil
}

func NormalizeKey(key string) string {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return DefaultKey
	}
	return trimmed
}

func (p *Policy) Resolve(key string) ResolvedRule {
	if p == nil {
		return ResolvedRule{Bucket: "default", Weight: 1}
	}
	if rule, ok := p.byKey[NormalizeKey(key)]; ok {
		return rule
	}
	return p.defaultRule
}

func (p *Policy) Buckets() []string {
	if p == nil {
		return nil
	}
	return slices.Clone(p.buckets)
}

func (p *Policy) BucketRule(bucket string) (ResolvedRule, bool) {
	if p == nil {
		return ResolvedRule{}, false
	}
	rule, ok := p.byBucket[strings.TrimSpace(bucket)]
	return rule, ok
}

func compileRule(rule Rule, isDefault bool) (ResolvedRule, error) {
	bucket := strings.TrimSpace(rule.Name)
	if isDefault {
		if bucket == "" {
			bucket = "default"
		}
	} else if bucket == "" {
		return ResolvedRule{}, fmt.Errorf("fairness rule name is required")
	}

	if rule.Weight < 0 || rule.ReservedConcurrency < 0 || rule.SoftQuota < 0 || rule.HardQuota < 0 || rule.Burst < 0 {
		return ResolvedRule{}, fmt.Errorf("fairness rule %q values must be >= 0", bucket)
	}
	if rule.Weight == 0 {
		rule.Weight = 1
	}
	if rule.HardQuota > 0 && rule.SoftQuota > rule.HardQuota {
		return ResolvedRule{}, fmt.Errorf("fairness rule %q soft_quota must be <= hard_quota", bucket)
	}
	if rule.HardQuota > 0 && rule.ReservedConcurrency > rule.HardQuota {
		return ResolvedRule{}, fmt.Errorf("fairness rule %q reserved_concurrency must be <= hard_quota", bucket)
	}

	return ResolvedRule{
		Bucket:              bucket,
		Weight:              rule.Weight,
		ReservedConcurrency: rule.ReservedConcurrency,
		SoftQuota:           rule.SoftQuota,
		HardQuota:           rule.HardQuota,
		Burst:               rule.Burst,
	}, nil
}
