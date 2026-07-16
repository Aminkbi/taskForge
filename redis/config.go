package redis

import (
	"context"
	"fmt"

	"github.com/aminkbi/taskforge"
)

// OptionsFromConfig compiles TaskForge's supported product configuration into
// Redis broker options. Connection, logging, state-store, and routing fields in
// base are preserved; product-control fields come from config.
func OptionsFromConfig(base Options, config taskforge.Config) (Options, error) {
	normalized, err := config.Normalize()
	if err != nil {
		return Options{}, fmt.Errorf("redis options: %w", err)
	}

	base.LeaseTTL = normalized.LeaseTTL
	base.Retention = *normalized.Retention
	base.FairnessPolicies = make(map[string]*FairnessPolicy)
	base.AdmissionPolicies = make(map[string]AdmissionPolicy)
	for _, pool := range normalized.WorkerPools {
		if pool.Fairness != nil {
			defaultRule := redisFairnessRule(pool.Fairness.Default)
			rules := make([]FairnessRule, len(pool.Fairness.Rules))
			for i, rule := range pool.Fairness.Rules {
				rules[i] = redisFairnessRule(rule)
			}
			policy, err := NewFairnessPolicy(defaultRule, rules)
			if err != nil {
				return Options{}, fmt.Errorf("redis options: worker pool %q fairness: %w", pool.Name, err)
			}
			base.FairnessPolicies[pool.Queue] = policy
		}
		if pool.Admission.Mode != taskforge.AdmissionDisabled {
			base.AdmissionPolicies[pool.Queue] = AdmissionPolicy{
				Mode:                     AdmissionMode(pool.Admission.Mode),
				MaxPending:               pool.Admission.MaxPending,
				MaxPendingPerFairnessKey: pool.Admission.MaxPendingPerFairnessKey,
				MaxOldestReadyAge:        pool.Admission.MaxOldestReadyAge,
				MaxRetryBacklog:          pool.Admission.MaxRetryBacklog,
				DeferInterval:            pool.Admission.DeferInterval,
			}
		}
	}
	if len(base.FairnessPolicies) == 0 {
		base.FairnessPolicies = nil
	}
	if len(base.AdmissionPolicies) == 0 {
		base.AdmissionPolicies = nil
	}

	base.DependencyBudgets = make(map[string]int, len(normalized.DependencyBudgets))
	for _, budget := range normalized.DependencyBudgets {
		base.DependencyBudgets[budget.Name] = budget.Capacity
	}
	if len(base.DependencyBudgets) == 0 {
		base.DependencyBudgets = nil
	}
	return base, nil
}

// NewFromConfig validates config before constructing a broker.
func NewFromConfig(config taskforge.Config, base Options) (*Broker, error) {
	options, err := OptionsFromConfig(base, config)
	if err != nil {
		return nil, err
	}
	return New(options), nil
}

// OpenFromConfig validates product configuration and the Redis connection
// before returning a broker. Prefer it during application startup.
func OpenFromConfig(ctx context.Context, config taskforge.Config, base Options) (*Broker, error) {
	options, err := OptionsFromConfig(base, config)
	if err != nil {
		return nil, err
	}
	return Open(ctx, options)
}

func redisFairnessRule(rule taskforge.FairnessRule) FairnessRule {
	return FairnessRule{
		Name:                rule.Name,
		Keys:                rule.Keys,
		Weight:              rule.Weight,
		ReservedConcurrency: rule.ReservedConcurrency,
		HardQuota:           rule.HardQuota,
	}
}
