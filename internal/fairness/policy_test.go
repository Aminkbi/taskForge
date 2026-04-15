package fairness

import "testing"

func TestPolicyResolvesExactKeysAndDefaultBucket(t *testing.T) {
	t.Parallel()

	policy, err := NewPolicy(Rule{SoftQuota: 2}, []Rule{
		{Name: "vip", Keys: []string{"tenant-vip"}, Weight: 2, ReservedConcurrency: 1},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}

	vip := policy.Resolve("tenant-vip")
	if vip.Bucket != "vip" || vip.Weight != 2 || vip.ReservedConcurrency != 1 {
		t.Fatalf("vip resolution = %+v", vip)
	}

	def := policy.Resolve("")
	if def.Bucket != "default" || def.SoftQuota != 2 {
		t.Fatalf("default resolution = %+v", def)
	}
}

func TestPolicyRejectsDuplicateKeys(t *testing.T) {
	t.Parallel()

	_, err := NewPolicy(Rule{}, []Rule{
		{Name: "first", Keys: []string{"tenant-1"}},
		{Name: "second", Keys: []string{"tenant-1"}},
	})
	if err == nil {
		t.Fatal("NewPolicy() error = nil, want non-nil")
	}
}
