package scheduler

import "testing"

func FuzzParseLeadershipFence(f *testing.F) {
	f.Add("scheduler-a|1")
	f.Add("scheduler-a|")
	f.Add("|1")

	f.Fuzz(func(t *testing.T, token string) {
		fence, err := parseLeadershipFence(token)
		if err == nil && fence.Token != token {
			t.Fatalf("parseLeadershipFence(%q) = %+v, want matching fence token", token, fence)
		}
	})
}
