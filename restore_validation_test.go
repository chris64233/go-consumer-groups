package consumergroups

import (
	"strings"
	"testing"
)

// 直接构造自相矛盾的持久化快照，验证 restore 的跨字段一致性校验会拒绝它们，
// 而不是带着坏状态继续运行。
func TestRestoreRejectsCorruptRevocationState(t *testing.T) {
	base := func() Snapshot {
		return Snapshot{Groups: []GroupSnapshot{{
			Name:       "g",
			Partitions: 2,
			Generation: 3,
			Phase:      PhaseRevoking,
			Leader:     "a",
			Members:    []MemberSnapshot{{ID: "a"}, {ID: "b"}},
			Assignment: Assignment{Generation: 3, Owners: []string{"a", "a"}},
			// 目标：分区 1 应归 b。
			TargetAssignment: Assignment{Generation: 3, Owners: []string{"a", "b"}},
			Revocations: []RevocationSnapshot{
				{MemberID: "a", Required: []int{1}},
			},
		}}}
	}

	cases := []struct {
		name   string
		mutate func(*Snapshot)
		want   string
	}{
		{
			name: "unknown phase",
			mutate: func(s *Snapshot) {
				s.Groups[0].Phase = "weird"
			},
			want: "unknown phase",
		},
		{
			name: "in-flight owner not a member",
			mutate: func(s *Snapshot) {
				s.Groups[0].Assignment.Owners[1] = "ghost"
			},
			want: "not a member",
		},
		{
			name: "in-flight partition missing from revocation set",
			mutate: func(s *Snapshot) {
				s.Groups[0].Revocations[0].Required = []int{}
			},
			want: "in flight but missing",
		},
		{
			name: "revocation obligation for non-member",
			mutate: func(s *Snapshot) {
				s.Groups[0].Assignment.Owners = []string{"a", "b"}
				s.Groups[0].Revocations[0].MemberID = "ghost"
			},
			want: "non-member",
		},
		{
			name: "acked partition not in required set",
			mutate: func(s *Snapshot) {
				s.Groups[0].Revocations[0].Acked = []int{0}
			},
			want: "not in required set",
		},
		{
			name: "stable but owners differ from target",
			mutate: func(s *Snapshot) {
				s.Groups[0].Phase = PhaseStable
				s.Groups[0].Revocations = nil
			},
			want: "stable but effective and target owners differ",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := base()
			tc.mutate(&snap)
			store := NewMemoryStore()
			if err := store.Save(snap); err != nil {
				t.Fatal(err)
			}
			_, err := NewCoordinator(store, nil)
			if err == nil {
				t.Fatalf("expected corrupt state (%s) to be rejected", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.want)
			}
		})
	}
}
