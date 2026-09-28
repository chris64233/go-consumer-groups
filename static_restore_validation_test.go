package consumergroups

import (
	"strings"
	"testing"
	"time"
)

// 直接构造自相矛盾的静态成员持久化快照，验证 restore 会拒绝它们。
func TestRestoreRejectsCorruptStaticState(t *testing.T) {
	base := func() Snapshot {
		return Snapshot{Groups: []GroupSnapshot{{
			Name:       "g",
			Partitions: 4,
			Generation: 2,
			Phase:      PhaseRevoking,
			Leader:     "d2",
			Members: []MemberSnapshot{
				{ID: "d2", JoinedAt: time.Unix(0, 0), LastHeartbeatAt: time.Unix(0, 0)},
				{ID: "s1s", Static: true, Instance: "s1", SessionVersion: 1,
					JoinedAt: time.Unix(0, 0), LastHeartbeatAt: time.Unix(0, 0)},
			},
			StaticInstances: []StaticInstanceSnapshot{{
				ID: "s1", JoinedAt: time.Unix(0, 0), Retention: time.Hour,
				SessionVersion: 1, Online: true, SessionID: "s1s",
				LastHeartbeatAt: time.Unix(0, 0),
			}},
			// online=[d2 s1]：target=[d2 s1 d2 s1]，在途 p0、p2 由 s1 待撤销。
			Assignment:       Assignment{Generation: 2, Owners: []string{"s1", "s1", "s1", "s1"}},
			TargetAssignment: Assignment{Generation: 2, Owners: []string{"d2", "s1", "d2", "s1"}},
			Revocations:      []RevocationSnapshot{{MemberID: "s1", Required: []int{0, 2}}},
		}}}
	}

	cases := []struct {
		name   string
		mutate func(*Snapshot)
		want   string
	}{
		{
			name: "session version mismatch",
			mutate: func(s *Snapshot) {
				s.Groups[0].Members[1].SessionVersion = 9
			},
			want: "version",
		},
		{
			name: "active session refers missing instance",
			mutate: func(s *Snapshot) {
				s.Groups[0].Members[1].Instance = "ghost"
			},
			want: "missing static instance",
		},
		{
			name: "active session id differs from instance current session",
			mutate: func(s *Snapshot) {
				s.Groups[0].StaticInstances[0].SessionID = "other"
			},
			want: "does not match",
		},
		{
			name: "offline instance has zero retain until",
			mutate: func(s *Snapshot) {
				is := &s.Groups[0].StaticInstances[0]
				is.Online = false
				is.OfflineAt = time.Unix(1, 0)
				// RetainUntil 保持零值
				s.Groups[0].Members = s.Groups[0].Members[:1] // 移除其活动会话
			},
			want: "zero retain_until",
		},
		{
			name: "offline instance still has active session",
			mutate: func(s *Snapshot) {
				is := &s.Groups[0].StaticInstances[0]
				is.Online = false
				is.OfflineAt = time.Unix(1, 0)
				is.RetainUntil = time.Unix(2, 0)
				// 活动会话 s1s 未移除
			},
			want: "instance \"s1\" is offline",
		},
		{
			name: "dead session refers missing instance",
			mutate: func(s *Snapshot) {
				s.Groups[0].DeadSessions = []DeadSessionSnapshot{
					{SessionID: "old", Instance: "ghost", Version: 1},
				}
			},
			want: "missing instance",
		},
		{
			name: "dead session is also active",
			mutate: func(s *Snapshot) {
				s.Groups[0].DeadSessions = []DeadSessionSnapshot{
					{SessionID: "s1s", Instance: "s1", Version: 1},
				}
			},
			want: "both dead and active",
		},
		{
			name: "non-positive session version",
			mutate: func(s *Snapshot) {
				s.Groups[0].StaticInstances[0].SessionVersion = 0
				s.Groups[0].Members[1].SessionVersion = 0
			},
			want: "non-positive session version",
		},
		{
			name: "non-positive retention",
			mutate: func(s *Snapshot) {
				s.Groups[0].StaticInstances[0].Retention = 0
			},
			want: "non-positive retention",
		},
		{
			name: "owner is neither member nor instance",
			mutate: func(s *Snapshot) {
				s.Groups[0].Assignment.Owners[1] = "ghost"
			},
			want: "not a member",
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
				t.Fatalf("expected corrupt static state (%s) to be rejected", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.want)
			}
		})
	}
}
