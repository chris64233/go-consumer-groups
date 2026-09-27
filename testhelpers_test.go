package consumergroups

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeClock 是线程安全的可控时钟，供超时相关测试使用。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// recordingStore 包装 Store，记录每次 Save 时每个组的 (generation, owners, target)，
// 用于在并发测试中回放并校验分配版本序列。
type recordingStore struct {
	inner Store
	mu    sync.Mutex
	saves []map[string]struct {
		generation int64
		owners     []string
		target     []string
		members    map[string]bool
	}
}

func newRecordingStore(inner Store) *recordingStore {
	return &recordingStore{inner: inner}
}

func (s *recordingStore) Save(snap Snapshot) error {
	rec := make(map[string]struct {
		generation int64
		owners     []string
		target     []string
		members    map[string]bool
	}, len(snap.Groups))
	for _, g := range snap.Groups {
		owners := append([]string(nil), g.Assignment.Owners...)
		target := append([]string(nil), g.Target.Owners...)
		members := make(map[string]bool, len(g.Members))
		for _, m := range g.Members {
			members[m.ID] = true
		}
		rec[g.Name] = struct {
			generation int64
			owners     []string
			target     []string
			members    map[string]bool
		}{g.Generation, owners, target, members}
	}
	s.mu.Lock()
	s.saves = append(s.saves, rec)
	s.mu.Unlock()
	return s.inner.Save(snap)
}

func (s *recordingStore) Load() (*Snapshot, error) { return s.inner.Load() }

func (s *recordingStore) records() []map[string]struct {
	generation int64
	owners     []string
	target     []string
	members    map[string]bool
} {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]map[string]struct {
		generation int64
		owners     []string
		target     []string
		members    map[string]bool
	}, len(s.saves))
	copy(out, s.saves)
	return out
}

func mustJoin(t *testing.T, c *Coordinator, group, member string) JoinResult {
	t.Helper()
	res, err := c.Join(group, member)
	if err != nil {
		t.Fatalf("Join(%q, %q) unexpected error: %v", group, member, err)
	}
	return res
}

// settleRebalance 扮演行为良好的客户端：按 Status 报告的待撤销集合
// 逐成员确认撤销，直到本代再均衡完成。
func settleRebalance(t *testing.T, c *Coordinator, group string) GroupStatus {
	t.Helper()
	for range 100 {
		st, err := c.Status(group)
		if err != nil {
			t.Fatalf("Status(%q): %v", group, err)
		}
		if st.RebalanceComplete {
			return st
		}
		byMember := make(map[string][]int)
		for _, pr := range st.PendingRevocations {
			byMember[pr.From] = append(byMember[pr.From], pr.Partition)
		}
		if len(byMember) == 0 {
			t.Fatalf("rebalance incomplete but no pending revocations: %+v", st)
		}
		for member, parts := range byMember {
			if _, err := c.AcknowledgeRevocation(group, RevocationAck{
				MemberID: member, Generation: st.Generation, Partitions: parts,
			}); err != nil {
				t.Fatalf("AcknowledgeRevocation(%q, %q, %v): %v", group, member, parts, err)
			}
		}
	}
	t.Fatal("rebalance did not settle within 100 rounds")
	return GroupStatus{}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// requireError 断言 err 包装了 want，并用 errors.As 提取到 T 类型。
func requireError[T error](t *testing.T, err error, want error) T {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error wrapping %v, got nil", want)
	}
	if !errors.Is(err, want) {
		t.Fatalf("expected error wrapping %v, got %v", want, err)
	}
	var target T
	if !errors.As(err, &target) {
		t.Fatalf("expected errors.As to extract %T from %v", target, err)
	}
	return target
}
