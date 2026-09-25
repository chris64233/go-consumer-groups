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

// recordingStore 包装 Store，记录每次 Save 时每个组的 (generation, owners)，
// 用于在并发测试中回放并校验分配版本序列。
type recordingStore struct {
	inner Store
	mu    sync.Mutex
	saves []map[string]struct {
		generation int64
		owners     []string
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
		members    map[string]bool
	}, len(snap.Groups))
	for _, g := range snap.Groups {
		owners := append([]string(nil), g.Assignment.Owners...)
		members := make(map[string]bool, len(g.Members))
		for _, m := range g.Members {
			members[m.ID] = true
		}
		rec[g.Name] = struct {
			generation int64
			owners     []string
			members    map[string]bool
		}{g.Generation, owners, members}
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
	members    map[string]bool
} {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]map[string]struct {
		generation int64
		owners     []string
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
