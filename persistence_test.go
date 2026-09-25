package consumergroups

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 重启后状态完整恢复：版本、成员、分配、位点、幂等记录都还在。
func TestFileStorePersistenceAndRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state", "coordinator.json")
	clk := newFakeClock()

	c, err := NewCoordinator(NewFileStore(path), clk)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 3, SessionTimeout: 7 * time.Second}); err != nil {
		t.Fatal(err)
	}
	mustJoin(t, c, "g", "a") // gen 1
	mustJoin(t, c, "g", "b") // gen 2: owners [a b a]
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 0, Offset: 42, Metadata: "m0", RequestID: "req-0",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Heartbeat("g", "b", 2); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("state file should exist: %v", err)
	}

	// 模拟进程重启：用同一个文件新建协调器。
	c2, err := NewCoordinator(NewFileStore(path), clk)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	st, err := c2.Status("g")
	if err != nil {
		t.Fatal(err)
	}
	if st.Generation != 2 {
		t.Fatalf("recovered generation = %d, want 2", st.Generation)
	}
	if !equalStrings(st.Assignment.Owners, []string{"a", "b", "a"}) {
		t.Fatalf("recovered owners = %v", st.Assignment.Owners)
	}
	if len(st.Offsets) != 1 || st.Offsets[0].Offset != 42 || st.Offsets[0].Metadata != "m0" {
		t.Fatalf("recovered offsets = %+v", st.Offsets)
	}
	if len(st.Members) != 2 {
		t.Fatalf("recovered members = %v", st.Members)
	}

	// 幂等记录在恢复后仍然生效：重放同请求号返回 Replayed，改内容仍冲突。
	res, err := c2.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 0, Offset: 42, RequestID: "req-0",
	})
	if err != nil || !res.Replayed {
		t.Fatalf("replay after recovery = %+v, %v", res, err)
	}
	if _, err := c2.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 0, Offset: 43, RequestID: "req-0",
	}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflict after recovery should still be detected, got %v", err)
	}

	// 恢复后版本继续递增，不会与旧版本冲突。
	r := mustJoin(t, c2, "g", "c")
	if r.Generation != 3 {
		t.Fatalf("generation after recovery join = %d, want 3", r.Generation)
	}
}

// 空文件（首次启动）与不存在文件都应正常初始化。
func TestFileStoreEmptyAndMissing(t *testing.T) {
	dir := t.TempDir()
	c, err := NewCoordinator(NewFileStore(filepath.Join(dir, "nope.json")), nil)
	if err != nil {
		t.Fatalf("missing file should start empty: %v", err)
	}
	if got := c.Groups(); len(got) != 0 {
		t.Fatalf("expected no groups, got %v", got)
	}
}

// 损坏的状态文件必须报错而不是静默吞掉。
func TestFileStoreCorruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewCoordinator(NewFileStore(path), nil); err == nil {
		t.Fatal("corrupt state file should fail to load")
	}
}

// MemoryStore 的快照是深拷贝：保存后协调器内部变化不会污染已保存快照。
func TestMemoryStoreSnapshotIsolation(t *testing.T) {
	store := NewMemoryStore()
	c, _ := NewCoordinator(store, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 2})
	mustJoin(t, c, "g", "a")

	snap1, err := store.Load()
	if err != nil || snap1 == nil {
		t.Fatalf("load: %v", err)
	}
	gen1 := snap1.Groups[0].Generation

	mustJoin(t, c, "g", "b") // 继续演进
	if snap1.Groups[0].Generation != gen1 {
		t.Fatalf("saved snapshot mutated: generation %d -> %d", gen1, snap1.Groups[0].Generation)
	}
	if len(snap1.Groups[0].Members) != 1 {
		t.Fatalf("saved snapshot members mutated: %v", snap1.Groups[0].Members)
	}
}

// 文件内容应是整份快照（含版本号），可被外部工具读取。
func TestFileStoreWritesWholeSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	c, _ := NewCoordinator(NewFileStore(path), nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 2})
	mustJoin(t, c, "g", "a")

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("state file is empty")
	}
	// 重新加载校验往返一致。
	c2, err := NewCoordinator(NewFileStore(path), nil)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := c2.Status("g")
	if st.Generation != 1 || !equalStrings(st.Assignment.Owners, []string{"a", "a"}) {
		t.Fatalf("round-trip status = %+v", st)
	}
}
