package consumergroups

import (
	"path/filepath"
	"testing"
	"time"
)

// 静态身份、会话版本、保留期限与所有权关系跨重启完整持久化：
// 在「接管后保留期内离线」状态下重启，恢复后仍能重连继续原分配。
func TestStaticStatePersistenceAndRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state", "coordinator.json")
	clk := newFakeClock()

	c, err := NewCoordinator(NewFileStore(path), clk)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateGroup(CreateGroupOptions{
		Name: "g", Partitions: 4, SessionTimeout: 10 * time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	r1 := mustStaticJoin(t, c, "g", "s1", "s1-pid-1", 30*time.Second) // gen1 全持有
	if r1.SessionVersion != 1 {
		t.Fatalf("version = %d", r1.SessionVersion)
	}
	// 新进程并发接管：会话版本升到 2（不触发再均衡）。
	r2 := mustStaticJoin(t, c, "g", "s1", "s1-pid-2", 30*time.Second)
	if r2.SessionVersion != 2 || r2.Generation != 1 {
		t.Fatalf("takeover = %+v", r2)
	}
	// 当前会话提交位点（实例级幂等记录应持久化）。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "s1-pid-2", SessionVersion: 2, Generation: 1,
		Partition: 0, Offset: 77, RequestID: "rid-77",
	}); err != nil {
		t.Fatal(err)
	}

	// 断线进入保留期离线。
	clk.Advance(11 * time.Second)
	if _, err := c.ExpireGroup("g", clk.Now()); err != nil {
		t.Fatal(err)
	}

	// 重启：用同一文件新建协调器。
	c2, err := NewCoordinator(NewFileStore(path), clk)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	st, err := c2.Status("g")
	if err != nil {
		t.Fatal(err)
	}
	if st.Generation != 1 {
		t.Fatalf("recovered generation = %d, want 1 (offline never rebalances)", st.Generation)
	}
	if len(st.StaticInstances) != 1 {
		t.Fatalf("static instances = %+v", st.StaticInstances)
	}
	is := findInstance(st, "s1")
	if is.Online || is.SessionVersion != 2 || is.SessionID != "s1-pid-2" ||
		is.RetainUntil.IsZero() || is.OfflineAt.IsZero() {
		t.Fatalf("recovered offline instance = %+v", is)
	}
	if !equalInts(is.HeldPartitions, []int{0, 1, 2, 3}) {
		t.Fatalf("recovered held partitions = %v", is.HeldPartitions)
	}
	if !equalStrings(st.Assignment.Owners, []string{"s1", "s1", "s1", "s1"}) {
		t.Fatalf("recovered owners = %v", st.Assignment.Owners)
	}
	if len(st.PendingTransfers) != 4 {
		t.Fatalf("recovered pending transfers = %+v", st.PendingTransfers)
	}
	for _, tr := range st.PendingTransfers {
		if tr.Reason != TransferReasonRetained || tr.Online {
			t.Fatalf("recovered transfer = %+v", tr)
		}
	}
	if st.Offsets[0].Offset != 77 {
		t.Fatalf("recovered offset = %d", st.Offsets[0].Offset)
	}

	// 旧进程（版本 1、2 之前的会话 s1-pid-1）在恢复后仍被栅栏。
	if _, err := c2.HeartbeatV2("g", HeartbeatRequest{
		MemberID: "s1-pid-1", Generation: 1, SessionVersion: 1,
	}); err == nil {
		t.Fatal("stale session fenced record did not survive restart")
	}

	// 保留期内重连继续原分配；实例级幂等记录跨重启重放成功。
	r3 := mustStaticJoin(t, c2, "g", "s1", "s1-pid-3", 30*time.Second)
	if r3.SessionVersion != 3 || r3.Generation != 1 || !r3.Rejoined {
		t.Fatalf("rejoin after recovery = %+v", r3)
	}
	replay, err := c2.CommitOffset("g", CommitRequest{
		MemberID: "s1-pid-3", SessionVersion: 3, Generation: 1,
		Partition: 0, Offset: 77, RequestID: "rid-77",
	})
	if err != nil || !replay.Replayed || replay.Offset != 77 {
		t.Fatalf("idempotent record did not survive restart: %+v %v", replay, err)
	}
}

// 撤销进行中（含静态义务方）重启：恢复后可在同一 generation 继续完成协作式再均衡。
func TestRestartResumesIncompleteRevocationWithStatic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	clk := newFakeClock()

	c, err := NewCoordinator(NewFileStore(path), clk)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4, SessionTimeout: time.Hour}); err != nil {
		t.Fatal(err)
	}
	mustStaticJoin(t, c, "g", "s1", "s1-1", time.Hour) // gen1 全持有
	j := mustJoin(t, c, "g", "d2")                     // gen2 revoking：s1 须撤销
	if j.Phase != PhaseRevoking {
		t.Fatalf("phase = %s, want revoking", j.Phase)
	}

	c2, err := NewCoordinator(NewFileStore(path), clk)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	st, _ := c2.Status("g")
	if st.Generation != 2 || st.Phase != PhaseRevoking {
		t.Fatalf("recovered gen=%d phase=%s", st.Generation, st.Phase)
	}
	pending := st.PendingRevocations["s1"]
	if len(pending) == 0 {
		t.Fatal("s1 revocation obligation did not survive restart")
	}
	is := findInstance(st, "s1")
	if !is.Online || is.SessionVersion != 1 || is.SessionID != "s1-1" {
		t.Fatalf("recovered static session = %+v", is)
	}
	// 恢复后由当前会话在同一 generation 确认，版本收敛。
	ack, err := c2.AckRevocation("g", RevocationAckRequest{
		MemberID: "s1-1", SessionVersion: 1, Generation: 2, Partitions: pending,
	})
	if err != nil {
		t.Fatalf("ack after recovery: %v", err)
	}
	if !ack.CompletedRebalance || ack.Phase != PhaseStable {
		t.Fatalf("ack after recovery = %+v", ack)
	}
}
