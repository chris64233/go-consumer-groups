package consumergroups

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// newStaticGroup 构造一个带显式会话超时与静态保留期的组。
func newStaticGroup(t *testing.T, partitions int, sessionTimeout, retention time.Duration) (*Coordinator, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	c, err := NewCoordinator(nil, clk)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateGroup(CreateGroupOptions{
		Name: "g", Partitions: partitions,
		SessionTimeout: sessionTimeout, StaticRetention: retention,
	}); err != nil {
		t.Fatal(err)
	}
	return c, clk
}

func mustJoinStatic(t *testing.T, c *Coordinator, group, id string, sessionVersion int64) JoinResult {
	t.Helper()
	res, err := c.JoinMember(JoinOptions{
		Group: group, MemberID: id, Static: true, SessionVersion: sessionVersion,
	})
	if err != nil {
		t.Fatalf("JoinMember(static %q v%d): %v", id, sessionVersion, err)
	}
	return res
}

func findStatic(st GroupStatus, id string) StaticInstanceStatus {
	for _, s := range st.StaticInstances {
		if s.InstanceID == id {
			return s
		}
	}
	return StaticInstanceStatus{}
}

// settleStatic 与 settle 相同，但确认时回传每个成员的当前会话版本，
// 因此可以驱动含静态成员的组收敛到 stable。
func settleStatic(t *testing.T, c *Coordinator, group string) {
	t.Helper()
	for {
		st, err := c.Status(group)
		if err != nil {
			t.Fatal(err)
		}
		if st.Phase == PhaseStable {
			return
		}
		gen := st.Generation
		sess := make(map[string]int64, len(st.Members))
		for _, m := range st.Members {
			sess[m.ID] = m.SessionVersion
		}
		pending := make(map[string][]int, len(st.PendingRevocations))
		for id, ps := range st.PendingRevocations {
			pending[id] = append([]int(nil), ps...)
		}
		for id, ps := range pending {
			if _, err := c.AckRevocation(group, RevocationAckRequest{
				MemberID: id, Generation: gen, SessionVersion: sess[id], Partitions: ps,
			}); err != nil {
				t.Fatalf("settle ack member=%q sess=%d %v: %v", id, sess[id], ps, err)
			}
		}
	}
}

// keepAlive 给动态成员补发当前版本心跳，使超时扫描只影响目标静态实例。
func keepAlive(t *testing.T, c *Coordinator, group string, gen int64, members ...string) {
	t.Helper()
	for _, id := range members {
		if _, err := c.Heartbeat(group, id, gen); err != nil {
			t.Fatalf("keepAlive(%q): %v", id, err)
		}
	}
}

// ackSess 带会话版本确认撤销。
func ackSess(t *testing.T, c *Coordinator, group, member string, gen, sess int64, partitions []int) RevocationAckResult {
	t.Helper()
	res, err := c.AckRevocation(group, RevocationAckRequest{
		MemberID: member, Generation: gen, SessionVersion: sess, Partitions: partitions,
	})
	if err != nil {
		t.Fatalf("AckRevocation(%q,%q,gen=%d,sess=%d,%v): %v", group, member, gen, sess, partitions, err)
	}
	return res
}

// ---- 会话版本：首次加入分配版本、重连递增、旧会话栅栏 ----

func TestStaticJoinAssignsSessionVersion(t *testing.T) {
	c, _ := newStaticGroup(t, 4, time.Hour, time.Hour)

	// 首次加入未提供版本 => 协调器分配初始会话版本 1，并正常触发一次再均衡。
	j := mustJoinStatic(t, c, "g", "s1", 0)
	if j.SessionVersion != 1 || j.Generation != 1 || j.Reconnected {
		t.Fatalf("first static join = %+v", j)
	}
	// 显式提供更高初始版本也被尊重。
	j2 := mustJoinStatic(t, c, "g", "s2", 42)
	if j2.SessionVersion != 42 {
		t.Fatalf("explicit session version = %d, want 42", j2.SessionVersion)
	}
}

// 保留期内以更高会话版本重连：分配版本不变、分区不转移、Reconnected=true。
func TestStaticReconnectWithinRetentionKeepsAssignment(t *testing.T) {
	c, clk := newStaticGroup(t, 4, 10*time.Second, time.Minute)
	j := mustJoinStatic(t, c, "g", "s1", 0) // gen1 stable [s1 s1 s1 s1], session 1
	if j.Generation != 1 {
		t.Fatalf("gen = %d", j.Generation)
	}

	// 会话超时：静态实例转入离线保留期，但不推进分配版本。
	clk.Advance(11 * time.Second)
	removed, err := c.ExpireGroup("g", clk.Now())
	if err != nil || removed != nil {
		t.Fatalf("expire static -> retention only, removed=%v err=%v", removed, err)
	}
	st, _ := c.Status("g")
	if st.Generation != 1 || st.Phase != PhaseStable {
		t.Fatalf("static offline must not bump generation: gen=%d phase=%s", st.Generation, st.Phase)
	}
	s := findStatic(st, "s1")
	if s.Online || s.SessionVersion != 1 || !equalInts(s.OwnedPartitions, []int{0, 1, 2, 3}) {
		t.Fatalf("offline static status = %+v", s)
	}
	if len(st.PendingTransfers) != 1 || st.PendingTransfers[0].InstanceID != "s1" ||
		!equalInts(st.PendingTransfers[0].Partitions, []int{0, 1, 2, 3}) {
		t.Fatalf("pending transfers = %+v", st.PendingTransfers)
	}
	if !s.RetainUntil.Equal(clk.Now().Add(time.Minute)) {
		t.Fatalf("retain until = %v, want %v", s.RetainUntil, clk.Now().Add(time.Minute))
	}

	// 保留期内以更高会话版本重连：继续原分配，版本不变。
	clk.Advance(30 * time.Second)
	rj, err := c.JoinMember(JoinOptions{Group: "g", MemberID: "s1", Static: true, SessionVersion: 2})
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if !rj.Reconnected || rj.Generation != 1 || rj.SessionVersion != 2 ||
		!equalStrings(rj.Assignment.Owners, []string{"s1", "s1", "s1", "s1"}) {
		t.Fatalf("reconnect result = %+v", rj)
	}
	st2, _ := c.Status("g")
	s2 := findStatic(st2, "s1")
	if !s2.Online || s2.SessionVersion != 2 || !s2.RetainUntil.IsZero() {
		t.Fatalf("reconnected static status = %+v", s2)
	}
	if len(st2.PendingTransfers) != 0 {
		t.Fatalf("pending transfers should clear after reconnect: %+v", st2.PendingTransfers)
	}
}

// 旧进程（更小会话版本）的心跳、撤销确认、位点提交全部被栅栏拒绝。
func TestStaticOldSessionFencedAfterTakeover(t *testing.T) {
	// 6 分区：s1 静态先全持有，b 在线加入造成 s1 对部分分区有撤销义务。
	c, _ := newStaticGroup(t, 6, time.Hour, time.Hour)
	mustJoinStatic(t, c, "g", "s1", 0) // gen1 stable [s1*6], session1
	mustJoin(t, c, "g", "b")           // gen2 revoking: target [b s1 b s1 b s1]
	st, _ := c.Status("g")
	if st.Phase != PhaseRevoking || !equalInts(st.PendingRevocations["s1"], []int{0, 2, 4}) {
		t.Fatalf("setup pending = %+v", st.PendingRevocations)
	}

	// 新进程在线接管（更高会话版本），不推进分配版本。
	takeover := mustJoinStatic(t, c, "g", "s1", 7)
	if !takeover.Reconnected || takeover.SessionVersion != 7 || takeover.Generation != 2 {
		t.Fatalf("takeover = %+v", takeover)
	}

	// 旧会话 v1 心跳：会话栅栏拒绝（即使携带正确分配版本）。
	if _, err := c.HeartbeatSession("g", HeartbeatRequest{
		MemberID: "s1", Generation: 2, SessionVersion: 1,
	}); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("old session heartbeat should be fenced, got %v", err)
	}
	// 旧会话确认当前版本撤销集合：必须拒绝，不能确认新版本撤销集合。
	if _, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "s1", Generation: 2, SessionVersion: 1, Partitions: []int{0, 2, 4},
	}); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("old session ack should be fenced, got %v", err)
	}
	// 旧会话提交位点：栅栏拒绝。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "s1", Generation: 2, SessionVersion: 1,
		Partition: 0, Offset: 100, RequestID: "old-1",
	}); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("old session commit should be fenced, got %v", err)
	}

	// 新会话 v7 一切正常：心跳、提交、确认。
	if hb, err := c.HeartbeatSession("g", HeartbeatRequest{
		MemberID: "s1", Generation: 2, SessionVersion: 7,
	}); err != nil || hb.SessionVersion != 7 || !equalInts(hb.Revoking, []int{0, 2, 4}) {
		t.Fatalf("new session heartbeat = %+v, %v", hb, err)
	}
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "s1", Generation: 2, SessionVersion: 7,
		Partition: 1, Offset: 50, RequestID: "new-1",
	}); err != nil {
		t.Fatalf("new session commit: %v", err)
	}
	ack, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "s1", Generation: 2, SessionVersion: 7, Partitions: []int{0, 2, 4},
	})
	if err != nil || !ack.CompletedRebalance {
		t.Fatalf("new session ack = %+v, %v", ack, err)
	}
}

// 同一实例两个进程并发（或先后）加入：只有会话版本更高者成为当前会话，
// 相同/更低版本立即收到 ErrStaleSession。
func TestStaticConcurrentJoinOnlyOneSession(t *testing.T) {
	c, _ := newStaticGroup(t, 2, time.Hour, time.Hour)
	mustJoinStatic(t, c, "g", "s1", 5)

	// 更低 / 相同版本不能抢占。
	for _, v := range []int64{4, 5} {
		_, err := c.JoinMember(JoinOptions{Group: "g", MemberID: "s1", Static: true, SessionVersion: v})
		if !errors.Is(err, ErrStaleSession) {
			t.Fatalf("join with session %d should be fenced, got %v", v, err)
		}
	}
	// 更高版本接管成功。
	r := mustJoinStatic(t, c, "g", "s1", 6)
	if r.SessionVersion != 6 {
		t.Fatalf("takeover session = %d", r.SessionVersion)
	}
	st, _ := c.Status("g")
	if s := findStatic(st, "s1"); !s.Online || s.SessionVersion != 6 {
		t.Fatalf("current session = %+v", s)
	}

	// 动态/静态身份冲突：同一 ID 不能以另一种身份加入。
	if _, err := c.Join("g", "s1"); !errors.Is(err, ErrStaticIdentityConflict) {
		t.Fatalf("dynamic join over static id should conflict, got %v", err)
	}
	if _, err := c.JoinMember(JoinOptions{
		Group: "g", MemberID: "d1", Static: false,
	}); err != nil { // 先建动态成员
		t.Fatal(err)
	}
	if _, err := c.JoinMember(JoinOptions{
		Group: "g", MemberID: "d1", Static: true, SessionVersion: 1,
	}); !errors.Is(err, ErrStaticIdentityConflict) {
		t.Fatalf("static join over dynamic id should conflict, got %v", err)
	}
}

// ---- 保留期清退与整批转移 ----

// 超过保留期仍未重连：实例被清退并触发再均衡；其整批分区只交给一个新所有者。
func TestStaticRetentionExpiryReassignsBatchToSingleOwner(t *testing.T) {
	// 8 分区：s1 静态先全持有；a、b 在线加入后，s1 仍持有若干分区。
	c, clk := newStaticGroup(t, 8, 10*time.Second, time.Minute)
	mustJoinStatic(t, c, "g", "s1", 0) // gen1 [s1*8]
	settleStatic(t, c, "g")
	mustJoin(t, c, "g", "a") // gen2
	settleStatic(t, c, "g")
	mustJoin(t, c, "g", "b") // gen3
	settleStatic(t, c, "g")
	st, _ := c.Status("g")
	var s1Parts []int
	for p, owner := range st.Assignment.Owners {
		if owner == "s1" {
			s1Parts = append(s1Parts, p)
		}
	}
	if len(s1Parts) == 0 {
		t.Fatalf("test setup: s1 should own some partitions, owners=%v", st.Assignment.Owners)
	}
	baseGen := st.Generation

	// s1 断线进入保留期：版本不变。扫描前给动态成员 a/b 补心跳，确保只有 s1 断线。
	clk.Advance(11 * time.Second)
	keepAlive(t, c, "g", baseGen, "a", "b")
	if removed, err := c.ExpireGroup("g", clk.Now()); err != nil || removed != nil {
		t.Fatalf("enter retention: removed=%v err=%v", removed, err)
	}
	// 保留期内扫描：仍保留，分区不动。
	clk.Advance(30 * time.Second)
	keepAlive(t, c, "g", baseGen, "a", "b")
	if removed, err := c.ExpireGroup("g", clk.Now()); err != nil || removed != nil {
		t.Fatalf("within retention: removed=%v err=%v", removed, err)
	}
	st2, _ := c.Status("g")
	if st2.Generation != baseGen {
		t.Fatalf("generation changed during retention: %d", st2.Generation)
	}
	for _, p := range s1Parts {
		if st2.Assignment.Owners[p] != "s1" {
			t.Fatalf("partition %d reassigned during retention, owners=%v", p, st2.Assignment.Owners)
		}
	}

	// 超过保留期：清退，整批分区转移；全部进同一个新所有者。
	clk.Advance(31 * time.Second)
	keepAlive(t, c, "g", baseGen, "a", "b")
	removed, err := c.ExpireGroup("g", clk.Now())
	if err != nil || len(removed) != 1 || removed[0] != "s1" {
		t.Fatalf("retention expiry removed=%v err=%v", removed, err)
	}
	st3, _ := c.Status("g")
	if st3.Generation != baseGen+1 {
		t.Fatalf("generation after static eviction = %d, want %d", st3.Generation, baseGen+1)
	}
	dests := make(map[string]bool)
	for _, p := range s1Parts {
		dests[st3.Assignment.Owners[p]] = true
	}
	if len(dests) != 1 {
		t.Fatalf("static batch %v split across owners: %v (owners=%v)", s1Parts, dests, st3.Assignment.Owners)
	}
	if dests[""] {
		t.Fatalf("batch must transfer to a live owner, not unowned: %v", st3.Assignment.Owners)
	}
	if _, stillMember := st3Member(st3, "s1"); stillMember {
		t.Fatalf("evicted static instance should be gone from members")
	}
	if len(st3.StaticInstances) != 0 || len(st3.PendingTransfers) != 0 {
		t.Fatalf("evicted instance should not appear in static views: %+v / %+v",
			st3.StaticInstances, st3.PendingTransfers)
	}
}

func st3Member(st GroupStatus, id string) (Member, bool) {
	for _, m := range st.Members {
		if m.ID == id {
			return m, true
		}
	}
	return Member{}, false
}

// 主动离开 / 管理员移除静态成员：立即清退（不等保留期），整批只给一个新所有者。
func TestStaticLeaveAndAdminRemoveImmediateBatchEviction(t *testing.T) {
	for _, admin := range []bool{false, true} {
		t.Run(fmt.Sprintf("admin=%v", admin), func(t *testing.T) {
			c, _ := newStaticGroup(t, 8, time.Hour, time.Hour)
			mustJoinStatic(t, c, "g", "s1", 0)
			mustJoin(t, c, "g", "a")
			mustJoin(t, c, "g", "b")
			settleStatic(t, c, "g")
			st, _ := c.Status("g")
			var batch []int
			for p, owner := range st.Assignment.Owners {
				if owner == "s1" {
					batch = append(batch, p)
				}
			}
			gen := st.Generation

			if admin {
				if err := c.RemoveMember("g", "s1"); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := c.Leave("g", "s1"); err != nil {
					t.Fatal(err)
				}
			}
			after, _ := c.Status("g")
			if after.Generation != gen+1 {
				t.Fatalf("generation = %d, want %d", after.Generation, gen+1)
			}
			dests := make(map[string]bool)
			for _, p := range batch {
				dests[after.Assignment.Owners[p]] = true
			}
			if len(dests) != 1 || dests[""] {
				t.Fatalf("batch %v not transferred to single live owner: %v", batch, after.Assignment.Owners)
			}
		})
	}
}

// ---- 断线前在途撤销：重连后新会话确认，旧会话不能确认 ----

func TestStaticReconnectAcksPendingRevocation(t *testing.T) {
	c, clk := newStaticGroup(t, 4, 10*time.Second, time.Minute)
	mustJoinStatic(t, c, "g", "s1", 0) // gen1 [s1*4] sess1
	mustJoin(t, c, "g", "b")           // gen2 revoking target [s1 b s1 b], s1 须撤销 {1,3}

	// s1 断线进入保留期（撤销义务挂起，版本停在 gen2）。
	clk.Advance(11 * time.Second)
	keepAlive(t, c, "g", 2, "b")
	if _, err := c.ExpireGroup("g", clk.Now()); err != nil {
		t.Fatal(err)
	}
	st, _ := c.Status("g")
	if st.Generation != 2 || !equalInts(st.PendingRevocations["s1"], []int{0, 2}) {
		t.Fatalf("obligation should survive offline: gen=%d pending=%+v",
			st.Generation, st.PendingRevocations)
	}
	// 离线期间任何会话的确认都被拒（Offline=true 的栅栏）。
	if _, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "s1", Generation: 2, SessionVersion: 1, Partitions: []int{0, 2},
	}); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("offline ack should be fenced, got %v", err)
	}

	// 以更高会话版本重连：版本仍为 gen2，新会话确认挂起的撤销集合，版本收敛。
	rj := mustJoinStatic(t, c, "g", "s1", 2)
	if rj.Generation != 2 || rj.Phase != PhaseRevoking {
		t.Fatalf("reconnect = %+v", rj)
	}
	ack := ackSess(t, c, "g", "s1", 2, 2, []int{0, 2})
	_ = ack
	st2, _ := c.Status("g")
	if st2.Phase != PhaseStable || !equalStrings(st2.Assignment.Owners, []string{"b", "s1", "b", "s1"}) {
		t.Fatalf("after reconnect ack: phase=%s owners=%v", st2.Phase, st2.Assignment.Owners)
	}
}

// 保留期届满时若仍有挂起撤销义务：强制回收，整批立即移交，不再等待确认。
func TestStaticRetentionExpiryDuringRevocationForceReclaims(t *testing.T) {
	c, clk := newStaticGroup(t, 4, 10*time.Second, time.Minute)
	mustJoinStatic(t, c, "g", "s1", 0)
	mustJoin(t, c, "g", "b") // gen2 revoking, s1 须撤销 {1,3}
	clk.Advance(11 * time.Second)
	keepAlive(t, c, "g", 2, "b")
	if _, err := c.ExpireGroup("g", clk.Now()); err != nil {
		t.Fatal(err)
	}
	clk.Advance(61 * time.Second) // 超过保留期
	keepAlive(t, c, "g", 2, "b")
	removed, err := c.ExpireGroup("g", clk.Now())
	if err != nil || len(removed) != 1 || removed[0] != "s1" {
		t.Fatalf("expiry = %v, %v", removed, err)
	}
	st, _ := c.Status("g")
	if st.Phase != PhaseStable || !equalStrings(st.Assignment.Owners, []string{"b", "b", "b", "b"}) {
		t.Fatalf("forced reclaim owners=%v phase=%s", st.Assignment.Owners, st.Phase)
	}
}

// ---- 位点：接管 + 旧会话最终提交 + 超时扫描并发，位点不倒退 ----

func TestStaticTakeoverCommitExpiryNoOffsetRegression(t *testing.T) {
	c, clk := newStaticGroup(t, 2, 10*time.Second, time.Minute)
	mustJoinStatic(t, c, "g", "s1", 0) // gen1, s1 持有 0、1, sess1

	// 旧会话先提交最终位点 1000（接管前落地）。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "s1", Generation: 1, SessionVersion: 1,
		Partition: 0, Offset: 1000, RequestID: "final-1000",
	}); err != nil {
		t.Fatal(err)
	}

	// 新会话 v2 接管。幂等记录按实例身份保留：新会话用同号同位点重放安全。
	mustJoinStatic(t, c, "g", "s1", 2)
	replay, err := c.CommitOffset("g", CommitRequest{
		MemberID: "s1", Generation: 1, SessionVersion: 2,
		Partition: 0, Offset: 1000, RequestID: "final-1000",
	})
	if err != nil || !replay.Replayed || replay.Offset != 1000 {
		t.Fatalf("cross-session replay = %+v, %v", replay, err)
	}
	// 新会话若从更旧位置提交 500：单调性拒绝，位点不倒退。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "s1", Generation: 1, SessionVersion: 2,
		Partition: 0, Offset: 500, RequestID: "stale-pos",
	}); !errors.Is(err, ErrOffsetBacktrack) {
		t.Fatalf("lower offset should be ErrOffsetBacktrack, got %v", err)
	}
	// 旧会话 v1 在接管后提交更大的位点也不行（会话栅栏先于位点比较）。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "s1", Generation: 1, SessionVersion: 1,
		Partition: 0, Offset: 2000, RequestID: "old-late",
	}); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("old session larger commit still fenced, got %v", err)
	}

	// 超时扫描把实例转入离线保留期后，任何提交都被拒；位点仍为 1000。
	clk.Advance(11 * time.Second)
	if _, err := c.ExpireGroup("g", clk.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "s1", Generation: 1, SessionVersion: 1,
		Partition: 0, Offset: 3000, RequestID: "after-offline",
	}); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("commit while offline should be fenced, got %v", err)
	}
	st, _ := c.Status("g")
	if st.Offsets[0].Offset != 1000 {
		t.Fatalf("offset regressed/moved: %d", st.Offsets[0].Offset)
	}
}

// ---- 持久化：静态身份/会话版本/保留期/所有权关系跨重启 ----

func TestStaticStatePersistenceRoundTrip(t *testing.T) {
	store := NewMemoryStore()
	clk := newFakeClock()
	c, err := NewCoordinator(store, clk)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateGroup(CreateGroupOptions{
		Name: "g", Partitions: 4, SessionTimeout: 10 * time.Second, StaticRetention: 2 * time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	mustJoinStatic(t, c, "g", "s1", 0)
	mustJoin(t, c, "g", "b") // gen2 revoking: s1 须撤销 {1,3}

	// s1 断线进入保留期，状态落盘后重建。
	clk.Advance(11 * time.Second)
	keepAlive(t, c, "g", 2, "b")
	if _, err := c.ExpireGroup("g", clk.Now()); err != nil {
		t.Fatal(err)
	}
	c2, err := NewCoordinator(store, clk)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := c2.Status("g")
	if st.Generation != 2 || st.Phase != PhaseRevoking {
		t.Fatalf("recovered gen=%d phase=%s", st.Generation, st.Phase)
	}
	s := findStatic(st, "s1")
	if s.Online || s.SessionVersion != 1 || !s.RetainUntil.Equal(clk.Now().Add(2*time.Minute)) {
		t.Fatalf("recovered static = %+v", s)
	}
	if !equalInts(s.OwnedPartitions, []int{0, 1, 2, 3}) {
		t.Fatalf("recovered owned = %v", s.OwnedPartitions)
	}
	if len(st.PendingTransfers) != 1 || !equalInts(st.PendingTransfers[0].Partitions, []int{0, 1, 2, 3}) {
		t.Fatalf("recovered pending transfers = %+v", st.PendingTransfers)
	}

	// 重建后保留期内重连（更高会话版本）并继续未完成的协作式再均衡。
	rj := mustJoinStatic(t, c2, "g", "s1", 9)
	if rj.Generation != 2 || !rj.Reconnected {
		t.Fatalf("reconnect after restart = %+v", rj)
	}
	ack := ackSess(t, c2, "g", "s1", 2, 9, []int{0, 2})
	_ = ack
	st2, _ := c2.Status("g")
	if st2.Phase != PhaseStable || !equalStrings(st2.Assignment.Owners, []string{"b", "s1", "b", "s1"}) {
		t.Fatalf("continued rebalance after restart: phase=%s owners=%v",
			st2.Phase, st2.Assignment.Owners)
	}

	// 再次重启：在线静态身份与会话版本保持。
	c3, err := NewCoordinator(store, clk)
	if err != nil {
		t.Fatal(err)
	}
	st3, _ := c3.Status("g")
	s3 := findStatic(st3, "s1")
	if !s3.Online || s3.SessionVersion != 9 {
		t.Fatalf("static session after second restart = %+v", s3)
	}
}

// ---- 并发：接管/旧会话提交/超时扫描/心跳相撞，状态机与会话栅栏不被破坏 ----

func TestStaticConcurrentTakeoverCommitExpire(t *testing.T) {
	c, _ := newStaticGroup(t, 12, time.Hour, time.Hour)
	mustJoinStatic(t, c, "g", "s1", 1)
	for i := 2; i <= 4; i++ {
		mustJoin(t, c, "g", fmt.Sprintf("b%d", i))
	}
	gen := stGen(c)

	// 新会话以更高版本接管，成为唯一当前会话。
	takeover := mustJoinStatic(t, c, "g", "s1", 10)
	if !takeover.Reconnected || takeover.SessionVersion != 10 {
		t.Fatalf("takeover = %+v", takeover)
	}

	var wg sync.WaitGroup
	// 当前会话 v10 并发提交一系列**单调递增**位点（不同请求号）：
	// 锁内串行使最大位点 1010 必然成为最终值，较小迟到值被单调性拒绝，位点不倒退。
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = c.CommitOffset("g", CommitRequest{
				MemberID: "s1", Generation: gen, SessionVersion: 10,
				Partition: 0, Offset: int64(1000 + i), RequestID: fmt.Sprintf("cur-%d", i),
			})
		}(i)
	}
	// 旧会话 v1 并发心跳/提交/撤销确认：全部被会话栅栏拒绝，位点不被它改写。
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = c.HeartbeatSession("g", HeartbeatRequest{
				MemberID: "s1", Generation: gen, SessionVersion: 1,
			})
			_, _ = c.CommitOffset("g", CommitRequest{
				MemberID: "s1", Generation: gen, SessionVersion: 1,
				Partition: 0, Offset: int64(1 + i), RequestID: fmt.Sprintf("old-%d", i),
			})
			_, _ = c.AckRevocation("g", RevocationAckRequest{
				MemberID: "s1", Generation: gen, SessionVersion: 1,
			})
		}(i)
	}
	// 旁观者反复查询。
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Status("g")
		}()
	}
	wg.Wait()

	st, _ := c.Status("g")
	s := findStatic(st, "s1")
	if !s.Online || s.SessionVersion != 10 {
		t.Fatalf("final session = %+v, want online v10", s)
	}
	// 最大合法位点必然保留；旧会话无法把位点改回小值。
	if st.Offsets[0].Offset != 1011 {
		t.Fatalf("final offset = %d, want 1011", st.Offsets[0].Offset)
	}
}

func stGen(c *Coordinator) int64 {
	st, err := c.Status("g")
	if err != nil {
		return 0
	}
	return st.Generation
}

// 保留期内有新成员加入：离线静态成员的分区被锚定，绝不参与重排；
// 新成员只能分到其余分区。重连后静态成员的原分区原样还在。
func TestStaticOfflinePartitionsAnchoredAgainstNewJoins(t *testing.T) {
	c, clk := newStaticGroup(t, 6, 10*time.Second, time.Minute)
	mustJoinStatic(t, c, "g", "s1", 0) // gen1 [s1*6]
	mustJoin(t, c, "g", "b")           // gen2
	settleStatic(t, c, "g")
	st, _ := c.Status("g")
	var s1Parts []int
	for p, owner := range st.Assignment.Owners {
		if owner == "s1" {
			s1Parts = append(s1Parts, p)
		}
	}
	gen := st.Generation

	// s1 离线进入保留期。
	clk.Advance(11 * time.Second)
	keepAlive(t, c, "g", gen, "b")
	if _, err := c.ExpireGroup("g", clk.Now()); err != nil {
		t.Fatal(err)
	}
	// 保留期内新成员 c 加入：触发再均衡（gen+1），但 s1 的分区必须原样锚定。
	jc := mustJoin(t, c, "g", "c")
	if jc.Generation != gen+1 {
		t.Fatalf("join gen = %d, want %d", jc.Generation, gen+1)
	}
	st2, _ := c.Status("g")
	for _, p := range s1Parts {
		if st2.Assignment.Owners[p] != "s1" || st2.TargetAssignment.Owners[p] != "s1" {
			t.Fatalf("offline static partition %d not anchored: eff=%q tgt=%q",
				p, st2.Assignment.Owners[p], st2.TargetAssignment.Owners[p])
		}
	}
	if _, hasObligation := st2.PendingRevocations["s1"]; hasObligation {
		t.Fatalf("offline static member must carry no revocation obligation: %+v",
			st2.PendingRevocations)
	}
	// c 只能拿到非 s1 的分区。
	for p, owner := range st2.TargetAssignment.Owners {
		if owner == "c" {
			for _, sp := range s1Parts {
				if p == sp {
					t.Fatalf("new member c assigned anchored partition %d", p)
				}
			}
		}
	}

	// 保留期内 s1 以更高会话版本重连：分配版本不再变化，原分区仍归 s1。
	clk.Advance(5 * time.Second)
	rj := mustJoinStatic(t, c, "g", "s1", 3)
	if !rj.Reconnected || rj.Generation != jc.Generation {
		t.Fatalf("reconnect = %+v", rj)
	}
	for _, p := range s1Parts {
		if rj.Assignment.Owners[p] != "s1" {
			t.Fatalf("partition %d not restored to s1: %v", p, rj.Assignment.Owners)
		}
	}
}

// 多个静态实例同批到期：各自整批独立转移，单实例的分区不会被拆散。
func TestStaticMultipleInstancesExpireSeparateBatches(t *testing.T) {
	c, clk := newStaticGroup(t, 12, 10*time.Second, time.Minute)
	mustJoinStatic(t, c, "g", "s1", 0)
	mustJoinStatic(t, c, "g", "s2", 0)
	mustJoin(t, c, "g", "a")
	settleStatic(t, c, "g")
	st, _ := c.Status("g")
	batches := map[string][]int{}
	for p, owner := range st.Assignment.Owners {
		if owner == "s1" || owner == "s2" {
			batches[owner] = append(batches[owner], p)
		}
	}
	if len(batches["s1"]) == 0 || len(batches["s2"]) == 0 {
		t.Fatalf("setup batches = %+v owners=%v", batches, st.Assignment.Owners)
	}
	baseGen := st.Generation

	clk.Advance(11 * time.Second)
	keepAlive(t, c, "g", baseGen, "a")
	if _, err := c.ExpireAll(clk.Now()); err != nil {
		t.Fatal(err)
	}
	clk.Advance(61 * time.Second)
	keepAlive(t, c, "g", baseGen, "a")
	changed, err := c.ExpireAll(clk.Now())
	if err != nil || len(changed) != 1 || changed[0] != "g" {
		t.Fatalf("expire = %v, %v", changed, err)
	}
	after, _ := c.Status("g")
	for id, batch := range batches {
		dests := map[string]bool{}
		for _, p := range batch {
			dests[after.Assignment.Owners[p]] = true
		}
		if len(dests) != 1 || dests[""] {
			t.Fatalf("instance %q batch %v split: %v owners=%v", id, batch, dests, after.Assignment.Owners)
		}
	}
}

// FileStore 真正落盘：静态身份/会话版本/保留期/所有权在离线保留期内写进 JSON，
// 重启读回后保留期内重连仍能继续原分配与未完成的协作式再均衡。
func TestStaticFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state", "coordinator.json")
	clk := newFakeClock()

	c, err := NewCoordinator(NewFileStore(path), clk)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateGroup(CreateGroupOptions{
		Name: "g", Partitions: 4, SessionTimeout: 10 * time.Second, StaticRetention: 90 * time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	mustJoinStatic(t, c, "g", "s1", 0)
	mustJoin(t, c, "g", "b") // gen2 revoking，s1 须撤销 {0,2}

	clk.Advance(11 * time.Second)
	keepAlive(t, c, "g", 2, "b")
	if _, err := c.ExpireGroup("g", clk.Now()); err != nil {
		t.Fatal(err)
	}

	// JSON 中应包含静态字段与保留期。
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"\"static\": true",
		"\"online\": false",
		"\"session_version\": 1",
		"\"static_retention_ns\": 90000000000",
	} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("state file missing %q\n%s", want, data)
		}
	}

	// 重启恢复离线保留态，再以更高会话版本重连并完成撤销。
	c2, err := NewCoordinator(NewFileStore(path), clk)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := c2.Status("g")
	if st.Generation != 2 || st.Phase != PhaseRevoking {
		t.Fatalf("recovered gen=%d phase=%s", st.Generation, st.Phase)
	}
	s := findStatic(st, "s1")
	if s.Online || s.SessionVersion != 1 || !s.RetainUntil.Equal(clk.Now().Add(90*time.Second)) {
		t.Fatalf("recovered static = %+v", s)
	}
	rj := mustJoinStatic(t, c2, "g", "s1", 5)
	if !rj.Reconnected || rj.Generation != 2 {
		t.Fatalf("reconnect = %+v", rj)
	}
	ack := ackSess(t, c2, "g", "s1", 2, 5, []int{0, 2})
	if !ack.CompletedRebalance {
		t.Fatalf("ack = %+v", ack)
	}
}
