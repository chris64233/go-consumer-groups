package consumergroups

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func mustStaticJoin(t *testing.T, c *Coordinator, group, instance, session string, retention time.Duration) StaticJoinResult {
	t.Helper()
	res, err := c.JoinStatic(group, StaticJoinOptions{
		InstanceID: instance, SessionID: session, Retention: retention,
	})
	if err != nil {
		t.Fatalf("JoinStatic(%q,%q,%q) unexpected error: %v", group, instance, session, err)
	}
	return res
}

// mustStaticAck 由静态会话（带会话版本）确认撤销并要求成功。
func mustStaticAck(t *testing.T, c *Coordinator, group, session string, version, gen int64, partitions []int) RevocationAckResult {
	t.Helper()
	res, err := c.AckRevocation(group, RevocationAckRequest{
		MemberID: session, SessionVersion: version, Generation: gen, Partitions: partitions,
	})
	if err != nil {
		t.Fatalf("static AckRevocation(session=%q v=%d gen=%d %v): %v", session, version, gen, partitions, err)
	}
	return res
}

// staticGroup 创建一个带可控时钟的组：会话超时较短、分区数固定。
func staticGroup(t *testing.T, partitions int, sessionTimeout time.Duration) (*Coordinator, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	c, err := NewCoordinator(nil, clk)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateGroup(CreateGroupOptions{
		Name: "g", Partitions: partitions, SessionTimeout: sessionTimeout,
	}); err != nil {
		t.Fatal(err)
	}
	return c, clk
}

func findInstance(st GroupStatus, id string) StaticInstanceStatus {
	for _, is := range st.StaticInstances {
		if is.InstanceID == id {
			return is
		}
	}
	return StaticInstanceStatus{}
}

// ---- 需求 1：稳定实例标识、会话版本递增、重连继续原分配、旧进程栅栏 ----

// 静态实例首次加入获得会话版本 1，像普通成员一样触发再均衡并取得分区。
func TestStaticJoinFirstTime(t *testing.T) {
	c, _ := staticGroup(t, 4, time.Hour)
	r := mustStaticJoin(t, c, "g", "s1", "s1-pid-1", time.Hour)
	if r.SessionVersion != 1 || r.Rejoined {
		t.Fatalf("first static join = %+v, want version=1 rejoined=false", r)
	}
	if r.Generation != 1 || r.Phase != PhaseStable {
		t.Fatalf("first join gen=%d phase=%s", r.Generation, r.Phase)
	}
	if !equalStrings(r.Assignment.Owners, []string{"s1", "s1", "s1", "s1"}) {
		t.Fatalf("owners = %v", r.Assignment.Owners)
	}
	st, _ := c.Status("g")
	if len(st.StaticInstances) != 1 || st.StaticInstances[0].InstanceID != "s1" ||
		!st.StaticInstances[0].Online || st.StaticInstances[0].SessionID != "s1-pid-1" {
		t.Fatalf("static instances = %+v", st.StaticInstances)
	}
}

// 参数校验：实例 ID / 会话 ID 必填、保留期必须为正。
func TestStaticJoinInvalidArgs(t *testing.T) {
	c, _ := staticGroup(t, 2, time.Hour)
	if _, err := c.JoinStatic("g", StaticJoinOptions{SessionID: "x", Retention: time.Second}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing instance: %v", err)
	}
	if _, err := c.JoinStatic("g", StaticJoinOptions{InstanceID: "s1", Retention: time.Second}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing session: %v", err)
	}
	if _, err := c.JoinStatic("g", StaticJoinOptions{InstanceID: "s1", SessionID: "x"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing retention: %v", err)
	}
}

// 会话超时后实例在保留期内仍占有分区：不推进分配版本、不转移任何分区；
// 以更高会话版本重连继续原分配（Rejoined=true，版本 +1），同样不触发再均衡。
func TestStaticReconnectWithinRetentionKeepsAssignment(t *testing.T) {
	c, clk := staticGroup(t, 4, 10*time.Second)
	r1 := mustStaticJoin(t, c, "g", "s1", "s1-old", 30*time.Second) // gen1 全持有
	before := []string{"s1", "s1", "s1", "s1"}

	// 超过会话超时（10s）但仍在保留期（断线后 30s）内。
	clk.Advance(11 * time.Second)
	removed, err := c.ExpireGroup("g", clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if removed != nil {
		t.Fatalf("temporary offline must not count as removal, got %v", removed)
	}
	st, _ := c.Status("g")
	if st.Generation != 1 {
		t.Fatalf("temporary offline must not bump generation, got %d", st.Generation)
	}
	if !equalStrings(st.Assignment.Owners, before) {
		t.Fatalf("partitions moved during retention: %v", st.Assignment.Owners)
	}
	is := findInstance(st, "s1")
	if is.Online || is.SessionVersion != 1 || is.RetainUntil.IsZero() {
		t.Fatalf("offline instance view = %+v", is)
	}
	// 待转移分区应展示为 retained（钉住，无新所有者）。
	if len(st.PendingTransfers) != 4 {
		t.Fatalf("pending transfers = %+v, want 4 retained", st.PendingTransfers)
	}
	for _, tr := range st.PendingTransfers {
		if tr.Reason != TransferReasonRetained || tr.Online || tr.TargetOwner != "" {
			t.Fatalf("retained transfer = %+v", tr)
		}
	}

	// 保留期内以新会话（更高会话版本）重连：继续原分配，不触发再均衡。
	clk.Advance(time.Second)
	r2 := mustStaticJoin(t, c, "g", "s1", "s1-new", 30*time.Second)
	if !r2.Rejoined || r2.SessionVersion != 2 {
		t.Fatalf("rejoin = %+v, want rejoined version=2", r2)
	}
	if r2.Generation != r1.Generation {
		t.Fatalf("rejoin within retention must not bump generation: %d -> %d", r1.Generation, r2.Generation)
	}
	if !equalStrings(r2.Assignment.Owners, before) {
		t.Fatalf("assignment changed after reconnect: %v", r2.Assignment.Owners)
	}
	st2, _ := c.Status("g")
	is2 := findInstance(st2, "s1")
	if !is2.Online || is2.SessionVersion != 2 || is2.SessionID != "s1-new" || !is2.RetainUntil.IsZero() {
		t.Fatalf("online instance after reconnect = %+v", is2)
	}
}

// 保留期内重连沿用同一会话 ID 也合法：旧进程仍因会话版本递增被栅栏，
// 且持久化不出现「既死又活」的矛盾。
func TestStaticReconnectSameSessionID(t *testing.T) {
	c, clk := staticGroup(t, 2, 10*time.Second)
	r1 := mustStaticJoin(t, c, "g", "s1", "s1-session", 30*time.Second)
	clk.Advance(11 * time.Second)
	if _, err := c.ExpireGroup("g", clk.Now()); err != nil {
		t.Fatal(err)
	}
	r2 := mustStaticJoin(t, c, "g", "s1", "s1-session", 30*time.Second)
	if r2.SessionVersion != r1.SessionVersion+1 {
		t.Fatalf("version = %d, want %d", r2.SessionVersion, r1.SessionVersion+1)
	}
	// 旧进程（携带版本 1）的心跳必须被栅栏。
	if _, err := c.HeartbeatV2("g", HeartbeatRequest{
		MemberID: "s1-session", Generation: r2.Generation, SessionVersion: 1,
	}); !errors.Is(err, ErrFencedSession) {
		t.Fatalf("old process heartbeat should be fenced, got %v", err)
	}
}

// 旧进程在被接管后发来的心跳、撤销确认、位点提交都必须被拒绝（ErrFencedSession）。
func TestStaleProcessOperationsFenced(t *testing.T) {
	c, _ := staticGroup(t, 4, time.Hour)
	mustStaticJoin(t, c, "g", "s1", "s1-old", time.Hour) // gen1
	mustJoin(t, c, "g", "d2")                            // gen2 revoking：s1 有撤销义务
	st, _ := c.Status("g")
	s1Pending := append([]int(nil), st.PendingRevocations["s1"]...)
	if len(s1Pending) == 0 {
		t.Fatal("expected s1 revocation obligation after d2 join")
	}

	// 新进程并发接管：会话版本升到 2，旧会话 s1-old 立即被栅栏。
	r2 := mustStaticJoin(t, c, "g", "s1", "s1-new", time.Hour)
	if r2.SessionVersion != 2 || r2.Generation != 2 {
		t.Fatalf("takeover = %+v", r2)
	}

	fence := func(name string, do func() error) {
		t.Helper()
		if err := do(); !errors.Is(err, ErrFencedSession) {
			t.Fatalf("%s by stale process = %v, want ErrFencedSession", name, err)
		}
	}
	// 旧进程心跳（伪造当前分配版本也不行——会话栅栏独立于分配版本）。
	fence("heartbeat", func() error {
		_, err := c.HeartbeatV2("g", HeartbeatRequest{
			MemberID: "s1-old", Generation: 2, SessionVersion: 1,
		})
		return err
	})
	// 旧进程确认撤销。
	fence("revocation ack", func() error {
		_, err := c.AckRevocation("g", RevocationAckRequest{
			MemberID: "s1-old", SessionVersion: 1, Generation: 2, Partitions: s1Pending,
		})
		return err
	})
	// 旧进程提交位点（它仍是分区生效所有者，但会话栅栏先于所有权栅栏）。
	fence("offset commit", func() error {
		_, err := c.CommitOffset("g", CommitRequest{
			MemberID: "s1-old", SessionVersion: 1, Generation: 2,
			Partition: s1Pending[0], Offset: 1, RequestID: "stale-1",
		})
		return err
	})

	// 新会话携带版本 2 的同类操作正常受理。
	if _, err := c.HeartbeatV2("g", HeartbeatRequest{
		MemberID: "s1-new", Generation: 2, SessionVersion: 2,
	}); err != nil {
		t.Fatalf("current session heartbeat: %v", err)
	}
	mustStaticAck(t, c, "g", "s1-new", 2, 2, s1Pending)
}

// 临时离线后、重连前的旧会话操作同样被栅栏（会话已不在活动集合中）。
func TestOfflineOldSessionFencedBeforeRejoin(t *testing.T) {
	c, clk := staticGroup(t, 2, 10*time.Second)
	r := mustStaticJoin(t, c, "g", "s1", "s1-old", 30*time.Second)
	clk.Advance(11 * time.Second)
	if _, err := c.ExpireGroup("g", clk.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.HeartbeatV2("g", HeartbeatRequest{
		MemberID: "s1-old", Generation: r.Generation, SessionVersion: 1,
	}); !errors.Is(err, ErrFencedSession) {
		t.Fatalf("offline session heartbeat = %v, want ErrFencedSession", err)
	}
}

// ---- 需求 2：暂时离线 vs 真正退出；整批只给一个新所有者 ----

// 保留期届满后仍未重连：实例被真正清退并纳入协作式再均衡，
// 其原持有分区作为一整批只移交给唯一后继。
func TestStaticRetentionExpiryBatchToSingleSuccessor(t *testing.T) {
	c, clk := staticGroup(t, 8, 10*time.Second)

	// s1(静态) + d2 + d3 稳定共存。在线主体字典序 [d2 d3 s1]，
	// target=[d2 d3 s1 d2 d3 s1 d2 d3]，s1={2,5}。
	mustStaticJoin(t, c, "g", "s1", "s1-1", time.Hour)
	j2 := mustJoin(t, c, "g", "d2")
	st, _ := c.Status("g")
	mustStaticAck(t, c, "g", "s1-1", 1, j2.Generation, st.PendingRevocations["s1"])
	j3 := mustJoin(t, c, "g", "d3")
	st, _ = c.Status("g")
	// gen3 中 s1 与 d2 都有撤销义务，需分别确认（s1 带会话版本，d2 为动态成员）。
	s1Pending := append([]int(nil), st.PendingRevocations["s1"]...)
	d2Pending := append([]int(nil), st.PendingRevocations["d2"]...)
	if _, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "s1-1", SessionVersion: 1, Generation: j3.Generation, Partitions: s1Pending,
	}); err != nil {
		t.Fatal(err)
	}
	mustAck(t, c, "g", "d2", j3.Generation, d2Pending)
	st, _ = c.Status("g")
	if st.Phase != PhaseStable {
		t.Fatalf("setup phase = %s", st.Phase)
	}
	s1Held := findInstance(st, "s1").HeldPartitions
	if !equalInts(s1Held, []int{2, 5}) {
		t.Fatalf("s1 held = %v", s1Held)
	}

	// 推进到 s1 会话超时（10s）之后，先让 d2/d3 在此刻续心跳，
	// 再扫描：确保只有停止心跳的 s1 被判超时离线。
	clk.Advance(11 * time.Second)
	if _, err := c.Heartbeat("g", "d2", 3); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Heartbeat("g", "d3", 3); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExpireGroup("g", clk.Now()); err != nil { // 转离线
		t.Fatal(err)
	}
	// 越过保留期限（严格超过 retainUntil 才清退）；扫描前再给 d2/d3 续心跳，
	// 避免它们因长间隔被判会话超时。
	clk.Advance(time.Hour + time.Second)
	if _, err := c.Heartbeat("g", "d2", 3); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Heartbeat("g", "d3", 3); err != nil {
		t.Fatal(err)
	}
	removed, err := c.ExpireGroup("g", clk.Now()) // 保留期届满
	if err != nil || !equalStrings(removed, []string{"s1"}) {
		t.Fatalf("expiry removed=%v err=%v", removed, err)
	}
	st, _ = c.Status("g")
	if st.Phase != PhaseStable {
		t.Fatalf("forced reclaim should settle immediately, phase=%s", st.Phase)
	}
	// 后继确定性地唯一：online sorted [d2 d3]，大于 s1 的最小者为 d2。
	for _, p := range s1Held {
		if got := st.Assignment.Owners[p]; got != "d2" {
			t.Fatalf("evicted batch partition %d owner = %q, want single successor d2", p, got)
		}
	}
	// s1 身份已彻底消失。
	if len(st.StaticInstances) != 0 {
		t.Fatalf("evicted instance still present: %+v", st.StaticInstances)
	}
}

// 主动退出：分区整批立即移交唯一后继，无需撤销确认。
func TestStaticLeaveEvictsBatch(t *testing.T) {
	c, _ := staticGroup(t, 6, time.Hour)
	mustStaticJoin(t, c, "g", "s1", "s1-1", time.Hour) // gen1 全持有
	if err := c.LeaveStatic("g", "s1"); err != nil {
		t.Fatal(err)
	}
	st, _ := c.Status("g")
	if st.Generation != 2 || st.Phase != PhaseStable {
		t.Fatalf("gen=%d phase=%s", st.Generation, st.Phase)
	}
	for p := 0; p < 6; p++ {
		if st.Assignment.Owners[p] != "" {
			t.Fatalf("partition %d = %q, want unowned after sole member leaves", p, st.Assignment.Owners[p])
		}
	}
	// 旧会话之后的操作按未知成员拒绝（身份已清除）。
	if _, err := c.HeartbeatV2("g", HeartbeatRequest{
		MemberID: "s1-1", Generation: 2, SessionVersion: 1,
	}); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("post-leave operation = %v, want ErrMemberNotFound", err)
	}
}

// 管理员移除（含保留期内离线实例）与主动退出同规则。
func TestAdminRemoveOfflineInstance(t *testing.T) {
	c, clk := staticGroup(t, 4, 10*time.Second)
	mustStaticJoin(t, c, "g", "s1", "s1-1", time.Hour)
	mustJoin(t, c, "g", "d2") // gen2
	st, _ := c.Status("g")
	mustStaticAck(t, c, "g", "s1-1", 1, 2, st.PendingRevocations["s1"])
	held := func() []int {
		s, _ := c.Status("g")
		return findInstance(s, "s1").HeldPartitions
	}()
	// 推进到会话超时后让 d2 续心跳，再扫描：只有 s1 离线。
	clk.Advance(11 * time.Second)
	if _, err := c.Heartbeat("g", "d2", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExpireGroup("g", clk.Now()); err != nil {
		t.Fatal(err)
	}
	if err := c.RemoveInstance("g", "s1"); err != nil { // 管理员直接清退离线实例
		t.Fatal(err)
	}
	s, _ := c.Status("g")
	if s.Phase != PhaseStable {
		t.Fatalf("phase = %s", s.Phase)
	}
	for _, p := range held {
		if s.Assignment.Owners[p] != "d2" {
			t.Fatalf("partition %d owner = %q, want d2", p, s.Assignment.Owners[p])
		}
	}
}

// 动态成员不能用 LeaveStatic，静态会话不能用动态 Leave。
func TestLeaveKindMismatch(t *testing.T) {
	c, _ := staticGroup(t, 2, time.Hour)
	mustJoin(t, c, "g", "d1")
	mustStaticJoin(t, c, "g", "s1", "s1-1", time.Hour)
	if err := c.Leave("g", "s1-1"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("dynamic Leave on static session = %v", err)
	}
	if err := c.LeaveStatic("g", "d1"); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("LeaveStatic on dynamic member = %v", err)
	}
}

// 多个静态实例在同一次扫描中保留期届满：每个实例的批次各自整体移交给唯一后继，
// 批次不被拆散、不互相串台。
func TestMultipleStaticBatchesExpireToDistinctSuccessors(t *testing.T) {
	c, clk := staticGroup(t, 12, 10*time.Second)
	// 在线主体字典序 [s1 s2 s3]，target 轮转：
	// [s1 s2 s3 s1 s2 s3 s1 s2 s3 s1 s2 s3]
	mustStaticJoin(t, c, "g", "s1", "s1-1", 30*time.Second)       // gen1 stable
	j2 := mustStaticJoin(t, c, "g", "s2", "s2-1", 30*time.Second) // gen2 revoking
	{
		s, _ := c.Status("g")
		for _, principal := range []string{"s1", "s2"} {
			if ps := s.PendingRevocations[principal]; len(ps) > 0 {
				mustStaticAck(t, c, "g", principal+"-1", 1, j2.Generation, ps)
			}
		}
	}
	j3 := mustStaticJoin(t, c, "g", "s3", "s3-1", 30*time.Second) // gen3 revoking
	{
		s, _ := c.Status("g")
		for _, principal := range []string{"s1", "s2", "s3"} {
			if ps := s.PendingRevocations[principal]; len(ps) > 0 {
				mustStaticAck(t, c, "g", principal+"-1", 1, j3.Generation, ps)
			}
		}
	}
	st, _ := c.Status("g")
	if st.Phase != PhaseStable {
		t.Fatalf("setup phase = %s", st.Phase)
	}
	held := map[string][]int{}
	for _, is := range st.StaticInstances {
		held[is.InstanceID] = append([]int(nil), is.HeldPartitions...)
	}
	// 每实例 4 个分区。
	for id, ps := range held {
		if len(ps) != 4 {
			t.Fatalf("%s held %v, want 4 partitions", id, ps)
		}
	}

	// s1、s2 停止心跳并越过会话超时（s3 续心跳保持在线）。
	clk.Advance(11 * time.Second)
	if _, err := c.HeartbeatV2("g", HeartbeatRequest{
		MemberID: "s3-1", Generation: st.Generation, SessionVersion: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if changed, err := c.ExpireAll(clk.Now()); err != nil || changed != nil {
		t.Fatalf("temporary offline changed=%v err=%v (should not report rebalance)", changed, err)
	}
	// 越过保留期，s1、s2 在同一次扫描中清退。
	clk.Advance(31 * time.Second)
	if _, err := c.HeartbeatV2("g", HeartbeatRequest{
		MemberID: "s3-1", Generation: st.Generation, SessionVersion: 1,
	}); err != nil {
		t.Fatal(err)
	}
	removed, err := c.ExpireGroup("g", clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !equalStrings(removed, []string{"s1", "s2"}) {
		t.Fatalf("removed = %v, want [s1 s2]", removed)
	}

	final, _ := c.Status("g")
	if final.Phase != PhaseStable {
		t.Fatalf("phase = %s", final.Phase)
	}
	// 唯一在线后继只有 s3：两个清退批次都必须整体归 s3（每批仍是 4 个分区）。
	count := map[string]int{}
	for _, p := range append(append([]int(nil), held["s1"]...), held["s2"]...) {
		owner := final.Assignment.Owners[p]
		if owner != "s3" {
			t.Fatalf("partition %d owner = %q, every partition of both batches must go to sole successor s3", p, owner)
		}
		count[owner]++
	}
	if count["s3"] != 8 {
		t.Fatalf("s3 received %d partitions, want all 8 from both batches", count["s3"])
	}
	if len(final.StaticInstances) != 1 || final.StaticInstances[0].InstanceID != "s3" {
		t.Fatalf("remaining instances = %+v", final.StaticInstances)
	}
}

// ---- 需求 3：并发加入唯一当前会话；位点不倒退；旧会话不能确认新撤销集合 ----

// 同一实例两个进程并发加入：只有会话版本更高者成为当前成员。
func TestConcurrentStaticJoinOnlyOneCurrentSession(t *testing.T) {
	c, _ := staticGroup(t, 4, time.Hour)
	const n = 16
	var wg sync.WaitGroup
	results := make([]StaticJoinResult, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = c.JoinStatic("g", StaticJoinOptions{
				InstanceID: "s1", SessionID: fmt.Sprintf("proc-%02d", i), Retention: time.Hour,
			})
		}(i)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			t.Fatalf("concurrent join error: %v", e)
		}
	}

	var maxVersion int64
	currentSession := ""
	versionCount := map[int64]int{}
	for _, r := range results {
		versionCount[r.SessionVersion]++
		if r.SessionVersion >= maxVersion {
			maxVersion = r.SessionVersion
			currentSession = r.SessionID
		}
	}
	if maxVersion != int64(n) {
		t.Fatalf("max session version = %d, want %d", maxVersion, n)
	}
	// 每个会话版本恰好发放一次。
	for v := int64(1); v <= int64(n); v++ {
		if versionCount[v] != 1 {
			t.Fatalf("session version %d granted %d times, want exactly 1", v, versionCount[v])
		}
	}
	st, _ := c.Status("g")
	is := findInstance(st, "s1")
	if is.SessionVersion != int64(n) || is.SessionID != currentSession || !is.Online {
		t.Fatalf("current session = %+v, want version=%d session=%q", is, n, currentSession)
	}
	// 活动会话只有一个；旧进程的心跳一律被栅栏。
	if len(st.Members) != 1 || st.Members[0].ID != currentSession {
		t.Fatalf("active members = %+v, want only %q", st.Members, currentSession)
	}
	for i, r := range results {
		if r.SessionVersion == maxVersion {
			continue
		}
		if _, err := c.HeartbeatV2("g", HeartbeatRequest{
			MemberID: r.SessionID, Generation: st.Generation, SessionVersion: r.SessionVersion,
		}); !errors.Is(err, ErrFencedSession) {
			t.Fatalf("proc %d (version %d) heartbeat = %v, want fenced", i, r.SessionVersion, err)
		}
	}
}

// 新会话接管、旧会话最终位点提交、超时扫描并发相撞：位点单调不倒退；
// 幂等请求号作用域为实例（跨重连重放），旧会话不能确认新版本撤销集合。
func TestTakeoverCommitAndExpiryRace(t *testing.T) {
	c, clk := staticGroup(t, 2, time.Hour)
	r1 := mustStaticJoin(t, c, "g", "s1", "s1-old", time.Hour) // gen1，s1 持有分区 0、1

	// 旧会话先提交分区 0 的位点 100。
	res, err := c.CommitOffset("g", CommitRequest{
		MemberID: "s1-old", SessionVersion: 1, Generation: r1.Generation,
		Partition: 0, Offset: 100, RequestID: "r-100",
	})
	if err != nil || res.Offset != 100 {
		t.Fatalf("initial commit = %+v %v", res, err)
	}

	// 新会话接管（版本 2）。
	r2 := mustStaticJoin(t, c, "g", "s1", "s1-new", time.Hour)

	var wg sync.WaitGroup
	// 旧会话迟到的最终提交（较小位点）：必须被栅栏，而不是落地或报 backtrack。
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, e := c.CommitOffset("g", CommitRequest{
			MemberID: "s1-old", SessionVersion: 1, Generation: r2.Generation,
			Partition: 0, Offset: 90, RequestID: "r-old-90",
		})
		if !errors.Is(e, ErrFencedSession) {
			t.Errorf("stale final commit = %v, want ErrFencedSession", e)
		}
	}()
	// 新会话提交更大位点 200。
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, e := c.CommitOffset("g", CommitRequest{
			MemberID: "s1-new", SessionVersion: 2, Generation: r2.Generation,
			Partition: 0, Offset: 200, RequestID: "r-200",
		}); e != nil {
			t.Errorf("new session commit: %v", e)
		}
	}()
	// 超时扫描同时进行（会话超时设为 1 小时，不会实际剔除，仅验证不互相破坏）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = c.ExpireAll(clk.Now())
	}()
	wg.Wait()

	st, _ := c.Status("g")
	if got := st.Offsets[0].Offset; got != 200 {
		t.Fatalf("offset = %d, want 200 (must not move backwards)", got)
	}

	// 跨重连幂等：新会话重放旧会话的请求号 r-100/100 -> Replayed，不重复推进。
	replay, err := c.CommitOffset("g", CommitRequest{
		MemberID: "s1-new", SessionVersion: 2, Generation: r2.Generation,
		Partition: 0, Offset: 100, RequestID: "r-100",
	})
	if err != nil || !replay.Replayed || replay.Offset != 200 {
		t.Fatalf("cross-reconnect replay = %+v %v", replay, err)
	}
	// 同请求号改位点仍是冲突（实例级幂等记录跨重连生效）。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "s1-new", SessionVersion: 2, Generation: r2.Generation,
		Partition: 0, Offset: 101, RequestID: "r-100",
	}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("cross-reconnect conflict = %v", err)
	}

	// 旧会话不能确认（哪怕存在的）撤销集合。
	j := mustJoin(t, c, "g", "d2") // 接管未增版本，故 d2 加入为 gen2
	st2, _ := c.Status("g")
	pending := st2.PendingRevocations["s1"]
	if _, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "s1-old", SessionVersion: 1, Generation: j.Generation, Partitions: pending,
	}); !errors.Is(err, ErrFencedSession) {
		t.Fatalf("stale session ack = %v, want ErrFencedSession", err)
	}
	// 新会话确认才被受理，再均衡随之推进（d2 无撤销义务，s1 确认即收敛）。
	ack, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "s1-new", SessionVersion: 2, Generation: j.Generation, Partitions: pending,
	})
	if err != nil {
		t.Fatalf("current session ack: %v", err)
	}
	if !ack.CompletedRebalance || ack.Phase != PhaseStable {
		t.Fatalf("current session ack = %+v", ack)
	}
}

// 撤销进行中静态实例断线：撤销义务挂起，其他成员确认不能让版本越过分区收敛；
// 保留期内重连后新会话确认挂起义务，版本在同一 generation 收敛。
func TestStaticReconnectDuringRevocationResumes(t *testing.T) {
	c, clk := staticGroup(t, 4, 10*time.Second)
	mustStaticJoin(t, c, "g", "s1", "s1-old", 30*time.Second) // gen1 全持有
	mustJoin(t, c, "g", "d2")                                 // gen2：s1 须撤销 {1,3}
	st, _ := c.Status("g")
	pending := append([]int(nil), st.PendingRevocations["s1"]...)

	// 推进到会话超时后让 d2 续心跳，再扫描：只有 s1 离线（仍在保留期）。
	clk.Advance(11 * time.Second)
	if _, err := c.Heartbeat("g", "d2", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExpireGroup("g", clk.Now()); err != nil {
		t.Fatal(err)
	}
	st, _ = c.Status("g")
	if st.Generation != 2 || st.Phase != PhaseRevoking {
		t.Fatalf("offline during revoking gen=%d phase=%s", st.Generation, st.Phase)
	}
	// 分区仍由离线实例生效持有，撤销义务挂起。
	if !equalStrings(st.Assignment.Owners, []string{"s1", "s1", "s1", "s1"}) {
		t.Fatalf("effective owners = %v", st.Assignment.Owners)
	}
	var suspended int
	for _, tr := range st.PendingTransfers {
		if tr.Reason == TransferReasonSuspended {
			suspended++
		}
	}
	if suspended != len(pending) {
		t.Fatalf("suspended transfers = %d, want %d (%+v)", suspended, len(pending), st.PendingTransfers)
	}

	// 保留期内以新会话重连，版本仍是 gen2，新会话确认挂起义务后收敛。
	r := mustStaticJoin(t, c, "g", "s1", "s1-new", 30*time.Second)
	if r.Generation != 2 || !r.Rejoined {
		t.Fatalf("rejoin gen=%d rejoined=%v", r.Generation, r.Rejoined)
	}
	ack, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "s1-new", SessionVersion: 2, Generation: 2, Partitions: pending,
	})
	if err != nil {
		t.Fatalf("resumed ack: %v", err)
	}
	if !ack.CompletedRebalance || ack.Phase != PhaseStable {
		t.Fatalf("resumed ack = %+v", ack)
	}
}
