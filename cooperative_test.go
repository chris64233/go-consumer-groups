package consumergroups

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// setupGroup 创建组并让 members 依次加入，但不代任何成员做撤销确认，
// 返回协调器与最终版本。调用方自行驱动 AckRevocation。
func setupRevokingGroup(t *testing.T, partitions int, members ...string) (*Coordinator, *fakeClock, int64) {
	t.Helper()
	clk := newFakeClock()
	c, err := NewCoordinator(nil, clk)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateGroup(CreateGroupOptions{
		Name: "g", Partitions: partitions, SessionTimeout: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	var gen int64
	for _, m := range members {
		r := mustJoin(t, c, "g", m)
		gen = r.Generation
	}
	return c, clk, gen
}

// settle 代当前版本所有有撤销义务的成员逐个确认，直到组进入 stable。
// 用于在测试中构造「某版本已稳定」的前置状态。
func settle(t *testing.T, c *Coordinator, group string) {
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
		pending := make(map[string][]int, len(st.PendingRevocations))
		for id, ps := range st.PendingRevocations {
			pending[id] = append([]int(nil), ps...)
		}
		for id, ps := range pending {
			mustAck(t, c, group, id, gen, ps)
		}
	}
}

// ---- 基本两阶段流程 ----

// 新成员加入后，受影响分区进入待撤销；旧主确认前新主拿不到分区，
// 未受影响分区继续由原成员消费；全部确认后版本在同一 generation 内收敛。
func TestCooperativeTwoPhaseRevocation(t *testing.T) {
	// 4 分区：a 单独在 gen1 持有 [a a a a]；b 加入后 gen2 target=[a b a b]。
	c, _, gen := setupRevokingGroup(t, 4, "a", "b")
	if gen != 2 {
		t.Fatalf("gen = %d, want 2", gen)
	}
	st, _ := c.Status("g")
	if st.Phase != PhaseRevoking {
		t.Fatalf("phase = %s, want revoking", st.Phase)
	}
	// 生效所有权未变：b 在 a 确认前拿不到任何分区。
	if !equalStrings(st.Assignment.Owners, []string{"a", "a", "a", "a"}) {
		t.Fatalf("effective owners = %v", st.Assignment.Owners)
	}
	if !equalStrings(st.TargetAssignment.Owners, []string{"a", "b", "a", "b"}) {
		t.Fatalf("target owners = %v", st.TargetAssignment.Owners)
	}
	// 待撤销集合与逐成员确认进度查询。
	if !equalInts(st.PendingRevocations["a"], []int{1, 3}) {
		t.Fatalf("pending = %v", st.PendingRevocations)
	}
	if len(st.RevocationProgress) != 1 {
		t.Fatalf("progress = %+v", st.RevocationProgress)
	}
	pr := st.RevocationProgress[0]
	if pr.MemberID != "a" || !equalInts(pr.Required, []int{1, 3}) ||
		!equalInts(pr.Acked, nil) || pr.Done {
		t.Fatalf("progress entry = %+v", pr)
	}

	// 未受影响分区 0、2 继续由 a 正常消费与提交。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 0, Offset: 10, RequestID: "k1",
	}); err != nil {
		t.Fatalf("unaffected partition commit: %v", err)
	}

	ack := mustAck(t, c, "g", "a", 2, []int{1, 3})
	if !ack.CompletedRebalance || ack.Phase != PhaseStable || ack.Generation != 2 {
		t.Fatalf("ack = %+v", ack)
	}
	st, _ = c.Status("g")
	if st.Generation != 2 { // 版本号不因收敛而再次 +1
		t.Fatalf("generation after convergence = %d, want 2", st.Generation)
	}
	if !equalStrings(st.Assignment.Owners, []string{"a", "b", "a", "b"}) {
		t.Fatalf("owners after convergence = %v", st.Assignment.Owners)
	}
	if len(st.PendingRevocations) != 0 || len(st.RevocationProgress) != 0 {
		t.Fatalf("pending/progress should be empty: %+v / %+v",
			st.PendingRevocations, st.RevocationProgress)
	}
}

// 多成员各自需要撤销时，任一成员单独确认都不能提前完成整个再均衡。
func TestCooperativePartialAckDoesNotComplete(t *testing.T) {
	// 5 分区：charlie gen1 全部持有；alpha 加入 gen2 并完成撤销，
	// 稳定态 [alpha charlie alpha charlie alpha]；bravo 再加入得到 gen3，
	// target=[alpha bravo charlie alpha bravo]。
	c, _ := NewCoordinator(nil, newFakeClock())
	if err := c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 5, SessionTimeout: time.Hour}); err != nil {
		t.Fatal(err)
	}
	mustJoin(t, c, "g", "charlie")                   // gen1
	mustJoin(t, c, "g", "alpha")                     // gen2 revoking
	mustAck(t, c, "g", "charlie", 2, []int{0, 2, 4}) // gen2 收敛
	jb := mustJoin(t, c, "g", "bravo")               // gen3 revoking
	if jb.Generation != 3 || jb.Phase != PhaseRevoking {
		t.Fatalf("join bravo = %+v", jb)
	}
	st, _ := c.Status("g")
	// gen3 相对 gen2 稳定态：分区 2 alpha→charlie，分区 4 alpha→bravo，
	// 分区 1 charlie→bravo，分区 3 charlie→alpha。
	if !equalInts(st.PendingRevocations["alpha"], []int{2, 4}) {
		t.Fatalf("alpha pending = %v", st.PendingRevocations["alpha"])
	}
	if !equalInts(st.PendingRevocations["charlie"], []int{1, 3}) {
		t.Fatalf("charlie pending = %v", st.PendingRevocations["charlie"])
	}
	if _, ok := st.PendingRevocations["bravo"]; ok {
		t.Fatalf("new member bravo must have no revocation obligation: %v", st.PendingRevocations)
	}

	// alpha 先确认：分区 2、4 立即转移给目标所有者（2→charlie，4→bravo）。
	ack := mustAck(t, c, "g", "alpha", 3, []int{2, 4})
	if ack.CompletedRebalance || ack.Phase != PhaseRevoking {
		t.Fatalf("partial ack = %+v, must not complete rebalance", ack)
	}
	st, _ = c.Status("g")
	if got := st.Assignment.Owners; !equalStrings(got,
		[]string{"alpha", "charlie", "charlie", "charlie", "bravo"}) {
		t.Fatalf("effective owners after alpha ack = %v", got)
	}
	// alpha 进度完成，charlie 仍未确认。
	var alphaDone, charlieDone bool
	for _, p := range st.RevocationProgress {
		switch p.MemberID {
		case "alpha":
			alphaDone = p.Done && equalInts(p.Acked, []int{2, 4})
		case "charlie":
			charlieDone = p.Done
		}
	}
	if !alphaDone || charlieDone {
		t.Fatalf("progress = %+v", st.RevocationProgress)
	}

	// charlie 确认后才整体收敛。
	ack = mustAck(t, c, "g", "charlie", 3, []int{1, 3})
	if !ack.CompletedRebalance || ack.Phase != PhaseStable {
		t.Fatalf("final ack = %+v", ack)
	}
}

// ---- 确认的校验与幂等重试 ----

func TestAckRevocationValidation(t *testing.T) {
	c, _, _ := setupRevokingGroup(t, 4, "a", "b") // gen2: a 须撤销 {1,3}

	// 漏项（只确认一个分区）：明确拒绝，不转移任何分区。
	_, err := c.AckRevocation("g", RevocationAckRequest{MemberID: "a", Generation: 2, Partitions: []int{1}})
	mme := requireError[*RevocationMismatchError](t, err, ErrRevocationMismatch)
	if !equalInts(mme.Missing, []int{3}) || len(mme.Extra) != 0 ||
		!equalInts(mme.Expected, []int{1, 3}) {
		t.Fatalf("missing-partition error = %+v", mme)
	}
	st, _ := c.Status("g")
	if !equalStrings(st.Assignment.Owners, []string{"a", "a", "a", "a"}) {
		t.Fatalf("rejected ack must not transfer anything, owners=%v", st.Assignment.Owners)
	}

	// 额外分区：确认一个不属于自己撤销义务的分区（0 是未受影响分区）。
	_, err = c.AckRevocation("g", RevocationAckRequest{
		MemberID: "a", Generation: 2, Partitions: []int{0, 1, 3},
	})
	mme = requireError[*RevocationMismatchError](t, err, ErrRevocationMismatch)
	if !equalInts(mme.Extra, []int{0}) || len(mme.Missing) != 0 {
		t.Fatalf("extra-partition error = %+v", mme)
	}

	// 新成员 b 本版本没有撤销义务，替别人确认被拒绝，且不能推动状态机。
	if _, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "b", Generation: 2, Partitions: []int{1, 3},
	}); !errors.Is(err, ErrNoRevocationInProgress) {
		t.Fatalf("ack by member without obligation should fail, got %v", err)
	}
	// 义务成员提交空集合同样按漏项拒绝（而不是被当作「无需撤销」）。
	_, err = c.AckRevocation("g", RevocationAckRequest{MemberID: "a", Generation: 2})
	if !errors.Is(err, ErrRevocationMismatch) {
		t.Fatalf("empty ack from obligated member should mismatch, got %v", err)
	}
	// 越界分区。
	if _, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "a", Generation: 2, Partitions: []int{1, 3, 9},
	}); !errors.Is(err, ErrInvalidPartition) {
		t.Fatalf("out-of-range ack should fail, got %v", err)
	}
	// 旧版本 / 超前版本确认。
	if _, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "a", Generation: 1, Partitions: []int{1, 3},
	}); !errors.Is(err, ErrIllegalGeneration) {
		t.Fatalf("stale generation ack should fail, got %v", err)
	}
	if _, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "a", Generation: 99, Partitions: []int{1, 3},
	}); !errors.Is(err, ErrIllegalGeneration) {
		t.Fatalf("future generation ack should fail, got %v", err)
	}
	// 非成员确认。
	if _, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "ghost", Generation: 2, Partitions: []int{1, 3},
	}); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("unknown member ack should fail, got %v", err)
	}
	// 组不存在。
	if _, err := c.AckRevocation("nope", RevocationAckRequest{
		MemberID: "a", Generation: 2, Partitions: []int{1},
	}); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("missing group ack should fail, got %v", err)
	}
}

// 确认可以安全重试：撤销阶段重复确认按幂等成功返回；
// 再均衡随首次确认收敛后，迟到的重复确认也不报错。
func TestAckRevocationIdempotentRetry(t *testing.T) {
	c, _, _ := setupRevokingGroup(t, 4, "a", "b") // a 须撤销 {1,3}

	first := mustAck(t, c, "g", "a", 2, []int{1, 3})
	if !first.CompletedRebalance {
		t.Fatalf("first ack should complete rebalance: %+v", first)
	}
	// 响应丢失后客户端重试：已处于 stable，同版本确认幂等成功，不报错。
	retry, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "a", Generation: 2, Partitions: []int{1, 3},
	})
	if err != nil {
		t.Fatalf("retry after completion: %v", err)
	}
	if !retry.CompletedRebalance || retry.Phase != PhaseStable {
		t.Fatalf("retry = %+v", retry)
	}

	// 多义务成员场景：a/b 已稳定分配 [a b a b a b]，c 加入产生新版本，
	// a、b 都需向 c 移交分区。任一成员单独确认都不完成整体再均衡。
	c2, _ := NewCoordinator(nil, newFakeClock())
	if err := c2.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 6, SessionTimeout: time.Hour}); err != nil {
		t.Fatal(err)
	}
	mustJoin(t, c2, "g", "a")
	mustJoin(t, c2, "g", "b")
	settle(t, c2, "g") // 收敛到 [a b a b a b]
	jc := mustJoin(t, c2, "g", "c")
	if jc.Phase != PhaseRevoking {
		t.Fatalf("setup phase = %s, want revoking", jc.Phase)
	}
	st, _ := c2.Status("g")
	if len(st.PendingRevocations) < 2 {
		t.Fatalf("expected multiple obligated members, got %v", st.PendingRevocations)
	}
	// 找一个有撤销义务的成员确认一次，再重复确认。
	var member string
	for id := range st.PendingRevocations {
		member = id
		break
	}
	partitions := append([]int(nil), st.PendingRevocations[member]...)
	ack1 := mustAck(t, c2, "g", member, st.Generation, partitions)
	if ack1.CompletedRebalance {
		t.Fatalf("one member ack should not complete multi-obligation rebalance")
	}
	ack2 := mustAck(t, c2, "g", member, st.Generation, partitions) // 重试
	if ack2.CompletedRebalance || ack2.Phase != PhaseRevoking {
		t.Fatalf("idempotent retry during revoking = %+v", ack2)
	}
}

// ---- 撤销阶段的成员变化：强制回收、版本作废 ----

// 义务成员在撤销阶段主动离开：剩余分区被强制回收并立即移交目标所有者，
// 无需它的确认。
func TestLeaveDuringRevocationForceReclaims(t *testing.T) {
	// a gen1 全持有；b 加入 gen2，a 须撤销 {1,3}。a 不确认直接离开。
	c, clk, _ := setupRevokingGroup(t, 4, "a", "b")
	if err := c.Leave("g", "a"); err != nil {
		t.Fatal(err)
	}
	st, _ := c.Status("g")
	if st.Generation != 3 {
		t.Fatalf("generation after leave = %d, want 3", st.Generation)
	}
	// a 的分区被强制回收：target 全归 b，且立即生效（无需任何撤销确认）。
	if st.Phase != PhaseStable {
		t.Fatalf("phase = %s, want stable (nothing to revoke from live owners)", st.Phase)
	}
	if !equalStrings(st.Assignment.Owners, []string{"b", "b", "b", "b"}) {
		t.Fatalf("owners after forced reclaim = %v", st.Assignment.Owners)
	}
	if len(st.PendingRevocations) != 0 {
		t.Fatalf("pending should be empty, got %v", st.PendingRevocations)
	}
	// 时钟仍可驱动且状态一致。
	clk.Advance(time.Second)
	if _, err := c.ExpireGroup("g", clk.Now()); err != nil {
		t.Fatal(err)
	}
}

// 义务成员在撤销阶段会话超时：扫描剔除并强制回收其剩余分区。
func TestExpireDuringRevocationForceReclaims(t *testing.T) {
	c, clk, _ := setupRevokingGroup(t, 4, "a", "b")
	// a 不心跳直到超时；b 持续心跳。
	clk.Advance(61 * time.Minute)
	if _, err := c.Heartbeat("g", "b", 2); err != nil {
		t.Fatal(err)
	}
	expired, err := c.ExpireGroup("g", clk.Now())
	if err != nil || !equalStrings(expired, []string{"a"}) {
		t.Fatalf("expired = %v, %v", expired, err)
	}
	st, _ := c.Status("g")
	if st.Generation != 3 || st.Phase != PhaseStable {
		t.Fatalf("gen=%d phase=%s", st.Generation, st.Phase)
	}
	if !equalStrings(st.Assignment.Owners, []string{"b", "b", "b", "b"}) {
		t.Fatalf("owners after timeout reclaim = %v", st.Assignment.Owners)
	}
}

// 撤销进行中再次发生成员变化：版本 +1，旧版本确认全部作废，
// 已确认的转移结果并入新版本的生效所有权，未确认的分区重新计算义务。
func TestMembershipChangeDuringRevocationBumpsGeneration(t *testing.T) {
	c, _ := NewCoordinator(nil, newFakeClock())
	if err := c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 6, SessionTimeout: time.Hour}); err != nil {
		t.Fatal(err)
	}
	mustJoin(t, c, "g", "a")
	mustJoin(t, c, "g", "b")
	settle(t, c, "g") // gen2 稳定：[a b a b a b]

	// c 加入 => gen3：target=[a b c a b c]。
	// p2 a→c（a 义务 {2}），p3 b→a 与 p5 b→c（b 义务 {3,5}）。
	jc := mustJoin(t, c, "g", "c")
	if jc.Generation != 3 || jc.Phase != PhaseRevoking {
		t.Fatalf("join c = %+v", jc)
	}
	st, _ := c.Status("g")
	if !equalInts(st.PendingRevocations["a"], []int{2, 4}) ||
		!equalInts(st.PendingRevocations["b"], []int{3, 5}) {
		t.Fatalf("gen3 pending = %+v", st.PendingRevocations)
	}
	// 只确认 a：分区 2、4 转给目标所有者（2→c，4→b）；b 的 3、5 仍未转移。
	mustAck(t, c, "g", "a", 3, []int{2, 4})
	st, _ = c.Status("g")
	if got := st.Assignment.Owners; !equalStrings(got, []string{"a", "b", "c", "b", "b", "b"}) {
		t.Fatalf("owners after a ack = %v", got)
	}

	// d 加入 => gen4，旧版本号 3 的确认立即作废。
	jd := mustJoin(t, c, "g", "d")
	if jd.Generation != 4 || jd.Phase != PhaseRevoking {
		t.Fatalf("join d = %+v", jd)
	}
	// b 用旧版本 3 补交它当时的义务 {3,5}：必须被拒绝，不能提前完成任何再均衡。
	if _, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "b", Generation: 3, Partitions: []int{3, 5},
	}); !errors.Is(err, ErrIllegalGeneration) {
		t.Fatalf("stale ack after new rebalance should fail, got %v", err)
	}
	st, _ = c.Status("g")
	// gen4 target：成员 [a b c d] => [a b c d a b]。
	if !equalStrings(st.TargetAssignment.Owners, []string{"a", "b", "c", "d", "a", "b"}) {
		t.Fatalf("gen4 target = %v", st.TargetAssignment.Owners)
	}
	// 生效所有权 [a b c b b b]：在途分区为 3（b→d）与 4（b→a），都归 b 待撤销；
	// 分区 5 的 gen4 目标又回到 b，b 继续持有（gen3 的撤销义务随版本作废，
	// 该分区从未被双重持有）。
	if !equalStrings(st.Assignment.Owners, []string{"a", "b", "c", "b", "b", "b"}) {
		t.Fatalf("gen4 effective owners = %v", st.Assignment.Owners)
	}
	if !equalInts(st.PendingRevocations["b"], []int{3, 4}) {
		t.Fatalf("b gen4 pending = %v, want [3 4]", st.PendingRevocations["b"])
	}
	if _, ok := st.PendingRevocations["a"]; ok {
		t.Fatalf("a should have no gen4 obligation: %v", st.PendingRevocations)
	}
	// b 按 gen4 的新义务确认 {3,4} 后收敛为 target=[a b c d a b]。
	ack := mustAck(t, c, "g", "b", 4, []int{3, 4})
	if !ack.CompletedRebalance {
		t.Fatalf("ack = %+v", ack)
	}
	st, _ = c.Status("g")
	if !equalStrings(st.Assignment.Owners, []string{"a", "b", "c", "d", "a", "b"}) {
		t.Fatalf("gen4 final owners = %v", st.Assignment.Owners)
	}
}

// 没有分区需要在存活成员间转移时，新版本立即 stable（无需任何确认）：
// 首次加入、组清空后重新加入、或新成员在目标分配中没拿到任何分区。
func TestRebalanceWithoutLiveTransferIsImmediatelyStable(t *testing.T) {
	c, _ := NewCoordinator(nil, newFakeClock())
	if err := c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 1, SessionTimeout: time.Hour}); err != nil {
		t.Fatal(err)
	}
	ja := mustJoin(t, c, "g", "a")
	if ja.Generation != 1 || ja.Phase != PhaseStable {
		t.Fatalf("first join = %+v", ja)
	}

	// 仅 1 个分区：a 始终是 target 所有者，后续加入的 b/c/d 拿不到分区，
	// 没有任何分区易主，全部立即 stable，且没有成员有撤销义务。
	jb := mustJoin(t, c, "g", "b")
	if jb.Phase != PhaseStable || jb.Generation != 2 {
		t.Fatalf("join b = %+v", jb)
	}
	jc := mustJoin(t, c, "g", "c")
	if jc.Phase != PhaseStable || jc.Generation != 3 {
		t.Fatalf("join c = %+v", jc)
	}
	jd := mustJoin(t, c, "g", "d")
	if jd.Phase != PhaseStable || jd.Generation != 4 {
		t.Fatalf("join d = %+v", jd)
	}
	st, _ := c.Status("g")
	if !equalStrings(st.Assignment.Owners, []string{"a"}) || len(st.PendingRevocations) != 0 {
		t.Fatalf("status = owners=%v pending=%v", st.Assignment.Owners, st.PendingRevocations)
	}
	// stable 阶段的同版本确认按幂等重放成功（响应可能已让再均衡收敛），
	// 不会报错或推进版本。
	ack, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "a", Generation: 4, Partitions: []int{0},
	})
	if err != nil || ack.Generation != 4 || !ack.CompletedRebalance {
		t.Fatalf("stable-phase ack = %+v, %v", ack, err)
	}

	// 全部离开后重新加入：无主分区立即分派，无需确认。
	for _, id := range []string{"d", "c", "b", "a"} {
		if err := c.Leave("g", id); err != nil {
			t.Fatal(err)
		}
	}
	st, _ = c.Status("g")
	if st.Generation != 8 || st.Phase != PhaseStable {
		t.Fatalf("emptied gen=%d phase=%s", st.Generation, st.Phase)
	}
	jr := mustJoin(t, c, "g", "a")
	if jr.Generation != 9 || jr.Phase != PhaseStable ||
		!equalStrings(jr.Assignment.Owners, []string{"a"}) {
		t.Fatalf("rejoin = %+v", jr)
	}
}

// ---- 心跳与撤销阶段 ----

func TestHeartbeatDuringRevocation(t *testing.T) {
	c, _, _ := setupRevokingGroup(t, 4, "a", "b") // gen2: a 须撤销 {1,3}

	// 义务成员的当前版本心跳被接受，并带回其待撤销列表与目标所有权。
	hb, err := c.Heartbeat("g", "a", 2)
	if err != nil {
		t.Fatalf("heartbeat during revoking: %v", err)
	}
	if hb.Phase != PhaseRevoking || !equalInts(hb.Revoking, []int{1, 3}) {
		t.Fatalf("a heartbeat = %+v revoking=%v", hb, hb.Revoking)
	}
	if !equalStrings(hb.TargetAssignment.Owners, []string{"a", "b", "a", "b"}) {
		t.Fatalf("heartbeat target = %v", hb.TargetAssignment.Owners)
	}
	// 新成员 b 没有撤销义务：Revoking 为空。
	hb2, err := c.Heartbeat("g", "b", 2)
	if err != nil || len(hb2.Revoking) != 0 {
		t.Fatalf("b heartbeat = %+v, %v", hb2, err)
	}
	// 旧版本心跳在撤销阶段同样被拒。
	if _, err := c.Heartbeat("g", "a", 1); !errors.Is(err, ErrIllegalGeneration) {
		t.Fatalf("stale heartbeat during revoking: %v", err)
	}

	// 收敛后心跳阶段为 stable，无待撤销分区。
	mustAck(t, c, "g", "a", 2, []int{1, 3})
	hb3, err := c.Heartbeat("g", "a", 2)
	if err != nil || hb3.Phase != PhaseStable || len(hb3.Revoking) != 0 {
		t.Fatalf("stable heartbeat = %+v, %v", hb3, err)
	}
}

// ---- 位点提交栅栏的补充场景 ----

// 待撤销期间旧主提交最终位点成功；新主提前提交被拒；
// 旧主确认后旧主迟到提交被栅栏、新主提交成功、未受影响分区全程可提交。
func TestCommitFencingDetailed(t *testing.T) {
	c, _, _ := setupRevokingGroup(t, 4, "a", "b") // target [a b a b]，a 须撤销 {1,3}

	// 待撤销分区 1：旧主 a 可提交最终位点。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 1, Offset: 100, RequestID: "f1",
	}); err != nil {
		t.Fatalf("final commit before ack: %v", err)
	}
	// 新主 b 确认前拿不到分区 1，提交被拒。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "b", Generation: 2, Partition: 1, Offset: 101, RequestID: "f2",
	}); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("new owner early commit: %v", err)
	}
	// 未受影响分区 2（仍归 a）全程可提交。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 2, Offset: 7, RequestID: "f3",
	}); err != nil {
		t.Fatalf("unaffected partition commit: %v", err)
	}

	mustAck(t, c, "g", "a", 2, []int{1, 3})

	// 转移完成：旧主 a 对分区 1 的迟到提交被栅栏阻止（幂等重放也不例外）。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 1, Offset: 100, RequestID: "f1",
	}); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("old owner replay after transfer must be fenced, got %v", err)
	}
	// 新主 b 从最终位点之后继续提交成功。
	res, err := c.CommitOffset("g", CommitRequest{
		MemberID: "b", Generation: 2, Partition: 1, Offset: 101, RequestID: "f4",
	})
	if err != nil || res.Offset != 101 {
		t.Fatalf("new owner commit after transfer = %+v, %v", res, err)
	}
}

// ---- 并发：心跳/离开/超时/确认只能推动一条单调状态机 ----

func TestConcurrentAcksHeartbeatLeaveMonotonic(t *testing.T) {
	clk := newFakeClock()
	c, _ := NewCoordinator(nil, clk)
	if err := c.CreateGroup(CreateGroupOptions{
		Name: "g", Partitions: 12, SessionTimeout: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	// 6 个成员依次加入；每个新成员制造一次再均衡，全部先不确认，
	// 然后把当前版本需要的确认补齐到 stable，再加入下一个，制造交错素材。
	var wg sync.WaitGroup
	mustJoin(t, c, "g", "m0")
	for i := 1; i < 6; i++ {
		id := fmt.Sprintf("m%d", i)
		mustJoin(t, c, "g", id)
	}
	st, _ := c.Status("g")
	gen := st.Generation

	// 并发：所有有义务的成员用「当前版本 + 自己的义务集合」反复确认（含重复重试），
	// 其余成员并发心跳；一个旁观者反复查询状态。整个过程状态机不得被破坏。
	obligations := make(map[string][]int, len(st.PendingRevocations))
	for id, ps := range st.PendingRevocations {
		obligations[id] = append([]int(nil), ps...)
	}
	for id, ps := range obligations {
		wg.Add(1)
		go func(id string, ps []int) {
			defer wg.Done()
			for attempt := 0; attempt < 5; attempt++ {
				_, _ = c.AckRevocation("g", RevocationAckRequest{
					MemberID: id, Generation: gen, Partitions: ps,
				})
			}
		}(id, ps)
	}
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("m%d", i)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_, _ = c.Heartbeat("g", id, gen)
		}(id)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_, _ = c.Status("g")
		}
	}()
	wg.Wait()

	final, _ := c.Status("g")
	if final.Generation != gen {
		t.Fatalf("generation changed without membership events: %d -> %d", gen, final.Generation)
	}
	if final.Phase != PhaseStable {
		t.Fatalf("phase = %s, want stable after all acks", final.Phase)
	}
	// 生效所有权必须等于目标所有权。
	if !equalStrings(final.Assignment.Owners, final.TargetAssignment.Owners) {
		t.Fatalf("effective=%v target=%v", final.Assignment.Owners, final.TargetAssignment.Owners)
	}
	// 生效所有权恰好是 6 个成员的确定性分配。
	if !assignmentMatchesMembers(final.Assignment, final.MemberIDs()) {
		t.Fatalf("final assignment inconsistent: %v", final.Assignment.Owners)
	}

	// 再叠加一个并发离开：版本只允许 +1，且离开成员分区被强制回收，
	// 其余在途分区状态仍自洽。
	leaving := ""
	for id := range obligations {
		leaving = id
		break
	}
	if leaving == "" { // 全员无义务（成员数>分区等极端情形）时跳过
		return
	}
	if err := c.Leave("g", leaving); err != nil {
		t.Fatal(err)
	}
	post, _ := c.Status("g")
	if post.Generation != gen+1 {
		t.Fatalf("generation after leave = %d, want %d", post.Generation, gen+1)
	}
	memberSet := make(map[string]bool)
	for _, m := range post.Members {
		memberSet[m.ID] = true
	}
	for p, eff := range post.Assignment.Owners {
		if eff != "" && !memberSet[eff] {
			t.Fatalf("partition %d effective owner %q is gone", p, eff)
		}
		if eff != post.TargetAssignment.Owners[p] {
			found := false
			for _, q := range post.PendingRevocations[eff] {
				if q == p {
					found = true
				}
			}
			if !found {
				t.Fatalf("partition %d in flight but not pending for %q", p, eff)
			}
		}
	}
}

// ---- 撤销状态的内存持久化往返 ----

func TestRevocationStateMemoryRoundTrip(t *testing.T) {
	store := NewMemoryStore()
	c, _ := NewCoordinator(store, newFakeClock())
	if err := c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 6}); err != nil {
		t.Fatal(err)
	}
	mustJoin(t, c, "g", "a")
	mustJoin(t, c, "g", "b")
	settle(t, c, "g")        // 收敛 gen2：[a b a b a b]
	mustJoin(t, c, "g", "c") // gen3 revoking：a 撤销 {2,4}、b 撤销 {3,5}
	st, _ := c.Status("g")
	if st.Phase != PhaseRevoking || len(st.PendingRevocations) != 2 {
		t.Fatalf("phase = %s pending = %+v", st.Phase, st.PendingRevocations)
	}

	// 用同一内存存储重建：撤销义务/进度、生效与目标所有权全部还原。
	c2, err := NewCoordinator(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := c2.Status("g")
	if err != nil {
		t.Fatal(err)
	}
	if r2.Generation != st.Generation || r2.Phase != PhaseRevoking {
		t.Fatalf("recovered gen=%d phase=%s", r2.Generation, r2.Phase)
	}
	if !equalStrings(r2.Assignment.Owners, st.Assignment.Owners) ||
		!equalStrings(r2.TargetAssignment.Owners, st.TargetAssignment.Owners) {
		t.Fatalf("recovered ownership effective=%v target=%v",
			r2.Assignment.Owners, r2.TargetAssignment.Owners)
	}
	if len(r2.PendingRevocations) != len(st.PendingRevocations) {
		t.Fatalf("recovered pending = %+v, want %+v",
			r2.PendingRevocations, st.PendingRevocations)
	}
	for id, want := range st.PendingRevocations {
		if !equalInts(r2.PendingRevocations[id], want) {
			t.Fatalf("recovered pending[%s] = %v, want %v", id, r2.PendingRevocations[id], want)
		}
	}

	// 部分确认后再重建：Acked 子集也被还原。
	var firstMember string
	for id := range r2.PendingRevocations {
		firstMember = id
		break
	}
	ps := append([]int(nil), r2.PendingRevocations[firstMember]...)
	if _, err := c2.AckRevocation("g", RevocationAckRequest{
		MemberID: firstMember, Generation: r2.Generation, Partitions: ps,
	}); err != nil {
		t.Fatal(err)
	}
	c3, err := NewCoordinator(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	r3, _ := c3.Status("g")
	if r3.Phase != PhaseRevoking {
		t.Fatalf("phase after partial ack recovery = %s", r3.Phase)
	}
	var foundDone bool
	for _, p := range r3.RevocationProgress {
		if p.MemberID == firstMember && p.Done && equalInts(p.Acked, ps) {
			foundDone = true
		}
	}
	if !foundDone {
		t.Fatalf("acked progress not recovered: %+v", r3.RevocationProgress)
	}
}

// 撤销阶段目标新所有者先行离开：尚未取得的分区不应卡住，旧主继续持有，
// 新版本立即收敛且无待撤销分区。
func TestNewOwnerLeavesDuringRevocation(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4})
	mustJoin(t, c, "g", "a") // gen1 stable [a a a a]
	jb := mustJoin(t, c, "g", "b")
	if jb.Phase != PhaseRevoking { // gen2 revoking target [a b a b]，a 须撤销 {1,3}
		t.Fatalf("phase = %s", jb.Phase)
	}
	// a 尚未确认，b 就离开：gen3 成员只剩 a，target 回到 [a a a a]，
	// 分区 1、3 从未离开 a，转移取消，立即 stable。
	if err := c.Leave("g", "b"); err != nil {
		t.Fatal(err)
	}
	st, _ := c.Status("g")
	if st.Generation != 3 || st.Phase != PhaseStable {
		t.Fatalf("after new owner leaves: gen=%d phase=%s", st.Generation, st.Phase)
	}
	if !equalStrings(st.Assignment.Owners, []string{"a", "a", "a", "a"}) {
		t.Fatalf("owners = %v", st.Assignment.Owners)
	}
	if len(st.PendingRevocations) != 0 {
		t.Fatalf("pending should be empty: %v", st.PendingRevocations)
	}
	// a 用旧版本 2 的迟到确认必须被拒绝（版本已前进），且不影响状态。
	if _, err := c.AckRevocation("g", RevocationAckRequest{
		MemberID: "a", Generation: 2, Partitions: []int{1, 3},
	}); !errors.Is(err, ErrIllegalGeneration) {
		t.Fatalf("stale ack after target left: %v", err)
	}
}
