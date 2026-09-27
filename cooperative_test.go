package consumergroups

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// ---- 协作式再均衡：目标所有权与待撤销集合 ----

// 成员变化只生成目标所有权与待撤销集合：旧所有者确认前新所有者不能取得
// 分区，未受影响分区继续由原成员消费。
func TestCooperativeRebalancePhases(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4})
	mustJoin(t, c, "g", "a")       // gen 1: [a a a a]，无待撤销
	r2 := mustJoin(t, c, "g", "b") // gen 2: 目标 [a b a b]，分区 1、3 待撤销

	if r2.Generation != 2 {
		t.Fatalf("generation = %d, want 2", r2.Generation)
	}
	// 目标所有权立即发布。
	if want := []string{"a", "b", "a", "b"}; !equalStrings(r2.Target.Owners, want) {
		t.Fatalf("target owners = %v, want %v", r2.Target.Owners, want)
	}
	// 有效所有权：撤销确认前全部仍归 a，未受影响分区（0、2）不受影响。
	if want := []string{"a", "a", "a", "a"}; !equalStrings(r2.Assignment.Owners, want) {
		t.Fatalf("effective owners before ack = %v, want %v", r2.Assignment.Owners, want)
	}
	if r2.RebalanceComplete {
		t.Fatal("rebalance should not be complete with pending revocations")
	}

	st, _ := c.Status("g")
	wantPending := []PartitionRevocation{{Partition: 1, From: "a", To: "b"}, {Partition: 3, From: "a", To: "b"}}
	if fmt.Sprintf("%v", st.PendingRevocations) != fmt.Sprintf("%v", wantPending) {
		t.Fatalf("pending revocations = %v, want %v", st.PendingRevocations, wantPending)
	}

	// 未受影响分区：原成员 a 继续消费并提交。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 0, Offset: 10, RequestID: "u1",
	}); err != nil {
		t.Fatalf("commit on unaffected partition: %v", err)
	}
	// 待撤销分区：旧所有者 a 可提交最终位点。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 1, Offset: 20, RequestID: "u2",
	}); err != nil {
		t.Fatalf("final commit on pending partition: %v", err)
	}
	// 新所有者 b 在确认前不能取得分区。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "b", Generation: 2, Partition: 1, Offset: 21, RequestID: "u3",
	}); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("new owner commit before ack should fail with ErrNotOwner, got %v", err)
	}

	// a 确认撤销，分区 1、3 转移给 b，再均衡完成。
	res, err := c.AcknowledgeRevocation("g", RevocationAck{MemberID: "a", Generation: 2, Partitions: []int{3, 1}})
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if !res.RebalanceComplete || !equalInts(res.Transferred, []int{1, 3}) {
		t.Fatalf("ack result = %+v, want transferred [1 3] complete", res)
	}
	st, _ = c.Status("g")
	if want := []string{"a", "b", "a", "b"}; !equalStrings(st.Assignment.Owners, want) {
		t.Fatalf("effective owners after ack = %v, want %v", st.Assignment.Owners, want)
	}
	if !st.RebalanceComplete || len(st.PendingRevocations) != 0 {
		t.Fatalf("rebalance should be complete: %+v", st.PendingRevocations)
	}

	// 转移完成后：a 的迟到提交被栅栏，b 可以提交。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 1, Offset: 22, RequestID: "u4",
	}); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("late commit from former owner should be fenced, got %v", err)
	}
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "b", Generation: 2, Partition: 1, Offset: 22, RequestID: "u5",
	}); err != nil {
		t.Fatalf("new owner commit after transfer: %v", err)
	}
}

// 撤销确认必须精确匹配组、成员、版本与分区集合：
// 漏项、额外分区、旧版本、非成员都明确拒绝，且不会推进再均衡。
func TestAckValidation(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4})
	mustJoin(t, c, "g", "a") // gen 1
	mustJoin(t, c, "g", "b") // gen 2: a 待撤销 [1 3]

	// 旧版本确认。
	if _, err := c.AcknowledgeRevocation("g", RevocationAck{
		MemberID: "a", Generation: 1, Partitions: []int{1, 3},
	}); !errors.Is(err, ErrIllegalGeneration) {
		t.Fatalf("stale generation ack should fail with ErrIllegalGeneration, got %v", err)
	}
	// 超前版本确认。
	if _, err := c.AcknowledgeRevocation("g", RevocationAck{
		MemberID: "a", Generation: 99, Partitions: []int{1, 3},
	}); !errors.Is(err, ErrIllegalGeneration) {
		t.Fatalf("future generation ack should fail with ErrIllegalGeneration, got %v", err)
	}
	// 非成员确认。
	if _, err := c.AcknowledgeRevocation("g", RevocationAck{
		MemberID: "ghost", Generation: 2, Partitions: []int{1, 3},
	}); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("unknown member ack should fail with ErrMemberNotFound, got %v", err)
	}
	// 分区越界。
	if _, err := c.AcknowledgeRevocation("g", RevocationAck{
		MemberID: "a", Generation: 2, Partitions: []int{1, 3, 9},
	}); !errors.Is(err, ErrInvalidPartition) {
		t.Fatalf("out-of-range ack should fail with ErrInvalidPartition, got %v", err)
	}
	// 漏项。
	_, err := c.AcknowledgeRevocation("g", RevocationAck{MemberID: "a", Generation: 2, Partitions: []int{1}})
	rme := requireError[*RevocationMismatchError](t, err, ErrRevocationMismatch)
	if !equalInts(rme.Missing, []int{3}) || len(rme.Extra) != 0 {
		t.Fatalf("mismatch fields = %+v, want missing [3]", rme)
	}
	// 额外分区。
	_, err = c.AcknowledgeRevocation("g", RevocationAck{MemberID: "a", Generation: 2, Partitions: []int{1, 2, 3}})
	rme = requireError[*RevocationMismatchError](t, err, ErrRevocationMismatch)
	if !equalInts(rme.Extra, []int{2}) || len(rme.Missing) != 0 {
		t.Fatalf("mismatch fields = %+v, want extra [2]", rme)
	}
	// 对无撤销义务的成员确认。
	if _, err := c.AcknowledgeRevocation("g", RevocationAck{
		MemberID: "b", Generation: 2, Partitions: []int{1},
	}); !errors.Is(err, ErrRevocationMismatch) {
		t.Fatalf("ack from member without revocations should mismatch, got %v", err)
	}
	// 不存在的组。
	if _, err := c.AcknowledgeRevocation("nope", RevocationAck{
		MemberID: "a", Generation: 2, Partitions: []int{1},
	}); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("unknown group ack should fail with ErrGroupNotFound, got %v", err)
	}

	// 上述失败确认都不得推进再均衡：待撤销集合原样保留。
	st, _ := c.Status("g")
	if st.RebalanceComplete || len(st.PendingRevocations) != 2 {
		t.Fatalf("failed acks must not advance rebalance: %+v", st.PendingRevocations)
	}
	if want := []string{"a", "a", "a", "a"}; !equalStrings(st.Assignment.Owners, want) {
		t.Fatalf("effective owners after failed acks = %v, want %v", st.Assignment.Owners, want)
	}
}

// 确认可重试：相同内容重放原样成功；并发重复确认也只生效一次。
func TestAckIdempotentRetry(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4})
	mustJoin(t, c, "g", "a")
	mustJoin(t, c, "g", "b") // gen 2: a 待撤销 [1 3]

	res, err := c.AcknowledgeRevocation("g", RevocationAck{MemberID: "a", Generation: 2, Partitions: []int{1, 3}})
	if err != nil || !res.RebalanceComplete {
		t.Fatalf("first ack = %+v, %v", res, err)
	}
	// 相同内容重试（顺序不同）：成功但不再转移。
	res, err = c.AcknowledgeRevocation("g", RevocationAck{MemberID: "a", Generation: 2, Partitions: []int{3, 1}})
	if err != nil {
		t.Fatalf("retry ack: %v", err)
	}
	if len(res.Transferred) != 0 || !res.RebalanceComplete {
		t.Fatalf("retry ack result = %+v, want no transfer", res)
	}
	// 空确认：无撤销义务，视为无操作成功。
	if _, err := c.AcknowledgeRevocation("g", RevocationAck{MemberID: "a", Generation: 2}); err != nil {
		t.Fatalf("empty ack should be a no-op success, got %v", err)
	}
	// 已完成后改交不同集合：拒绝。
	if _, err := c.AcknowledgeRevocation("g", RevocationAck{
		MemberID: "a", Generation: 2, Partitions: []int{1},
	}); !errors.Is(err, ErrRevocationMismatch) {
		t.Fatalf("different set after completion should mismatch, got %v", err)
	}
	// 位点只被转移后的新所有者推进一次语义不变。
	st, _ := c.Status("g")
	if want := []string{"a", "b", "a", "b"}; !equalStrings(st.Assignment.Owners, want) {
		t.Fatalf("owners after retries = %v, want %v", st.Assignment.Owners, want)
	}
}

// 并发重复确认同一份撤销集合：都成功，分区只转移一次。
func TestConcurrentDuplicateAcks(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4})
	mustJoin(t, c, "g", "a")
	mustJoin(t, c, "g", "b") // gen 2: a 待撤销 [1 3]

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = c.AcknowledgeRevocation("g", RevocationAck{
				MemberID: "a", Generation: 2, Partitions: []int{1, 3},
			})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent ack %d failed: %v", i, err)
		}
	}
	st, _ := c.Status("g")
	if !st.RebalanceComplete {
		t.Fatal("rebalance should be complete after duplicate acks")
	}
	if want := []string{"a", "b", "a", "b"}; !equalStrings(st.Assignment.Owners, want) {
		t.Fatalf("owners = %v, want %v", st.Assignment.Owners, want)
	}
}

// 多个成员各自负有撤销义务时，任何一方的确认都不能提前完成整个再均衡。
func TestAckDoesNotCompleteRebalancePrematurely(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4})
	mustJoin(t, c, "g", "a") // gen 1
	mustJoin(t, c, "g", "b") // gen 2
	settleRebalance(t, c, "g")
	mustJoin(t, c, "g", "c") // gen 3: 目标 [a b c a]；a 待撤销 [2]，b 待撤销 [3]

	st, _ := c.Status("g")
	if want := []string{"a", "b", "c", "a"}; !equalStrings(st.Target.Owners, want) {
		t.Fatalf("target = %v, want %v", st.Target.Owners, want)
	}

	// a 确认自己的部分：再均衡未完成（b 还欠分区 3）。
	res, err := c.AcknowledgeRevocation("g", RevocationAck{MemberID: "a", Generation: 3, Partitions: []int{2}})
	if err != nil {
		t.Fatalf("ack a: %v", err)
	}
	if res.RebalanceComplete {
		t.Fatal("rebalance must not complete while b still owes revocations")
	}
	st, _ = c.Status("g")
	// 分区 2 已转移给 c，分区 3 仍归 b。
	if want := []string{"a", "b", "c", "b"}; !equalStrings(st.Assignment.Owners, want) {
		t.Fatalf("owners after partial ack = %v, want %v", st.Assignment.Owners, want)
	}

	// b 确认后整个再均衡完成。
	res, err = c.AcknowledgeRevocation("g", RevocationAck{MemberID: "b", Generation: 3, Partitions: []int{3}})
	if err != nil || !res.RebalanceComplete {
		t.Fatalf("ack b = %+v, %v", res, err)
	}
	st, _ = c.Status("g")
	if want := []string{"a", "b", "c", "a"}; !equalStrings(st.Assignment.Owners, want) {
		t.Fatalf("owners after full ack = %v, want %v", st.Assignment.Owners, want)
	}
}

// 成员在撤销阶段主动离开：其剩余待撤销分区被协调器强制回收。
func TestLeaveDuringRevocationForceReclaims(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4})
	mustJoin(t, c, "g", "a")
	mustJoin(t, c, "g", "b") // gen 2: a 待撤销 [1 3]

	if err := c.Leave("g", "a"); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	st, _ := c.Status("g")
	if st.Generation != 3 {
		t.Fatalf("generation = %d, want 3", st.Generation)
	}
	// a 的全部分区（含待撤销的 1、3 与未受影响的 0、2）被强制回收给 b。
	if want := []string{"b", "b", "b", "b"}; !equalStrings(st.Assignment.Owners, want) {
		t.Fatalf("owners after force reclaim = %v, want %v", st.Assignment.Owners, want)
	}
	if !st.RebalanceComplete {
		t.Fatalf("rebalance should be complete after force reclaim: %+v", st.PendingRevocations)
	}
	// 强制回收进度可查。
	var prog *MemberAckProgress
	for i := range st.AckProgress {
		if st.AckProgress[i].MemberID == "a" {
			prog = &st.AckProgress[i]
		}
	}
	if prog == nil || !equalInts(prog.ForceReclaimed, []int{0, 1, 2, 3}) {
		t.Fatalf("ack progress for a = %+v, want force reclaimed [0 1 2 3]", prog)
	}
	// 离开成员的迟到确认被拒绝。
	if _, err := c.AcknowledgeRevocation("g", RevocationAck{
		MemberID: "a", Generation: 3, Partitions: []int{1, 3},
	}); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("late ack from departed member should fail, got %v", err)
	}
}

// 成员在撤销阶段超时：超时扫描强制回收其剩余待撤销分区。
func TestExpireDuringRevocationForceReclaims(t *testing.T) {
	clk := newFakeClock()
	c, _ := NewCoordinator(nil, clk)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4, SessionTimeout: 5 * time.Second})
	mustJoin(t, c, "g", "a")
	mustJoin(t, c, "g", "b") // gen 2: a 待撤销 [1 3]

	// b 保持心跳，a 停滞直至超时。
	clk.Advance(6 * time.Second)
	if _, err := c.Heartbeat("g", "b", 2); err != nil {
		t.Fatal(err)
	}
	expired, err := c.ExpireGroup("g", clk.Now())
	if err != nil || !equalStrings(expired, []string{"a"}) {
		t.Fatalf("expired = %v, %v", expired, err)
	}
	st, _ := c.Status("g")
	if want := []string{"b", "b", "b", "b"}; !equalStrings(st.Assignment.Owners, want) {
		t.Fatalf("owners after expiry reclaim = %v, want %v", st.Assignment.Owners, want)
	}
	if !st.RebalanceComplete {
		t.Fatal("rebalance should be complete after expiry reclaim")
	}
}

// 心跳与加入的返回值携带目标所有权与本成员待撤销分区，供客户端驱动确认。
func TestHeartbeatAndJoinReportRevocations(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4})
	mustJoin(t, c, "g", "a")
	r2 := mustJoin(t, c, "g", "b") // gen 2
	if len(r2.Revocations) != 0 {
		t.Fatalf("new member should have no revocations, got %v", r2.Revocations)
	}

	hb, err := c.Heartbeat("g", "a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !equalInts(hb.Revocations, []int{1, 3}) {
		t.Fatalf("heartbeat revocations = %v, want [1 3]", hb.Revocations)
	}
	if hb.RebalanceComplete {
		t.Fatal("heartbeat should report incomplete rebalance")
	}
	if want := []string{"a", "b", "a", "b"}; !equalStrings(hb.Target.Owners, want) {
		t.Fatalf("heartbeat target = %v, want %v", hb.Target.Owners, want)
	}

	// 确认后心跳不再报告待撤销分区。
	if _, err := c.AcknowledgeRevocation("g", RevocationAck{MemberID: "a", Generation: 2, Partitions: []int{1, 3}}); err != nil {
		t.Fatal(err)
	}
	hb, err = c.Heartbeat("g", "a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(hb.Revocations) != 0 || !hb.RebalanceComplete {
		t.Fatalf("heartbeat after ack = %+v, want no revocations and complete", hb)
	}
}

// Status 暴露当前所有权、目标所有权、待撤销集合与各成员确认进度。
func TestStatusExposesCooperativeState(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4})
	mustJoin(t, c, "g", "a")
	mustJoin(t, c, "g", "b")
	settleRebalance(t, c, "g")
	mustJoin(t, c, "g", "c") // gen 3: a 待撤销 [2]，b 待撤销 [3]

	st, _ := c.Status("g")
	if st.RebalanceComplete {
		t.Fatal("rebalance should be in progress")
	}
	wantPending := []PartitionRevocation{{Partition: 2, From: "a", To: "c"}, {Partition: 3, From: "b", To: "a"}}
	if fmt.Sprintf("%v", st.PendingRevocations) != fmt.Sprintf("%v", wantPending) {
		t.Fatalf("pending = %v, want %v", st.PendingRevocations, wantPending)
	}
	if len(st.AckProgress) != 2 {
		t.Fatalf("ack progress = %+v, want 2 entries", st.AckProgress)
	}
	if pa := st.AckProgress[0]; pa.MemberID != "a" ||
		!equalInts(pa.Required, []int{2}) || !equalInts(pa.Outstanding, []int{2}) || len(pa.Acked) != 0 {
		t.Fatalf("progress a = %+v", pa)
	}
	if pb := st.AckProgress[1]; pb.MemberID != "b" ||
		!equalInts(pb.Required, []int{3}) || !equalInts(pb.Outstanding, []int{3}) {
		t.Fatalf("progress b = %+v", pb)
	}

	// a 确认后：进度反映 Acked，b 仍 Outstanding。
	if _, err := c.AcknowledgeRevocation("g", RevocationAck{MemberID: "a", Generation: 3, Partitions: []int{2}}); err != nil {
		t.Fatal(err)
	}
	st, _ = c.Status("g")
	if pa := st.AckProgress[0]; !equalInts(pa.Acked, []int{2}) || len(pa.Outstanding) != 0 {
		t.Fatalf("progress a after ack = %+v", pa)
	}
	if pb := st.AckProgress[1]; !equalInts(pb.Outstanding, []int{3}) {
		t.Fatalf("progress b after a ack = %+v", pb)
	}
}

// 并发心跳 / 确认 / 离开 / 超时扫描只推动一条单调的版本状态机，
// 最终再均衡收敛且有效所有权与成员集合一致。
func TestConcurrentCooperativeRebalance(t *testing.T) {
	clk := newFakeClock()
	store := newRecordingStore(NewMemoryStore())
	c, err := NewCoordinator(store, clk)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 8, SessionTimeout: time.Hour})

	const n = 4
	for i := 0; i < n; i++ {
		mustJoin(t, c, "g", fmt.Sprintf("m%d", i))
	}

	var wg sync.WaitGroup
	// 离开完成后关闭 left：客户端循环只有在离开已发生且再均衡完成时才退出，
	// 否则离开触发的新一轮撤销可能无人确认。
	left := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = c.Leave("g", "m3")
		close(left)
	}()
	// 行为良好的客户端循环：心跳 -> 按返回值确认撤销。
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for {
				st, err := c.Status("g")
				if err != nil {
					return
				}
				select {
				case <-left:
					if st.RebalanceComplete {
						return
					}
				default:
				}
				hb, err := c.Heartbeat("g", id, st.Generation)
				if errors.Is(err, ErrMemberNotFound) {
					return
				}
				if err != nil {
					continue // 版本已前进，重读状态
				}
				if len(hb.Revocations) > 0 {
					_, _ = c.AcknowledgeRevocation("g", RevocationAck{
						MemberID: id, Generation: hb.Generation, Partitions: hb.Revocations,
					})
				}
				time.Sleep(time.Millisecond)
			}
		}(fmt.Sprintf("m%d", i))
	}
	// 并发超时扫描。
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = c.ExpireAll(clk.Now())
	}()
	wg.Wait()

	st, err := c.Status("g")
	if err != nil {
		t.Fatal(err)
	}
	if !st.RebalanceComplete {
		t.Fatalf("rebalance should converge, pending = %+v", st.PendingRevocations)
	}
	if !assignmentMatchesMembers(st.Assignment, st.MemberIDs()) {
		t.Fatalf("final assignment inconsistent: owners=%v members=%v",
			st.Assignment.Owners, st.MemberIDs())
	}
	// 持久化的版本序列单调不减。
	records := store.records()
	var prev int64
	for i, rec := range records {
		g := rec["g"]
		if i > 0 && g.generation < prev {
			t.Fatalf("persisted generation went backwards at save %d: %d -> %d", i, prev, g.generation)
		}
		prev = g.generation
	}
}

// 再均衡中途的协调状态（目标、待撤销、确认进度）持久化并可在重启后恢复，
// 恢复后确认与提交栅栏继续生效。
func TestCooperativeStatePersistedAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	clk := newFakeClock()

	c, err := NewCoordinator(NewFileStore(path), clk)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4})
	mustJoin(t, c, "g", "a")
	mustJoin(t, c, "g", "b")
	settleRebalance(t, c, "g")
	mustJoin(t, c, "g", "c") // gen 3: a 待撤销 [2]，b 待撤销 [3]
	// a 确认，b 不确认：制造再均衡中途状态。
	if _, err := c.AcknowledgeRevocation("g", RevocationAck{MemberID: "a", Generation: 3, Partitions: []int{2}}); err != nil {
		t.Fatal(err)
	}

	// 模拟重启。
	c2, err := NewCoordinator(NewFileStore(path), clk)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	st, err := c2.Status("g")
	if err != nil {
		t.Fatal(err)
	}
	if st.Generation != 3 {
		t.Fatalf("recovered generation = %d, want 3", st.Generation)
	}
	if want := []string{"a", "b", "c", "a"}; !equalStrings(st.Target.Owners, want) {
		t.Fatalf("recovered target = %v, want %v", st.Target.Owners, want)
	}
	if want := []string{"a", "b", "c", "b"}; !equalStrings(st.Assignment.Owners, want) {
		t.Fatalf("recovered effective owners = %v, want %v", st.Assignment.Owners, want)
	}
	wantPending := []PartitionRevocation{{Partition: 3, From: "b", To: "a"}}
	if fmt.Sprintf("%v", st.PendingRevocations) != fmt.Sprintf("%v", wantPending) {
		t.Fatalf("recovered pending = %v, want %v", st.PendingRevocations, wantPending)
	}
	// a 的确认进度被保留：重试 a 的确认按幂等成功，不再转移。
	res, err := c2.AcknowledgeRevocation("g", RevocationAck{MemberID: "a", Generation: 3, Partitions: []int{2}})
	if err != nil || len(res.Transferred) != 0 {
		t.Fatalf("re-ack after recovery = %+v, %v", res, err)
	}
	// b 在恢复后确认，再均衡完成。
	res, err = c2.AcknowledgeRevocation("g", RevocationAck{MemberID: "b", Generation: 3, Partitions: []int{3}})
	if err != nil || !res.RebalanceComplete {
		t.Fatalf("ack b after recovery = %+v, %v", res, err)
	}
	// 提交栅栏在恢复后依然有效：b 对分区 3 的迟到提交被拒绝。
	if _, err := c2.CommitOffset("g", CommitRequest{
		MemberID: "b", Generation: 3, Partition: 3, Offset: 1, RequestID: "late",
	}); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("late commit after recovery should be fenced, got %v", err)
	}
}

// 旧版本（v1，无协作式字段）快照仍可加载：按再均衡已完成处理。
func TestLegacyV1SnapshotLoads(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	legacy := `{
  "version": 1,
  "groups": [
    {
      "name": "g",
      "partitions": 2,
      "session_timeout_ns": 30000000000,
      "generation": 1,
      "leader": "a",
      "last_rebalance": "2026-01-01T00:00:00Z",
      "members": [
        {"id": "a", "joined_at": "2026-01-01T00:00:00Z", "last_heartbeat_at": "2026-01-01T00:00:00Z", "requests": []}
      ],
      "assignment": {"generation": 1, "created_at": "2026-01-01T00:00:00Z", "owners": ["a", "a"]},
      "offsets": []
    }
  ]
}`
	if err := os.WriteFile(path, []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := NewCoordinator(NewFileStore(path), nil)
	if err != nil {
		t.Fatalf("load legacy snapshot: %v", err)
	}
	st, err := c.Status("g")
	if err != nil {
		t.Fatal(err)
	}
	if st.Generation != 1 || !st.RebalanceComplete {
		t.Fatalf("legacy status = %+v", st)
	}
	if !equalStrings(st.Target.Owners, []string{"a", "a"}) {
		t.Fatalf("legacy target should mirror assignment, got %v", st.Target.Owners)
	}
	// 版本序列无缝延续。
	r := mustJoin(t, c, "g", "b")
	if r.Generation != 2 {
		t.Fatalf("generation after legacy load = %d, want 2", r.Generation)
	}
}
