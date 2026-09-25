package consumergroups

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---- 组管理 ----

func TestCreateGroupAndStatus(t *testing.T) {
	c, err := NewCoordinator(nil, nil)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	if err := c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	st, err := c.Status("g")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Generation != 0 || st.Partitions != 4 {
		t.Fatalf("unexpected initial status: %+v", st)
	}
	if len(st.Assignment.Owners) != 4 {
		t.Fatalf("expected 4 empty owner slots, got %v", st.Assignment.Owners)
	}
	if got := c.Groups(); len(got) != 1 || got[0] != "g" {
		t.Fatalf("Groups() = %v", got)
	}

	if err := c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 1}); !errors.Is(err, ErrGroupAlreadyExists) {
		t.Fatalf("duplicate create should fail with ErrGroupAlreadyExists, got %v", err)
	}
	for _, bad := range []CreateGroupOptions{
		{Name: "", Partitions: 1},
		{Name: "x", Partitions: 0},
		{Name: "x", Partitions: -1},
	} {
		if err := c.CreateGroup(bad); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("CreateGroup(%+v) should fail with ErrInvalidArgument, got %v", bad, err)
		}
	}
	if _, err := c.Status("missing"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("missing group Status should fail with ErrGroupNotFound, got %v", err)
	}
}

// ---- 分配版本与确定性分配 ----

func TestJoinProducesDeterministicAssignment(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	if err := c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 5}); err != nil {
		t.Fatal(err)
	}

	r1 := mustJoin(t, c, "g", "charlie")
	if r1.Generation != 1 || r1.Leader != "charlie" {
		t.Fatalf("first join: %+v", r1)
	}
	want := []string{"charlie", "charlie", "charlie", "charlie", "charlie"}
	if got := r1.Assignment.Owners; !equalStrings(got, want) {
		t.Fatalf("gen 1 owners = %v, want %v", got, want)
	}

	// 乱序加入，但分配按成员 ID 字典序确定性产生。
	mustJoin(t, c, "g", "alpha")
	mustJoin(t, c, "g", "bravo")
	st, _ := c.Status("g")
	if st.Generation != 3 {
		t.Fatalf("generation after 3 joins = %d, want 3", st.Generation)
	}
	// 排序后 [alpha bravo charlie]：分区 i -> members[i%3]
	want = []string{"alpha", "bravo", "charlie", "alpha", "bravo"}
	if got := st.Assignment.Owners; !equalStrings(got, want) {
		t.Fatalf("gen 3 owners = %v, want %v", got, want)
	}
	if st.Leader != "charlie" { // 第一个加入者
		t.Fatalf("leader = %q, want charlie", st.Leader)
	}

	// 每次 Join 返回的 Assignment 都是一份完整快照（同一版本内一分区一主）。
	if !assignmentIsExclusive(st.Assignment, 5) {
		t.Fatalf("assignment violates exclusivity: %v", st.Assignment.Owners)
	}

	// 重复加入。
	if _, err := c.Join("g", "alpha"); !errors.Is(err, ErrMemberAlreadyExists) {
		t.Fatalf("duplicate join should fail, got %v", err)
	}
}

// assignmentIsExclusive 校验同一版本内一个分区至多归属一个成员，
// 且 Owners 中不存在跨分区以外的重复归属冲突（天然满足，这里校验长度与槽位）。
func assignmentIsExclusive(a Assignment, partitions int) bool {
	if len(a.Owners) != partitions {
		return false
	}
	counts := make(map[string]int)
	for _, owner := range a.Owners {
		if owner != "" {
			counts[owner]++
		}
	}
	// 每个成员的分区数差值不超过 1（均匀分配）。
	min, max := -1, 0
	for _, n := range counts {
		if min == -1 || n < min {
			min = n
		}
		if n > max {
			max = n
		}
	}
	return max-min <= 1
}

func TestLeaveRebalancesAndIncrementsGeneration(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4})
	mustJoin(t, c, "g", "a") // gen 1
	mustJoin(t, c, "g", "b") // gen 2
	mustJoin(t, c, "g", "c") // gen 3

	if err := c.Leave("g", "a"); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	st, _ := c.Status("g")
	if st.Generation != 4 {
		t.Fatalf("generation after leave = %d, want 4", st.Generation)
	}
	want := []string{"b", "c", "b", "c"}
	if got := st.Assignment.Owners; !equalStrings(got, want) {
		t.Fatalf("owners after leave = %v, want %v", got, want)
	}
	if st.Leader != "b" { // 存活成员中加入最早
		t.Fatalf("leader = %q, want b", st.Leader)
	}

	if err := c.Leave("g", "a"); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("leaving unknown member should fail, got %v", err)
	}
}

func TestLastMemberLeavesToEmptyGeneration(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 2})
	mustJoin(t, c, "g", "a") // gen 1
	if err := c.Leave("g", "a"); err != nil {
		t.Fatal(err)
	}
	st, _ := c.Status("g")
	if st.Generation != 2 || st.Leader != "" {
		t.Fatalf("empty group status: %+v", st)
	}
	for i, owner := range st.Assignment.Owners {
		if owner != "" {
			t.Fatalf("partition %d should have no owner, got %q", i, owner)
		}
	}
	// 重新加入产生新版本，旧成员身份不会复活。
	r := mustJoin(t, c, "g", "a")
	if r.Generation != 3 {
		t.Fatalf("rejoin generation = %d, want 3", r.Generation)
	}
}

// ---- 心跳与版本隔离 ----

func TestHeartbeatAcceptsCurrentRejectsStaleGeneration(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 2})
	r1 := mustJoin(t, c, "g", "a") // gen 1
	mustJoin(t, c, "g", "b")       // gen 2

	// 当前版本心跳被接受。
	hb, err := c.Heartbeat("g", "b", 2)
	if err != nil {
		t.Fatalf("Heartbeat current generation: %v", err)
	}
	if hb.Generation != 2 || hb.Assignment.Generation != 2 {
		t.Fatalf("heartbeat result: %+v", hb)
	}

	// 旧版本心跳被拒绝，且成员不会被“复活”或改变状态。
	gm := requireError[*GenerationMismatchError](t,
		mustHeartbeatFail(c, "g", "a", r1.Generation), ErrIllegalGeneration)
	if gm.Want != 2 || gm.Got != 1 {
		t.Fatalf("mismatch fields = %+v", gm)
	}

	// 超前版本同样拒绝。
	_ = requireError[*GenerationMismatchError](t,
		mustHeartbeatFail(c, "g", "a", 99), ErrIllegalGeneration)

	// 非成员。
	if _, err := c.Heartbeat("g", "ghost", 2); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("heartbeat from unknown member: %v", err)
	}
}

func mustHeartbeatFail(c *Coordinator, group, member string, gen int64) error {
	_, err := c.Heartbeat(group, member, gen)
	return err
}

// 成员被新版本淘汰后，携带旧版本的迟到心跳不能复活成员。
func TestLateHeartbeatAfterExpiryCannotReviveMember(t *testing.T) {
	clk := newFakeClock()
	c, _ := NewCoordinator(nil, clk)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 3, SessionTimeout: 10 * time.Second})
	rA := mustJoin(t, c, "g", "a") // gen 1
	mustJoin(t, c, "g", "b")       // gen 2

	clk.Advance(11 * time.Second)
	// b 仍在心跳；a 超时。先让 b 心跳刷新，再扫描只剔除 a。
	if _, err := c.Heartbeat("g", "b", 2); err != nil {
		t.Fatal(err)
	}
	expired, err := c.ExpireGroup("g", clk.Now())
	if err != nil || len(expired) != 1 || expired[0] != "a" {
		t.Fatalf("ExpireGroup = %v, %v", expired, err)
	}
	st, _ := c.Status("g")
	if st.Generation != 3 {
		t.Fatalf("generation after expire = %d, want 3", st.Generation)
	}

	// a 的迟到心跳（携带 gen 1）：版本不匹配，拒绝。
	if _, err := c.Heartbeat("g", "a", rA.Generation); !errors.Is(err, ErrIllegalGeneration) {
		t.Fatalf("late stale heartbeat should be ErrIllegalGeneration, got %v", err)
	}
	// 即使伪造当前版本，成员也已不存在，不能复活。
	if _, err := c.Heartbeat("g", "a", 3); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("late heartbeat with current gen should be ErrMemberNotFound, got %v", err)
	}
	st2, _ := c.Status("g")
	if len(st2.Members) != 1 || st2.Members[0].ID != "b" {
		t.Fatalf("member a must not be revived, members=%v", st2.Members)
	}
}

// ---- 会话超时 ----

func TestSessionExpiry(t *testing.T) {
	clk := newFakeClock()
	c, _ := NewCoordinator(nil, clk)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 2, SessionTimeout: 5 * time.Second})
	mustJoin(t, c, "g", "a") // gen 1
	mustJoin(t, c, "g", "b") // gen 2

	// 未超时：不推进版本。
	clk.Advance(4 * time.Second)
	if expired, err := c.ExpireGroup("g", clk.Now()); err != nil || expired != nil {
		t.Fatalf("no expiry expected, expired=%v err=%v", expired, err)
	}
	st, _ := c.Status("g")
	if st.Generation != 2 {
		t.Fatalf("generation should stay 2, got %d", st.Generation)
	}

	// a 续期，b 不续期。
	if _, err := c.Heartbeat("g", "a", 2); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Second) // a: 2s ago, b: 6s ago
	expired, err := c.ExpireGroup("g", clk.Now())
	if err != nil || !equalStrings(expired, []string{"b"}) {
		t.Fatalf("expired = %v, %v", expired, err)
	}
	st, _ = c.Status("g")
	if st.Generation != 3 || !equalStrings(st.Assignment.Owners, []string{"a", "a"}) {
		t.Fatalf("post-expiry status gen=%d owners=%v", st.Generation, st.Assignment.Owners)
	}
}

func TestExpireAllAcrossGroups(t *testing.T) {
	clk := newFakeClock()
	c, _ := NewCoordinator(nil, clk)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g1", Partitions: 1, SessionTimeout: time.Second})
	_ = c.CreateGroup(CreateGroupOptions{Name: "g2", Partitions: 1, SessionTimeout: time.Second})
	mustJoin(t, c, "g1", "a")
	mustJoin(t, c, "g2", "b")
	clk.Advance(2 * time.Second)
	changed, err := c.ExpireAll(clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !equalStrings(changed, []string{"g1", "g2"}) {
		t.Fatalf("changed groups = %v", changed)
	}
	// 再次扫描为空，不产生冗余再均衡。
	changed2, _ := c.ExpireAll(clk.Now())
	if changed2 != nil {
		t.Fatalf("expected no changes on second scan, got %v", changed2)
	}
}

// ---- 位点提交：所有权、单调性、幂等性 ----

func TestCommitOffsetOwnerOnly(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4})
	mustJoin(t, c, "g", "a") // gen 1
	mustJoin(t, c, "g", "b") // gen 2: owners [a b a b]

	// 非所有者提交：b 不是分区 0 的所有者。
	_, err := c.CommitOffset("g", CommitRequest{
		MemberID: "b", Generation: 2, Partition: 0, Offset: 10, RequestID: "r1",
	})
	noe := requireError[*NotPartitionOwnerError](t, err, ErrNotOwner)
	if noe.Owner != "a" || noe.Member != "b" || noe.Partition != 0 {
		t.Fatalf("NotPartitionOwnerError fields = %+v", noe)
	}

	// 旧版本提交一律拒绝，哪怕提交者在旧版本里曾是所有者。
	_, err = c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 1, Partition: 1, Offset: 10, RequestID: "r2",
	})
	gm := requireError[*GenerationMismatchError](t, err, ErrIllegalGeneration)
	if gm.Want != 2 || gm.Got != 1 {
		t.Fatalf("generation mismatch = %+v", gm)
	}

	// 合法提交。
	res, err := c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 0, Offset: 10, RequestID: "r3",
	})
	if err != nil || res.Offset != 10 || res.Replayed {
		t.Fatalf("commit = %+v, %v", res, err)
	}

	// 非法分区。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 4, Offset: 1, RequestID: "r4",
	}); !errors.Is(err, ErrInvalidPartition) {
		t.Fatalf("out-of-range partition should fail, got %v", err)
	}
	// 缺少请求号。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 0, Offset: 11,
	}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing request id should fail, got %v", err)
	}
}

func TestCommitOffsetMonotonic(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 1})
	mustJoin(t, c, "g", "a") // gen 1

	commit := func(offset int64, rid string, allowBacktrack bool) error {
		_, err := c.CommitOffset("g", CommitRequest{
			MemberID: "a", Generation: 1, Partition: 0,
			Offset: offset, RequestID: rid, AllowBacktrack: allowBacktrack,
		})
		return err
	}

	if err := commit(100, "r1", false); err != nil {
		t.Fatal(err)
	}
	if err := commit(100, "r2", false); err != nil { // 等值允许
		t.Fatalf("equal offset commit should succeed, got %v", err)
	}
	err := commit(99, "r3", false)
	obe := requireError[*OffsetBacktrackError](t, err, ErrOffsetBacktrack)
	if obe.Current != 100 || obe.Requested != 99 {
		t.Fatalf("backtrack error fields = %+v", obe)
	}
	// 被拒绝的后退提交不会改变位点。
	st, _ := c.Status("g")
	if st.Offsets[0].Offset != 100 {
		t.Fatalf("offset after rejected backtrack = %d", st.Offsets[0].Offset)
	}
	// 显式允许后退（重置场景）。
	if err := commit(50, "r4", true); err != nil {
		t.Fatalf("AllowBacktrack commit: %v", err)
	}
	st, _ = c.Status("g")
	if st.Offsets[0].Offset != 50 {
		t.Fatalf("offset after allowed backtrack = %d", st.Offsets[0].Offset)
	}
}

func TestCommitIdempotencyAndConflict(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 4})
	mustJoin(t, c, "g", "a") // gen 1, 所有分区都属于 a

	req := CommitRequest{MemberID: "a", Generation: 1, Partition: 0, Offset: 10, RequestID: "dup"}
	res1, err := c.CommitOffset("g", req)
	if err != nil {
		t.Fatal(err)
	}
	// 完全相同的重放：成功、标记 Replayed，不重复推进。
	res2, err := c.CommitOffset("g", req)
	if err != nil {
		t.Fatal(err)
	}
	if !res2.Replayed || res2.Offset != 10 {
		t.Fatalf("replay = %+v, want replayed offset 10", res2)
	}
	if res1 == res2 {
		t.Fatal("expected distinct result values")
	}

	// 同请求号改交同位点不同分区 => 冲突。
	_, err = c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 1, Partition: 1, Offset: 10, RequestID: "dup",
	})
	rce := requireError[*RequestConflictError](t, err, ErrRequestConflict)
	if rce.ExistingPartition != 0 || rce.GotPartition != 1 {
		t.Fatalf("conflict error fields = %+v", rce)
	}

	// 同请求号改交不同位点 => 冲突。
	_, err = c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 1, Partition: 0, Offset: 11, RequestID: "dup",
	})
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("different offset for same request id should conflict, got %v", err)
	}

	// 冲突提交不得落盘：位点仍是 10。
	st, _ := c.Status("g")
	if st.Offsets[0].Offset != 10 {
		t.Fatalf("offset after conflicts = %d, want 10", st.Offsets[0].Offset)
	}
}

// 再均衡后，旧所有者对新所有者分区的提交必须失败。
func TestCommitRejectedAfterRebalance(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 2})
	mustJoin(t, c, "g", "a") // gen 1: [a a]
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 1, Partition: 1, Offset: 5, RequestID: "r1",
	}); err != nil {
		t.Fatal(err)
	}
	mustJoin(t, c, "g", "b") // gen 2: [a b]

	// a 携带新版本提交分区 1：它已不是所有者。
	_, err := c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 2, Partition: 1, Offset: 6, RequestID: "r2",
	})
	if !errors.Is(err, ErrNotOwner) {
		t.Fatalf("former owner commit should fail with ErrNotOwner, got %v", err)
	}
	// b 成为新所有者，可以提交。
	if _, err := c.CommitOffset("g", CommitRequest{
		MemberID: "b", Generation: 2, Partition: 1, Offset: 6, RequestID: "r3",
	}); err != nil {
		t.Fatalf("new owner commit: %v", err)
	}
	// a 用旧版本重试旧请求号：版本先拒绝，不会触碰幂等表。
	_, err = c.CommitOffset("g", CommitRequest{
		MemberID: "a", Generation: 1, Partition: 1, Offset: 5, RequestID: "r1",
	})
	if !errors.Is(err, ErrIllegalGeneration) {
		t.Fatalf("stale generation commit: %v", err)
	}
}

// ---- 并发不变量 ----

// 并发离开 / 超时扫描 / 心跳只能形成一条单调递增的版本序列，
// 且每个已发布版本的分配与成员集合一致。
func TestConcurrentLeaveExpireHeartbeatVersionChain(t *testing.T) {
	clk := newFakeClock()
	store := newRecordingStore(NewMemoryStore())
	c, err := NewCoordinator(store, clk)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 8, SessionTimeout: time.Hour})

	const n = 6
	var gens []int64
	for i := 0; i < n; i++ {
		r := mustJoin(t, c, "g", fmt.Sprintf("m%02d", i))
		gens = append(gens, r.Generation)
	}

	var wg sync.WaitGroup
	// 一半成员并发离开。
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = c.Leave("g", fmt.Sprintf("m%02d", i))
		}(i)
	}
	// 另一半成员并发用「加入时拿到的旧版本」发心跳——全部应被拒绝为版本不匹配，
	// 或在极端交错中成员仍存活但版本已前进；无论哪种都不得改写状态。
	for i := 3; i < n; i++ {
		oldGen := gens[i]
		wg.Add(1)
		go func(i int, oldGen int64) {
			defer wg.Done()
			_, _ = c.Heartbeat("g", fmt.Sprintf("m%02d", i), oldGen)
		}(i, oldGen)
	}
	// 超时扫描（超时设置为 1 小时，不会实际剔除，但与离开并发执行）。
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
	// 3 个离开 => 版本从 n 再 +3。
	if want := int64(n + 3); st.Generation != want {
		t.Fatalf("final generation = %d, want %d", st.Generation, want)
	}
	if len(st.Members) != 3 {
		t.Fatalf("remaining members = %d, want 3", len(st.Members))
	}
	if !assignmentMatchesMembers(st.Assignment, st.MemberIDs()) {
		t.Fatalf("final assignment inconsistent with members: owners=%v members=%v",
			st.Assignment.Owners, st.MemberIDs())
	}

	// 回放所有持久化快照：版本号严格按保存顺序单调不减（重入 Save 时相等），
	// 且每次快照的 owners 恰好是当时成员集合的确定性分配。
	records := store.records()
	var prev int64
	for i, rec := range records {
		g := rec["g"]
		if i > 0 && g.generation < prev {
			t.Fatalf("persisted generation went backwards at save %d: %d -> %d", i, prev, g.generation)
		}
		if g.generation == 0 {
			prev = g.generation
			continue
		}
		ids := make([]string, 0, len(g.members))
		for id := range g.members {
			ids = append(ids, id)
		}
		expected := planAssignment(g.generation, 8, membersFromIDs(ids, clk.Now()), clk.Now())
		if !equalStrings(g.owners, expected.Owners) {
			t.Fatalf("persisted owners gen=%d = %v, want %v", g.generation, g.owners, expected.Owners)
		}
		prev = g.generation
	}
}

// 并发提交不能丢掉较大的合法值；较小的迟到提交必须失败。
func TestConcurrentCommitsKeepLargestOffset(t *testing.T) {
	c, _ := NewCoordinator(nil, nil)
	_ = c.CreateGroup(CreateGroupOptions{Name: "g", Partitions: 1})
	mustJoin(t, c, "g", "a") // gen 1, a 是分区 0 所有者

	const writers = 20
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = c.CommitOffset("g", CommitRequest{
				MemberID: "a", Generation: 1, Partition: 0,
				Offset: int64(i + 1), RequestID: fmt.Sprintf("req-%d", i),
			})
		}(i)
	}
	wg.Wait()

	var successes, backtracks int
	for _, e := range errs {
		switch {
		case e == nil:
			successes++
		case errors.Is(e, ErrOffsetBacktrack):
			backtracks++
		default:
			t.Fatalf("unexpected commit error: %v", e)
		}
	}
	if successes+backtracks != writers || successes == 0 {
		t.Fatalf("successes=%d backtracks=%d", successes, backtracks)
	}
	st, _ := c.Status("g")
	if got, want := st.Offsets[0].Offset, int64(writers); got != want {
		t.Fatalf("final offset = %d, want %d (largest legal value lost)", got, want)
	}
}

// ---- 工具 ----

func equalStrings(a, b []string) bool {
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

func (s GroupStatus) MemberIDs() []string {
	ids := make([]string, len(s.Members))
	for i, m := range s.Members {
		ids[i] = m.ID
	}
	return ids
}

func assignmentMatchesMembers(a Assignment, memberIDs []string) bool {
	idSet := make(map[string]bool, len(memberIDs))
	for _, id := range memberIDs {
		idSet[id] = true
	}
	for _, owner := range a.Owners {
		if owner != "" && !idSet[owner] {
			return false
		}
	}
	expected := planAssignment(a.Generation, len(a.Owners), membersFromIDs(memberIDs, time.Time{}), time.Time{})
	return equalStrings(a.Owners, expected.Owners)
}

func membersFromIDs(ids []string, joinedAt time.Time) map[string]*member {
	m := make(map[string]*member, len(ids))
	for _, id := range ids {
		m[id] = &member{id: id, joinedAt: joinedAt, lastHeartbeatAt: joinedAt}
	}
	return m
}
