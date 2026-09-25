package consumergroups

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fakeClock 是可手动推进的时钟。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func newTestCoordinator(t *testing.T, clock *fakeClock) *Coordinator {
	t.Helper()
	c, err := NewCoordinator(Config{
		SessionTimeout: 30 * time.Second,
		Now:            clock.Now,
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	return c
}

func mustCreateGroup(t *testing.T, c *Coordinator, id string) {
	t.Helper()
	err := c.CreateGroup(id, []TopicConfig{
		{Name: "orders", Partitions: 4},
		{Name: "payments", Partitions: 2},
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
}

func mustJoin(t *testing.T, c *Coordinator, group, member string) int64 {
	t.Helper()
	gen, _, err := c.JoinGroup(group, member)
	if err != nil {
		t.Fatalf("JoinGroup(%s): %v", member, err)
	}
	return gen
}

func TestCreateGroupAndJoin(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	c := newTestCoordinator(t, clock)
	mustCreateGroup(t, c, "g1")

	if err := c.CreateGroup("g1", nil); !errors.Is(err, ErrGroupExists) {
		t.Fatalf("expected ErrGroupExists, got %v", err)
	}

	gen1, assign1, err := c.JoinGroup("g1", "m1")
	if err != nil {
		t.Fatalf("join m1: %v", err)
	}
	if gen1 != 1 {
		t.Fatalf("first join should bump generation to 1, got %d", gen1)
	}
	// 唯一成员应拿到全部分区。
	if len(assign1["orders"]) != 4 || len(assign1["payments"]) != 2 {
		t.Fatalf("sole member should own all partitions, got %v", assign1)
	}

	gen2, _, err := c.JoinGroup("g1", "m2")
	if err != nil {
		t.Fatalf("join m2: %v", err)
	}
	if gen2 != 2 {
		t.Fatalf("second join should bump generation to 2, got %d", gen2)
	}

	state, err := c.State("g1")
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if state.Generation != 2 || len(state.Members) != 2 {
		t.Fatalf("unexpected state: %+v", state)
	}
	// 同一版本内一个分区至多归属一个成员，且全部分区都有归属。
	for topic, parts := range state.Assignment {
		owners := map[string]int{}
		for p, owner := range parts {
			if owner != "m1" && owner != "m2" {
				t.Fatalf("partition %s[%d] owned by unknown member %q", topic, p, owner)
			}
			owners[owner]++
		}
	}
	if len(state.Assignment["orders"]) != 4 || len(state.Assignment["payments"]) != 2 {
		t.Fatalf("assignment must cover all partitions: %v", state.Assignment)
	}
}

func TestAssignmentIsDeterministic(t *testing.T) {
	topics := map[string]int{"a": 5, "b": 3}
	members := []string{"m3", "m1", "m2"}
	first := computeAssignment(topics, members)
	for i := 0; i < 50; i++ {
		// 打乱成员顺序，结果必须一致。
		shuffled := []string{members[(i+1)%3], members[(i+2)%3], members[i%3]}
		got := computeAssignment(topics, shuffled)
		for topic, parts := range first {
			for p, owner := range parts {
				if got[topic][p] != owner {
					t.Fatalf("non-deterministic assignment: %v vs %v", first, got)
				}
			}
		}
	}
}

func TestHeartbeatRejectsStaleGeneration(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	c := newTestCoordinator(t, clock)
	mustCreateGroup(t, c, "g1")

	gen1 := mustJoin(t, c, "g1", "m1")
	if err := c.Heartbeat("g1", "m1", gen1); err != nil {
		t.Fatalf("heartbeat with current generation should succeed: %v", err)
	}

	mustJoin(t, c, "g1", "m2") // 触发再均衡，版本前进

	if err := c.Heartbeat("g1", "m1", gen1); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale heartbeat should be rejected, got %v", err)
	}
	state, _ := c.State("g1")
	if err := c.Heartbeat("g1", "m1", state.Generation); err != nil {
		t.Fatalf("heartbeat with new generation should succeed: %v", err)
	}
	if err := c.Heartbeat("g1", "ghost", state.Generation); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("unknown member heartbeat should fail, got %v", err)
	}
}

func TestLeaveAndLateOperationsCannotResurrect(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	c := newTestCoordinator(t, clock)
	mustCreateGroup(t, c, "g1")

	gen1 := mustJoin(t, c, "g1", "m1")
	gen2 := mustJoin(t, c, "g1", "m2")

	if err := c.LeaveGroup("g1", "m2"); err != nil {
		t.Fatalf("leave: %v", err)
	}
	state, _ := c.State("g1")
	if state.Generation != gen2+1 {
		t.Fatalf("leave should bump generation, got %d", state.Generation)
	}
	if len(state.Members) != 1 || state.Members[0].ID != "m1" {
		t.Fatalf("m2 should be gone: %+v", state.Members)
	}

	// 迟到的心跳与提交都不能复活 m2，也不能用旧版本操作。
	if err := c.Heartbeat("g1", "m2", gen2); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("late heartbeat from removed member should fail, got %v", err)
	}
	err := c.CommitOffset("g1", "m2", gen2, "orders", 0, 10, "r1")
	if !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("late commit from removed member should fail, got %v", err)
	}
	if err := c.Heartbeat("g1", "m1", gen1); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale heartbeat from m1 should fail, got %v", err)
	}
	state, _ = c.State("g1")
	if len(state.Members) != 1 || state.Generation != gen2+1 {
		t.Fatalf("late operations must not change state: %+v", state)
	}
}

func TestSessionTimeoutTriggersRebalance(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	c := newTestCoordinator(t, clock)
	mustCreateGroup(t, c, "g1")

	mustJoin(t, c, "g1", "m1")
	gen2 := mustJoin(t, c, "g1", "m2")

	// 推进 20s（未超时），m1 心跳续期；m2 停留在加入时刻。
	clock.Advance(20 * time.Second)
	if err := c.Heartbeat("g1", "m1", gen2); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	expired, err := c.ScanTimeouts()
	if err != nil {
		t.Fatalf("ScanTimeouts: %v", err)
	}
	if len(expired) != 0 {
		t.Fatalf("no one should expire yet, got %v", expired)
	}

	// 再推进 15s：m1 距上次心跳 15s（未超时），m2 已 35s（超时）。
	clock.Advance(15 * time.Second)
	expired, err = c.ScanTimeouts()
	if err != nil {
		t.Fatalf("ScanTimeouts: %v", err)
	}
	if len(expired["g1"]) != 1 || expired["g1"][0] != "m2" {
		t.Fatalf("m2 should expire, got %v", expired)
	}
	state, _ := c.State("g1")
	if state.Generation != gen2+1 {
		t.Fatalf("timeout should bump generation, got %d", state.Generation)
	}
	// m2 被淘汰后，m1 应独占全部分区。
	if len(state.Assignment["orders"]) != 4 {
		t.Fatalf("m1 should own all partitions: %v", state.Assignment)
	}
	for p, owner := range state.Assignment["orders"] {
		if owner != "m1" {
			t.Fatalf("orders[%d] should belong to m1, got %q", p, owner)
		}
	}
}

func TestCommitOffsetOwnershipAndGeneration(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	c := newTestCoordinator(t, clock)
	mustCreateGroup(t, c, "g1")

	gen1 := mustJoin(t, c, "g1", "m1")
	// m1 独占全部 6 个分区，提交成功。
	if err := c.CommitOffset("g1", "m1", gen1, "orders", 0, 100, "r1"); err != nil {
		t.Fatalf("owner commit should succeed: %v", err)
	}

	gen2 := mustJoin(t, c, "g1", "m2")
	state, _ := c.State("g1")

	// 旧版本提交被拒绝。
	if err := c.CommitOffset("g1", "m1", gen1, "orders", 1, 5, "r2"); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale commit should be rejected, got %v", err)
	}
	// 非所有者提交被拒绝：找一个属于 m2 的分区让 m1 提交。
	var m2Topic string
	var m2Part int
	for topic, parts := range state.Assignment {
		for p, owner := range parts {
			if owner == "m2" {
				m2Topic, m2Part = topic, p
			}
		}
	}
	err := c.CommitOffset("g1", "m1", gen2, m2Topic, m2Part, 5, "r3")
	if !errors.Is(err, ErrNotPartitionOwner) {
		t.Fatalf("non-owner commit should be rejected, got %v", err)
	}
	// 所有者用当前版本提交成功。
	if err := c.CommitOffset("g1", "m2", gen2, m2Topic, m2Part, 5, "r3"); err != nil {
		t.Fatalf("owner commit should succeed: %v", err)
	}
	// 未知主题 / 分区。
	if err := c.CommitOffset("g1", "m1", gen2, "nope", 0, 1, "r4"); !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("unknown topic, got %v", err)
	}
	if err := c.CommitOffset("g1", "m1", gen2, "orders", 99, 1, "r4"); !errors.Is(err, ErrUnknownPartition) {
		t.Fatalf("unknown partition, got %v", err)
	}
}

func TestCommitOffsetIdempotencyAndRegression(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	c := newTestCoordinator(t, clock)
	mustCreateGroup(t, c, "g1")
	gen := mustJoin(t, c, "g1", "m1")

	if err := c.CommitOffset("g1", "m1", gen, "orders", 0, 100, "req-1"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// 同一请求号重投相同位点：幂等成功。
	if err := c.CommitOffset("g1", "m1", gen, "orders", 0, 100, "req-1"); err != nil {
		t.Fatalf("idempotent replay should succeed: %v", err)
	}
	// 同一请求号改交不同位点：冲突。
	if err := c.CommitOffset("g1", "m1", gen, "orders", 0, 200, "req-1"); !errors.Is(err, ErrCommitConflict) {
		t.Fatalf("conflicting request id should fail, got %v", err)
	}
	// 位点不得后退。
	if err := c.CommitOffset("g1", "m1", gen, "orders", 0, 50, "req-2"); !errors.Is(err, ErrOffsetRegression) {
		t.Fatalf("regression should fail, got %v", err)
	}
	// 前进成功。
	if err := c.CommitOffset("g1", "m1", gen, "orders", 0, 150, "req-2"); err != nil {
		t.Fatalf("forward commit should succeed: %v", err)
	}
	state, _ := c.State("g1")
	if got := state.Offsets["orders"][0].Offset; got != 150 {
		t.Fatalf("offset should be 150, got %d", got)
	}
}

func TestConcurrentCommitsKeepLargestOffset(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	c := newTestCoordinator(t, clock)
	mustCreateGroup(t, c, "g1")
	gen := mustJoin(t, c, "g1", "m1")

	const n = 200
	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := c.CommitOffset("g1", "m1", gen, "orders", 0, int64(i), fmt.Sprintf("req-%d", i))
			if err != nil && !errors.Is(err, ErrOffsetRegression) {
				t.Errorf("unexpected commit error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	state, _ := c.State("g1")
	if got := state.Offsets["orders"][0].Offset; got != n {
		t.Fatalf("largest offset must survive concurrency, got %d want %d", got, n)
	}
}

func TestConcurrentMembershipChangesSingleGenerationSequence(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	c := newTestCoordinator(t, clock)
	mustCreateGroup(t, c, "g1")
	mustJoin(t, c, "g1", "m0")

	var wg sync.WaitGroup
	// 并发加入、心跳、离开、超时扫描。
	for i := 1; i <= 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			member := fmt.Sprintf("m%d", i)
			gen, _, err := c.JoinGroup("g1", member)
			if err != nil {
				t.Errorf("join: %v", err)
				return
			}
			_ = c.Heartbeat("g1", member, gen) // 可能已过期，忽略
			if i%3 == 0 {
				_ = c.LeaveGroup("g1", member)
			}
		}(i)
	}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			clock.Advance(time.Second)
			_, _ = c.ScanTimeouts()
		}()
	}
	wg.Wait()

	// 不变量：版本序列清晰——最终版本的分配完整且每个分区恰好一个所有者，
	// 所有者都是当前存活成员。
	state, err := c.State("g1")
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	alive := map[string]bool{}
	for _, m := range state.Members {
		alive[m.ID] = true
	}
	total := 0
	for topic, parts := range state.Assignment {
		for p, owner := range parts {
			total++
			if !alive[owner] {
				t.Fatalf("partition %s[%d] owned by dead member %q", topic, p, owner)
			}
		}
	}
	if len(state.Members) > 0 && total != 6 {
		t.Fatalf("assignment must cover all 6 partitions, got %d", total)
	}
}

func TestRebalancePublishedAtomically(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	c := newTestCoordinator(t, clock)
	mustCreateGroup(t, c, "g1")
	mustJoin(t, c, "g1", "m0")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// 读者持续读取状态，校验每次读到的分配都是完整的一套。
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				state, err := c.State("g1")
				if err != nil {
					t.Errorf("State: %v", err)
					return
				}
				if len(state.Members) == 0 {
					continue
				}
				count := 0
				for _, parts := range state.Assignment {
					count += len(parts)
				}
				if count != 6 {
					t.Errorf("read partial assignment: gen=%d partitions=%d", state.Generation, count)
					return
				}
			}
		}()
	}
	// 写者不断触发再均衡。
	for i := 1; i <= 50; i++ {
		member := fmt.Sprintf("m%d", i)
		if _, _, err := c.JoinGroup("g1", member); err != nil {
			t.Fatalf("join: %v", err)
		}
		if err := c.LeaveGroup("g1", member); err != nil {
			t.Fatalf("leave: %v", err)
		}
	}
	close(stop)
	wg.Wait()
}

func TestPersistenceRoundTrip(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	path := filepath.Join(t.TempDir(), "coordinator.json")
	storage := NewFileStorage(path)

	c, err := NewCoordinator(Config{SessionTimeout: 30 * time.Second, Storage: storage, Now: clock.Now})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	mustCreateGroup(t, c, "g1")
	mustJoin(t, c, "g1", "m1")
	gen := mustJoin(t, c, "g1", "m2") // 排序后 orders[0] 仍归属 m1
	if err := c.CommitOffset("g1", "m1", gen, "orders", 0, 42, "req-1"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	before, _ := c.State("g1")

	// 用同一存储重建协调器，状态应完整恢复。
	c2, err := NewCoordinator(Config{SessionTimeout: 30 * time.Second, Storage: storage, Now: clock.Now})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	after, err := c2.State("g1")
	if err != nil {
		t.Fatalf("State after reopen: %v", err)
	}
	if after.Generation != before.Generation {
		t.Fatalf("generation not persisted: %d vs %d", after.Generation, before.Generation)
	}
	if len(after.Members) != 2 {
		t.Fatalf("members not persisted: %+v", after.Members)
	}
	if got := after.Offsets["orders"][0].Offset; got != 42 {
		t.Fatalf("offset not persisted: %d", got)
	}
	for topic, parts := range before.Assignment {
		for p, owner := range parts {
			if after.Assignment[topic][p] != owner {
				t.Fatalf("assignment not persisted for %s[%d]", topic, p)
			}
		}
	}
	// 恢复后幂等语义仍然有效：重放 req-1 相同位点成功，改交冲突。
	if err := c2.CommitOffset("g1", "m1", gen, "orders", 0, 42, "req-1"); err != nil {
		t.Fatalf("idempotent replay after reopen should succeed: %v", err)
	}
	if err := c2.CommitOffset("g1", "m1", gen, "orders", 0, 43, "req-1"); !errors.Is(err, ErrCommitConflict) {
		t.Fatalf("conflict after reopen should be detected, got %v", err)
	}
}

func TestOffsetsSurviveRebalance(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	c := newTestCoordinator(t, clock)
	mustCreateGroup(t, c, "g1")
	gen := mustJoin(t, c, "g1", "m1")
	if err := c.CommitOffset("g1", "m1", gen, "orders", 0, 77, "req-1"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// 再均衡不丢位点。
	mustJoin(t, c, "g1", "m2")
	if err := c.LeaveGroup("g1", "m2"); err != nil {
		t.Fatalf("leave: %v", err)
	}
	state, _ := c.State("g1")
	if got := state.Offsets["orders"][0].Offset; got != 77 {
		t.Fatalf("offset should survive rebalance, got %d", got)
	}
}
