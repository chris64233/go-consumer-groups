package consumergroups

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// DefaultSessionTimeout 是未显式指定时的默认会话超时。
const DefaultSessionTimeout = 30 * time.Second

// Coordinator 是消费组协调器：管理组成员、分配版本（generation）、
// 分区分配与位点提交。
//
// 并发模型：所有变更操作在同一把互斥锁下串行执行，因此成员离开、
// 超时扫描与心跳无论以何种顺序并发到达，都只会形成一条单调递增的
// 分配版本序列；被新版本淘汰的迟到操作在版本校验处即被拒绝，
// 无法复活成员或覆盖新分配。
//
// 分配发布：每次再均衡先整体计算出新的 Assignment，再原子地替换组上的
// 指针字段，读者（Status / Heartbeat / Join 的返回值）拿到的永远是
// 一份完整的版本快照，不会读到半套新分配。
//
// 状态在每次变更后通过 Store 持久化；进程重启后可从 Store 恢复。
type Coordinator struct {
	mu     sync.Mutex
	clock  Clock
	store  Store
	groups map[string]*group
}

// NewCoordinator 创建协调器并从 store 恢复既有状态。
// store 为 nil 时使用内存存储（不跨进程持久化）。
// clock 为 nil 时使用系统时钟。
func NewCoordinator(store Store, clock Clock) (*Coordinator, error) {
	if store == nil {
		store = NewMemoryStore()
	}
	if clock == nil {
		clock = systemClock{}
	}
	c := &Coordinator{
		clock:  clock,
		store:  store,
		groups: make(map[string]*group),
	}
	snap, err := store.Load()
	if err != nil {
		return nil, fmt.Errorf("load coordinator state: %w", err)
	}
	if snap != nil {
		if err := c.restore(snap); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// CreateGroup 创建消费组。主题分区数固定，创建后不可变。
// 初始分配版本为 0（尚无任何成员加入，无有效分配）。
func (c *Coordinator) CreateGroup(opts CreateGroupOptions) error {
	if opts.Name == "" {
		return fmt.Errorf("%w: group name is required", ErrInvalidArgument)
	}
	if opts.Partitions <= 0 {
		return fmt.Errorf("%w: partitions must be positive, got %d", ErrInvalidArgument, opts.Partitions)
	}
	timeout := opts.SessionTimeout
	if timeout <= 0 {
		timeout = DefaultSessionTimeout
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.groups[opts.Name]; ok {
		return fmt.Errorf("%w: %q", ErrGroupAlreadyExists, opts.Name)
	}
	c.groups[opts.Name] = &group{
		name:           opts.Name,
		partitions:     opts.Partitions,
		sessionTimeout: timeout,
		generation:     0,
		members:        make(map[string]*member),
		assignment: Assignment{
			Generation: 0,
			CreatedAt:  c.clock.Now(),
			Owners:     make([]string, opts.Partitions),
		},
		offsets: make(map[int]*Offset),
	}
	return c.persistLocked()
}

// Join 将成员加入组，触发一次再均衡：分配版本 +1，并整体发布
// 包含新成员的完整分配。重复加入同一成员 ID 返回 ErrMemberAlreadyExists。
func (c *Coordinator) Join(groupName, memberID string) (JoinResult, error) {
	if memberID == "" {
		return JoinResult{}, fmt.Errorf("%w: member id is required", ErrInvalidArgument)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return JoinResult{}, err
	}
	if _, ok := g.members[memberID]; ok {
		return JoinResult{}, fmt.Errorf("%w: group=%q member=%q", ErrMemberAlreadyExists, groupName, memberID)
	}

	now := c.clock.Now()
	g.members[memberID] = &member{
		id:              memberID,
		joinedAt:        now,
		lastHeartbeatAt: now,
		requests:        make(map[string]requestRecord),
	}
	if g.leader == "" {
		g.leader = memberID
	}
	c.rebalanceLocked(g, now)
	if err := c.persistLocked(); err != nil {
		return JoinResult{}, err
	}
	return JoinResult{
		MemberID:   memberID,
		Generation: g.generation,
		Leader:     g.leader,
		Assignment: g.assignment,
	}, nil
}

// Heartbeat 上报成员心跳。只有携带当前分配版本的心跳才被接受；
// 携带旧版本（或超前版本）的心跳返回 ErrIllegalGeneration，
// 成员已被剔除时返回 ErrMemberNotFound。被接受的心跳会刷新会话截止时间。
func (c *Coordinator) Heartbeat(groupName, memberID string, generation int64) (HeartbeatResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return HeartbeatResult{}, err
	}
	// 先校验版本：迟到的心跳即使指向仍存在的成员也不得生效。
	if err := g.checkGeneration(generation); err != nil {
		return HeartbeatResult{}, err
	}
	m, ok := g.members[memberID]
	if !ok {
		return HeartbeatResult{}, fmt.Errorf("%w: group=%q member=%q", ErrMemberNotFound, groupName, memberID)
	}
	m.lastHeartbeatAt = c.clock.Now()
	if err := c.persistLocked(); err != nil {
		return HeartbeatResult{}, err
	}
	return HeartbeatResult{Generation: g.generation, Assignment: g.assignment}, nil
}

// Leave 让成员主动离开组，触发再均衡（分配版本 +1）。
// 成员不存在时返回 ErrMemberNotFound。
func (c *Coordinator) Leave(groupName, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return err
	}
	if _, ok := g.members[memberID]; !ok {
		return fmt.Errorf("%w: group=%q member=%q", ErrMemberNotFound, groupName, memberID)
	}
	c.removeMemberLocked(g, memberID)
	c.rebalanceLocked(g, c.clock.Now())
	return c.persistLocked()
}

// ExpireGroup 扫描单个组，剔除会话超时（now - lastHeartbeat > 会话超时）的
// 成员。只要有成员被剔除就触发一次再均衡；返回被剔除的成员 ID（升序）。
// 没有成员超时时不推进版本，返回空切片。
func (c *Coordinator) ExpireGroup(groupName string, now time.Time) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return nil, err
	}
	expired := c.expireLocked(g, now)
	if len(expired) == 0 {
		return nil, nil
	}
	return expired, c.persistLocked()
}

// ExpireAll 对所有组执行一次超时扫描（组名升序处理，保证行为确定）。
// 返回发生再均衡的组名。典型用法是由后台定时器周期调用。
func (c *Coordinator) ExpireAll(now time.Time) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	names := make([]string, 0, len(c.groups))
	for name := range c.groups {
		names = append(names, name)
	}
	sort.Strings(names)

	var changed []string
	for _, name := range names {
		if expired := c.expireLocked(c.groups[name], now); len(expired) > 0 {
			changed = append(changed, name)
		}
	}
	if len(changed) == 0 {
		return nil, nil
	}
	return changed, c.persistLocked()
}

// CommitOffset 提交分区位点。
//
// 校验顺序：组存在 -> 分配版本匹配 -> 成员在组 -> 分区合法 ->
// 成员是当前所有者 -> 幂等请求号 -> 位点单调性。
//
// 幂等性：同一成员对同一请求号的重放（分区与位点完全一致）直接成功且
// 不重复推进；同请求号携带不同分区或位点返回 ErrRequestConflict。
// 单调性：默认位点不得后退，后退返回 ErrOffsetBacktrack；
// 并发提交在锁内串行执行，较大位点先落地时，较小的迟到提交被拒绝，
// 因此不会丢掉较大的合法值。
func (c *Coordinator) CommitOffset(groupName string, req CommitRequest) (CommitResult, error) {
	if req.RequestID == "" {
		return CommitResult{}, fmt.Errorf("%w: request id is required for idempotent commit", ErrInvalidArgument)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return CommitResult{}, err
	}
	if err := g.checkGeneration(req.Generation); err != nil {
		return CommitResult{}, err
	}
	m, ok := g.members[req.MemberID]
	if !ok {
		return CommitResult{}, fmt.Errorf("%w: group=%q member=%q", ErrMemberNotFound, groupName, req.MemberID)
	}
	if req.Partition < 0 || req.Partition >= g.partitions {
		return CommitResult{}, fmt.Errorf("%w: group=%q partition=%d (partitions=%d)",
			ErrInvalidPartition, groupName, req.Partition, g.partitions)
	}
	if owner := g.assignment.OwnerOf(req.Partition); owner != req.MemberID {
		return CommitResult{}, &NotPartitionOwnerError{
			Group: groupName, Partition: req.Partition, Owner: owner, Member: req.MemberID,
		}
	}

	// 幂等请求号：命中记录时按内容一致性判定重放或冲突。
	if rec, seen := m.requests[req.RequestID]; seen {
		if rec.partition != req.Partition || rec.offset != req.Offset {
			return CommitResult{}, &RequestConflictError{
				Group: groupName, Member: req.MemberID, RequestID: req.RequestID,
				ExistingPartition: rec.partition, ExistingOffset: rec.offset,
				GotPartition: req.Partition, GotOffset: req.Offset,
			}
		}
		// 重放：不重复推进位点，返回当前生效值。
		return CommitResult{Offset: g.offsets[req.Partition].Offset, Replayed: true}, nil
	}

	now := c.clock.Now()
	cur, exists := g.offsets[req.Partition]
	if exists && req.Offset < cur.Offset && !req.AllowBacktrack {
		return CommitResult{}, &OffsetBacktrackError{
			Group: groupName, Partition: req.Partition, Current: cur.Offset, Requested: req.Offset,
		}
	}
	if !exists {
		cur = &Offset{Partition: req.Partition}
		g.offsets[req.Partition] = cur
	}
	cur.Offset = req.Offset
	cur.Metadata = req.Metadata
	cur.CommittedAt = now
	cur.LastRequestID = req.RequestID
	m.requests[req.RequestID] = requestRecord{
		partition: req.Partition, offset: req.Offset, metadata: req.Metadata, committedAt: now,
	}
	if err := c.persistLocked(); err != nil {
		return CommitResult{}, err
	}
	return CommitResult{Offset: cur.Offset}, nil
}

// Status 返回组的完整状态快照（深拷贝，调用方可安全持有）。
func (c *Coordinator) Status(groupName string) (GroupStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return GroupStatus{}, err
	}
	return g.status(), nil
}

// Groups 返回当前所有组名（升序）。
func (c *Coordinator) Groups() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.groups))
	for name := range c.groups {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ---- 内部实现（调用方须已持有 c.mu）----

type group struct {
	name           string
	partitions     int
	sessionTimeout time.Duration
	generation     int64
	leader         string
	members        map[string]*member
	assignment     Assignment
	offsets        map[int]*Offset
	lastRebalance  time.Time
}

type member struct {
	id              string
	joinedAt        time.Time
	lastHeartbeatAt time.Time
	// requests 记录该成员已受理的幂等请求号及其提交内容，
	// 成员被剔除后随之清除（请求号作用域为成员的一次在组生命周期）。
	requests map[string]requestRecord
}

type requestRecord struct {
	partition   int
	offset      int64
	metadata    string
	committedAt time.Time
}

func (c *Coordinator) groupLocked(name string) (*group, error) {
	g, ok := c.groups[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, name)
	}
	return g, nil
}

func (g *group) checkGeneration(generation int64) error {
	if generation != g.generation {
		return &GenerationMismatchError{Group: g.name, Want: g.generation, Got: generation}
	}
	return nil
}

// rebalanceLocked 推进分配版本并整体发布新的完整分配。
func (c *Coordinator) rebalanceLocked(g *group, now time.Time) {
	g.generation++
	g.assignment = planAssignment(g.generation, g.partitions, g.members, now)
	g.leader = pickLeader(g.members)
	g.lastRebalance = now
}

func (c *Coordinator) removeMemberLocked(g *group, memberID string) {
	delete(g.members, memberID)
}

// expireLocked 剔除超时成员；有剔除时触发再均衡。返回被剔除成员 ID（升序）。
func (c *Coordinator) expireLocked(g *group, now time.Time) []string {
	var expired []string
	for id, m := range g.members {
		if now.Sub(m.lastHeartbeatAt) > g.sessionTimeout {
			expired = append(expired, id)
		}
	}
	if len(expired) == 0 {
		return nil
	}
	sort.Strings(expired)
	for _, id := range expired {
		c.removeMemberLocked(g, id)
	}
	c.rebalanceLocked(g, now)
	return expired
}

func (g *group) status() GroupStatus {
	members := make([]Member, 0, len(g.members))
	for _, m := range g.members {
		members = append(members, Member{
			ID:              m.id,
			JoinedAt:        m.joinedAt,
			LastHeartbeatAt: m.lastHeartbeatAt,
		})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })

	offsets := make([]Offset, 0, len(g.offsets))
	for _, o := range g.offsets {
		offsets = append(offsets, *o)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i].Partition < offsets[j].Partition })

	return GroupStatus{
		Name:          g.name,
		Partitions:    g.partitions,
		Generation:    g.generation,
		Leader:        g.leader,
		Members:       members,
		Assignment:    g.assignment.clone(),
		Offsets:       offsets,
		LastRebalance: g.lastRebalance,
	}
}

func (a Assignment) clone() Assignment {
	owners := make([]string, len(a.Owners))
	copy(owners, a.Owners)
	a.Owners = owners
	return a
}

// persistLocked 将当前完整状态写入 Store。调用方必须已持有 c.mu，
// 保证落盘的是一条一致的版本序列中的某个快照。
func (c *Coordinator) persistLocked() error {
	if err := c.store.Save(c.snapshotLocked()); err != nil {
		return fmt.Errorf("persist coordinator state: %w", err)
	}
	return nil
}
