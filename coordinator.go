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
// 协作式分区再均衡与位点提交。
//
// 并发模型：所有变更操作在同一把互斥锁下串行执行，因此成员离开、
// 超时扫描、心跳与撤销确认无论以何种顺序并发到达，都只会形成一条
// 单调递增的分配版本序列；被新版本淘汰的迟到操作在版本校验处即被拒绝，
// 无法复活成员或覆盖新分配。
//
// 协作式再均衡：每次成员变化推进版本并生成目标所有权（target），
// 与当前有效所有权的差异构成待撤销集合。旧所有者确认撤销（或超时/
// 离开被强制回收）之前，新所有者不能取得分区，未受影响的分区继续由
// 原成员消费。撤销确认与强制回收只在本版本内推进有效所有权，
// 版本号本身只在成员变化时 +1。
//
// 分配发布：每次再均衡先整体计算出新的 Assignment，再原子地替换组上的
// 指针字段；撤销转移采用写时复制替换整个 Owners 切片。读者（Status /
// Heartbeat / Join 的返回值）拿到的永远是一份完整的版本快照，
// 不会读到半套新分配。
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
		target: Assignment{
			Generation: 0,
			CreatedAt:  c.clock.Now(),
			Owners:     make([]string, opts.Partitions),
		},
		pending:   make(map[int]revocation),
		acked:     make(map[string][]int),
		reclaimed: make(map[string][]int),
		offsets:   make(map[int]*Offset),
	}
	return c.persistLocked()
}

// Join 将成员加入组，触发一次协作式再均衡：分配版本 +1，生成目标所有权；
// 需要易主的分区进入待撤销集合，确认前仍归旧所有者。重复加入同一成员 ID
// 返回 ErrMemberAlreadyExists。
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
		MemberID:          memberID,
		Generation:        g.generation,
		Leader:            g.leader,
		Assignment:        g.assignment.clone(),
		Target:            g.target.clone(),
		Revocations:       g.outstandingFor(memberID),
		RebalanceComplete: len(g.pending) == 0,
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
	return HeartbeatResult{
		Generation:        g.generation,
		Assignment:        g.assignment.clone(),
		Target:            g.target.clone(),
		Revocations:       g.outstandingFor(memberID),
		RebalanceComplete: len(g.pending) == 0,
	}, nil
}

// Leave 让成员主动离开组，触发再均衡（分配版本 +1）。该成员名下
// 待撤销的分区由协调器强制回收，立即转移给目标所有者。
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
// 协作式再均衡下，所有权校验针对有效所有权：处于待撤销状态的分区
// 在旧所有者确认撤销前仍归其所有，可提交最终位点；一旦确认（或被
// 强制回收）完成转移，旧成员的迟到提交即被栅栏（ErrNotOwner），
// 携带旧版本的提交则在版本校验处被拒绝。
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

// AcknowledgeRevocation 确认成员在当前分配版本下撤销一批分区。
//
// 确认必须精确匹配四要素：组、成员、分配版本、分区集合。校验顺序：
// 组存在 -> 分配版本匹配 -> 成员在组 -> 分区编号合法 -> 集合精确匹配。
//
// 集合精确匹配：请求的分区集合（顺序无关）必须与该成员当前待撤销集合
// 完全一致；漏报或多报返回 ErrRevocationMismatch（携带 Missing/Extra
// 明细），不会部分生效，也不会提前完成整个再均衡。
//
// 幂等：确认成功后以相同内容重试返回成功（Transferred 为空）；上一版本
// 的确认返回 ErrIllegalGeneration。
//
// 确认生效后，这些分区的有效所有权立即转移给目标所有者；此后旧所有者
// 的迟到提交会在所有者校验处被栅栏（ErrNotOwner）。
func (c *Coordinator) AcknowledgeRevocation(groupName string, ack RevocationAck) (RevocationAckResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return RevocationAckResult{}, err
	}
	if err := g.checkGeneration(ack.Generation); err != nil {
		return RevocationAckResult{}, err
	}
	if _, ok := g.members[ack.MemberID]; !ok {
		return RevocationAckResult{}, fmt.Errorf("%w: group=%q member=%q", ErrMemberNotFound, groupName, ack.MemberID)
	}

	requested, err := normalizePartitions(ack.Partitions, g.partitions)
	if err != nil {
		return RevocationAckResult{}, fmt.Errorf("%w: group=%q: %v", ErrInvalidPartition, groupName, err)
	}
	outstanding := g.outstandingFor(ack.MemberID)
	if len(outstanding) == 0 {
		// 本成员当前没有待撤销分区：相同内容的重试（或无撤销义务时的
		// 空确认）原样成功，其余一律拒绝。
		if equalIntSets(requested, g.acked[ack.MemberID]) || len(requested) == 0 {
			return RevocationAckResult{RebalanceComplete: len(g.pending) == 0}, nil
		}
		return RevocationAckResult{}, &RevocationMismatchError{
			Group: groupName, Member: ack.MemberID, Generation: ack.Generation,
			Extra: requested,
		}
	}
	missing, extra := diffIntSets(outstanding, requested)
	if len(missing) > 0 || len(extra) > 0 {
		return RevocationAckResult{}, &RevocationMismatchError{
			Group: groupName, Member: ack.MemberID, Generation: ack.Generation,
			Missing: missing, Extra: extra,
		}
	}

	// 写时复制：整体替换 Owners 切片，已发出的快照不受转移影响。
	owners := make([]string, len(g.assignment.Owners))
	copy(owners, g.assignment.Owners)
	transferred := make([]int, len(outstanding))
	for i, p := range outstanding {
		rev := g.pending[p]
		owners[p] = rev.to
		delete(g.pending, p)
		transferred[i] = p
	}
	g.assignment.Owners = owners
	g.acked[ack.MemberID] = append(g.acked[ack.MemberID], outstanding...)

	if err := c.persistLocked(); err != nil {
		return RevocationAckResult{}, err
	}
	return RevocationAckResult{Transferred: transferred, RebalanceComplete: len(g.pending) == 0}, nil
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
	// assignment 当前有效所有权：待撤销分区在确认前仍归旧所有者。
	assignment Assignment
	// target 本代目标所有权（与 assignment 同一代）。
	target Assignment
	// pending 待撤销集合：分区 -> 撤销记录。确认或强制回收后移除。
	pending map[int]revocation
	// acked 本代各成员已确认撤销的分区（用于幂等重试与进度查询）。
	acked map[string][]int
	// reclaimed 本代各成员因离开/超时被强制回收的分区。
	reclaimed     map[string][]int
	offsets       map[int]*Offset
	lastRebalance time.Time
}

// revocation 是一条待撤销记录。
type revocation struct {
	partition int
	from      string
	to        string
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

// rebalanceLocked 推进分配版本，生成目标所有权与待撤销集合。
//
// 对每个分区比较当前有效所有者与目标所有者：
//   - 一致：不受影响，继续由原成员消费；
//   - 旧所有者为空：直接归属新所有者；
//   - 旧所有者已离开/超时：强制回收，立即转移给目标所有者；
//   - 其余：进入待撤销集合，旧所有者确认前继续持有。
func (c *Coordinator) rebalanceLocked(g *group, now time.Time) {
	g.generation++
	target := planAssignment(g.generation, g.partitions, g.members, now)

	owners := make([]string, g.partitions)
	g.pending = make(map[int]revocation)
	g.acked = make(map[string][]int)
	g.reclaimed = make(map[string][]int)
	for p := 0; p < g.partitions; p++ {
		cur := g.assignment.OwnerOf(p)
		want := target.OwnerOf(p)
		switch {
		case cur == want:
			owners[p] = cur
		case cur == "":
			owners[p] = want
		default:
			if _, alive := g.members[cur]; !alive {
				g.reclaimed[cur] = append(g.reclaimed[cur], p)
				owners[p] = want
			} else {
				g.pending[p] = revocation{partition: p, from: cur, to: want}
				owners[p] = cur
			}
		}
	}
	g.assignment = Assignment{Generation: g.generation, CreatedAt: now, Owners: owners}
	g.target = target
	g.leader = pickLeader(g.members)
	g.lastRebalance = now
}

// outstandingFor 返回成员当前待确认撤销的分区（升序）。
func (g *group) outstandingFor(memberID string) []int {
	var out []int
	for p, rev := range g.pending {
		if rev.from == memberID {
			out = append(out, p)
		}
	}
	sort.Ints(out)
	return out
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

	pending := make([]PartitionRevocation, 0, len(g.pending))
	for _, rev := range g.pending {
		pending = append(pending, PartitionRevocation{
			Partition: rev.partition, From: rev.from, To: rev.to,
		})
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].Partition < pending[j].Partition })

	return GroupStatus{
		Name:               g.name,
		Partitions:         g.partitions,
		Generation:         g.generation,
		Leader:             g.leader,
		Members:            members,
		Assignment:         g.assignment.clone(),
		Target:             g.target.clone(),
		PendingRevocations: pending,
		AckProgress:        g.ackProgress(),
		RebalanceComplete:  len(g.pending) == 0,
		Offsets:            offsets,
		LastRebalance:      g.lastRebalance,
	}
}

// ackProgress 汇总本代各成员的撤销确认进度（按成员 ID 升序）。
func (g *group) ackProgress() []MemberAckProgress {
	byMember := make(map[string]*MemberAckProgress)
	entry := func(id string) *MemberAckProgress {
		p, ok := byMember[id]
		if !ok {
			p = &MemberAckProgress{MemberID: id}
			byMember[id] = p
		}
		return p
	}
	for _, rev := range g.pending {
		e := entry(rev.from)
		e.Outstanding = append(e.Outstanding, rev.partition)
	}
	for id, ps := range g.acked {
		e := entry(id)
		e.Acked = append(e.Acked, ps...)
	}
	for id, ps := range g.reclaimed {
		e := entry(id)
		e.ForceReclaimed = append(e.ForceReclaimed, ps...)
	}

	out := make([]MemberAckProgress, 0, len(byMember))
	for _, p := range byMember {
		sort.Ints(p.Outstanding)
		sort.Ints(p.Acked)
		sort.Ints(p.ForceReclaimed)
		p.Required = make([]int, 0, len(p.Acked)+len(p.Outstanding)+len(p.ForceReclaimed))
		p.Required = append(p.Required, p.Acked...)
		p.Required = append(p.Required, p.Outstanding...)
		p.Required = append(p.Required, p.ForceReclaimed...)
		sort.Ints(p.Required)
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MemberID < out[j].MemberID })
	return out
}

func (a Assignment) clone() Assignment {
	owners := make([]string, len(a.Owners))
	copy(owners, a.Owners)
	a.Owners = owners
	return a
}

// normalizePartitions 去重、排序分区集合并校验范围。
func normalizePartitions(ps []int, partitions int) ([]int, error) {
	if len(ps) == 0 {
		return nil, nil
	}
	seen := make(map[int]struct{}, len(ps))
	out := make([]int, 0, len(ps))
	for _, p := range ps {
		if p < 0 || p >= partitions {
			return nil, fmt.Errorf("partition %d out of range [0, %d)", p, partitions)
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	sort.Ints(out)
	return out, nil
}

// equalIntSets 比较两个已排序去重的分区集合是否相等。
func equalIntSets(a, b []int) bool {
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

// diffIntSets 比较已排序的期望集合 want 与实际集合 got，
// 返回漏报（want 有 got 无）与多报（got 有 want 无）的分区。
func diffIntSets(want, got []int) (missing, extra []int) {
	i, j := 0, 0
	for i < len(want) || j < len(got) {
		switch {
		case i < len(want) && (j >= len(got) || want[i] < got[j]):
			missing = append(missing, want[i])
			i++
		case j < len(got) && (i >= len(want) || got[j] < want[i]):
			extra = append(extra, got[j])
			j++
		default:
			i++
			j++
		}
	}
	return missing, extra
}

// persistLocked 将当前完整状态写入 Store。调用方必须已持有 c.mu，
// 保证落盘的是一条一致的版本序列中的某个快照。
func (c *Coordinator) persistLocked() error {
	if err := c.store.Save(c.snapshotLocked()); err != nil {
		return fmt.Errorf("persist coordinator state: %w", err)
	}
	return nil
}
