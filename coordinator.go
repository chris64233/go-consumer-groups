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
// 协作式两阶段再均衡与位点提交。
//
// 版本状态机（单调，只会沿一个方向推进）：
//
//	stable ──成员加入/离开/超时──▶ revoking ──全部旧所有者确认撤销──▶ stable
//	           generation +=1        （同一 generation 内收敛）
//
// 每次成员变化都使 generation +1 并重新计算目标所有权（target）：
//   - 新旧版本中所有者不变的分区「未受影响」，始终由原成员继续消费；
//   - 无主分区立即分派给新所有者；
//   - 离开/超时成员名下的剩余分区被协调器强制回收，立即移交目标所有者；
//   - 在两个存活成员之间转移的分区进入「待撤销」：旧所有者确认撤销之前，
//     新所有者不能取得该分区（生效所有权仍指向旧所有者）。
//
// 并发模型：所有变更操作（心跳、离开、超时扫描、撤销确认、加入、提交）
// 在同一把互斥锁下串行执行，因此无论以何种顺序并发到达，都只会形成一条
// 单调的版本状态机序列；携带旧版本的迟到操作在版本校验处即被拒绝，
// 无法复活成员、无法提前完成再均衡，也无法覆盖新生效的所有权。
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
// 初始分配版本为 0（尚无任何成员加入，无有效分配），阶段为 stable。
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
	empty := Assignment{
		Generation: 0,
		CreatedAt:  c.clock.Now(),
		Owners:     make([]string, opts.Partitions),
	}
	c.groups[opts.Name] = &group{
		name:           opts.Name,
		partitions:     opts.Partitions,
		sessionTimeout: timeout,
		generation:     0,
		phase:          PhaseStable,
		members:        make(map[string]*member),
		assignment:     empty,
		target:         empty.clone(),
		offsets:        make(map[int]*Offset),
	}
	return c.persistLocked()
}

// Join 将成员加入组，触发一次协作式再均衡：分配版本 +1，重新计算目标所有权。
//
// 未受影响分区继续由原成员消费；从存活成员转移给其他存活成员的分区进入
// 待撤销状态，返回值的 Assignment（生效所有权）里新成员尚未取得它们，
// TargetAssignment 才是最终目标；无主分区则立即归新成员生效持有。
// 重复加入同一成员 ID 返回 ErrMemberAlreadyExists。
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
	return g.joinResultLocked(memberID), nil
}

// Heartbeat 上报成员心跳。只有携带当前分配版本的心跳才被接受；
// 携带旧版本（或超前版本）的心跳返回 ErrIllegalGeneration，
// 成员已被剔除时返回 ErrMemberNotFound。被接受的心跳会刷新会话截止时间。
//
// 撤销阶段心跳同样被接受（成员一边继续消费未受影响分区、一边等待撤销确认），
// 返回值携带生效所有权、目标所有权与该成员尚待撤销的分区列表。
// 心跳本身不推进版本状态机。
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
	return g.heartbeatResultLocked(memberID), nil
}

// Leave 让成员主动离开组，触发再均衡（分配版本 +1）。
// 离开成员名下的剩余分区（含其尚待撤销的分区）被协调器强制回收并立即
// 移交目标所有者，不需要它的撤销确认。成员不存在时返回 ErrMemberNotFound。
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
// 超时成员的剩余分区被强制回收。没有成员超时时不推进版本，返回空切片。
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

// AckRevocation 确认成员已在指定分配版本下停止消费给定分区集合、可以移交。
//
// 校验顺序：组存在 -> 分配版本严格匹配当前版本（旧版本/超前版本一律拒绝）->
// 成员在组 -> 成员在本版本确有撤销义务 -> 分区全部合法 ->
// 确认集合与应撤销集合精确相等。
//
// 漏项（少交）、额外分区（多交，含替别人确认）返回 ErrRevocationMismatch，
// 且不转移任何分区；旧版本确认返回 ErrIllegalGeneration，不能提前完成再均衡；
// 稳定阶段或无撤销义务的确认返回 ErrNoRevocationInProgress。
//
// 确认可安全重试：同一版本内重复确认（无论再均衡是否已随本次确认收敛）
// 都按幂等成功返回，不会重复转移或报错。确认一旦受理，对应分区的生效所有权
// 立即从旧所有者切到目标所有者；全体义务成员确认完毕后版本收敛为 stable。
func (c *Coordinator) AckRevocation(groupName string, req RevocationAckRequest) (RevocationAckResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return RevocationAckResult{}, err
	}
	if err := g.checkGeneration(req.Generation); err != nil {
		return RevocationAckResult{}, err
	}
	if _, ok := g.members[req.MemberID]; !ok {
		return RevocationAckResult{}, fmt.Errorf("%w: group=%q member=%q", ErrMemberNotFound, groupName, req.MemberID)
	}

	// 稳定阶段的同版本确认视为已完成确认的幂等重放（响应可能在网络中丢失）。
	if g.phase == PhaseStable {
		return RevocationAckResult{
			Generation:         g.generation,
			Phase:              PhaseStable,
			CompletedRebalance: true,
		}, nil
	}

	required, hasObligation := g.revokeRequired[req.MemberID]
	if !hasObligation || len(required) == 0 {
		// 与本成员无关的确认绝不能推动状态机、更不能提前完成整个再均衡。
		return RevocationAckResult{}, fmt.Errorf(
			"%w: group=%q member=%q generation=%d has no revocation obligation",
			ErrNoRevocationInProgress, groupName, req.MemberID, g.generation)
	}

	got := make(map[int]struct{}, len(req.Partitions))
	for _, p := range req.Partitions {
		if p < 0 || p >= g.partitions {
			return RevocationAckResult{}, fmt.Errorf("%w: group=%q partition=%d (partitions=%d)",
				ErrInvalidPartition, groupName, p, g.partitions)
		}
		got[p] = struct{}{}
	}

	// 集合必须精确相等：漏项 / 额外分区都明确拒绝，且本次不转移任何分区。
	var missing, extra []int
	for p := range required {
		if _, ok := got[p]; !ok {
			missing = append(missing, p)
		}
	}
	for p := range got {
		if _, ok := required[p]; !ok {
			extra = append(extra, p)
		}
	}
	if len(missing) > 0 || len(extra) > 0 {
		sort.Ints(missing)
		sort.Ints(extra)
		return RevocationAckResult{}, &RevocationMismatchError{
			Group:    groupName,
			Member:   req.MemberID,
			WantGen:  g.generation,
			Expected: sortedSet(required),
			Got:      sortedSet(got),
			Missing:  missing,
			Extra:    extra,
		}
	}

	// 幂等重试：该成员已确认过（其他人尚未确认，版本仍在撤销阶段）。
	already := g.revokeAcked[req.MemberID]
	if already != nil && len(already) == len(required) {
		return RevocationAckResult{
			Generation:          g.generation,
			Phase:               g.phase,
			RemainingPartitions: nil,
			CompletedRebalance:  false,
		}, nil
	}

	// 受理确认：分区生效所有权逐个移交目标所有者。
	acked := make(map[int]struct{}, len(required))
	for p := range required {
		g.assignment.Owners[p] = g.target.Owners[p]
		acked[p] = struct{}{}
	}
	g.revokeAcked[req.MemberID] = acked

	// 全体义务成员确认完毕 => 版本在同一 generation 内收敛。
	completed := true
	for member, required := range g.revokeRequired {
		if len(g.revokeAcked[member]) != len(required) {
			completed = false
			break
		}
	}
	if completed {
		c.completeRevocationLocked(g)
	}
	if err := c.persistLocked(); err != nil {
		return RevocationAckResult{}, err
	}
	return RevocationAckResult{
		Generation:          g.generation,
		Phase:               g.phase,
		RemainingPartitions: nil,
		CompletedRebalance:  completed,
	}, nil
}

// CommitOffset 提交分区位点。
//
// 校验顺序：组存在 -> 分配版本匹配 -> 成员在组 -> 分区合法 ->
// 成员是当前生效所有者 -> 幂等请求号 -> 位点单调性。
//
// 所有权栅栏：提交只认「生效所有权」。待撤销分区在旧所有者确认撤销前仍归
// 其有效持有，因此它可以提交最终位点；一旦确认受理、分区转移完成，旧成员
// 的迟到提交（即使携带当前版本）立即返回 ErrNotOwner，无法在新所有者接手后
// 覆盖位点。
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
	// 生效所有权栅栏：待撤销期间旧主可提交最终位点；转移完成后旧主被拒。
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

// Status 返回组的完整状态快照（深拷贝，调用方可安全持有）：当前生效所有权、
// 目标所有权、待撤销集合、各成员确认进度、成员、位点等全部协调状态。
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
	phase          RebalancePhase
	leader         string
	members        map[string]*member
	// assignment 当前生效所有权：未受影响分区与已移交完成的分区指向当前可消费方，
	// 待撤销分区仍指向旧所有者。其 Generation 始终等于组当前 generation。
	assignment Assignment
	// target 本版本的目标所有权，发布后不可变；stable 时与 assignment 一致。
	target Assignment
	// revokeRequired 成员 -> 本版本它必须撤销的分区集合（两存活成员间转移的分区）。
	revokeRequired map[string]map[int]struct{}
	// revokeAcked 成员 -> 已确认撤销的分区集合（revokeRequired 的子集）。
	revokeAcked   map[string]map[int]struct{}
	offsets       map[int]*Offset
	lastRebalance time.Time
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

// rebalanceLocked 推进一个分配版本：基于变更后的成员集合重新计算目标所有权，
// 并据此得到新的生效所有权与每成员撤销义务。
func (c *Coordinator) rebalanceLocked(g *group, now time.Time) {
	g.generation++
	target := planAssignment(g.generation, g.partitions, g.members, now)
	g.target = target
	g.leader = pickLeader(g.members)
	g.lastRebalance = now

	// 新版本一律作废旧版本的全部撤销确认，按新生效/目标差异重新计算。
	required := make(map[string]map[int]struct{})
	effective := make([]string, g.partitions)
	for p := 0; p < g.partitions; p++ {
		old := g.assignment.OwnerOf(p)
		new := target.OwnerOf(p)
		switch {
		case old == new:
			// 未受影响：原成员继续消费（也覆盖双方都为空串的情形）。
			effective[p] = old
		case old == "":
			// 无主分区：新所有者立即生效取得。
			effective[p] = new
		default:
			if _, alive := g.members[old]; !alive {
				// 旧所有者已离开/超时：强制回收其剩余分区，立即移交目标所有者，
				// 不等待任何确认（new 为空串时分区回到无主）。
				effective[p] = new
			} else {
				// 两个存活成员之间的转移：旧主确认撤销前继续有效持有。
				effective[p] = old
				set := required[old]
				if set == nil {
					set = make(map[int]struct{})
					required[old] = set
				}
				set[p] = struct{}{}
			}
		}
	}

	g.assignment = Assignment{Generation: g.generation, CreatedAt: now, Owners: effective}
	g.revokeRequired = required
	g.revokeAcked = make(map[string]map[int]struct{})
	if len(required) == 0 {
		// 没有需要存活成员交出的分区：版本在本代内立即收敛。
		c.completeRevocationLocked(g)
	} else {
		g.phase = PhaseRevoking
	}
}

// completeRevocationLocked 令生效所有权追上目标所有权，版本收敛为 stable。
func (c *Coordinator) completeRevocationLocked(g *group) {
	g.phase = PhaseStable
	g.assignment = g.target.clone()
	g.revokeRequired = nil
	g.revokeAcked = nil
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

// pendingLocked 返回成员尚未确认撤销的分区（升序）。
func (g *group) pendingLocked(memberID string) []int {
	required := g.revokeRequired[memberID]
	if len(required) == 0 {
		return nil
	}
	acked := g.revokeAcked[memberID]
	var pending []int
	for p := range required {
		if _, ok := acked[p]; !ok {
			pending = append(pending, p)
		}
	}
	sort.Ints(pending)
	return pending
}

func (g *group) joinResultLocked(memberID string) JoinResult {
	return JoinResult{
		MemberID:         memberID,
		Generation:       g.generation,
		Phase:            g.phase,
		Leader:           g.leader,
		Assignment:       g.assignment.clone(),
		TargetAssignment: g.target.clone(),
	}
}

func (g *group) heartbeatResultLocked(memberID string) HeartbeatResult {
	return HeartbeatResult{
		Generation:       g.generation,
		Phase:            g.phase,
		Assignment:       g.assignment.clone(),
		TargetAssignment: g.target.clone(),
		Revoking:         g.pendingLocked(memberID),
	}
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

	pending := make(map[string][]int, len(g.revokeRequired))
	for id := range g.revokeRequired {
		if ps := g.pendingLocked(id); len(ps) > 0 {
			pending[id] = ps
		}
	}

	progress := make([]RevocationProgress, 0, len(g.revokeRequired))
	for id, set := range g.revokeRequired {
		required := sortedSet(set)
		acked := sortedSet(g.revokeAcked[id])
		progress = append(progress, RevocationProgress{
			MemberID: id,
			Required: required,
			Acked:    acked,
			Done:     len(acked) == len(required),
		})
	}
	sort.Slice(progress, func(i, j int) bool { return progress[i].MemberID < progress[j].MemberID })

	return GroupStatus{
		Name:               g.name,
		Partitions:         g.partitions,
		Generation:         g.generation,
		Phase:              g.phase,
		Leader:             g.leader,
		Members:            members,
		Assignment:         g.assignment.clone(),
		TargetAssignment:   g.target.clone(),
		PendingRevocations: pending,
		RevocationProgress: progress,
		Offsets:            offsets,
		LastRebalance:      g.lastRebalance,
	}
}

func (a Assignment) clone() Assignment {
	owners := make([]string, len(a.Owners))
	copy(owners, a.Owners)
	a.Owners = owners
	return a
}

// sortedSet 返回集合的升序切片；nil/空集合返回 nil。
func sortedSet(set map[int]struct{}) []int {
	if len(set) == 0 {
		return nil
	}
	out := make([]int, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Ints(out)
	return out
}

// persistLocked 将当前完整状态写入 Store。调用方必须已持有 c.mu，
// 保证落盘的是一条一致的版本序列中的某个快照。
func (c *Coordinator) persistLocked() error {
	if err := c.store.Save(c.snapshotLocked()); err != nil {
		return fmt.Errorf("persist coordinator state: %w", err)
	}
	return nil
}
