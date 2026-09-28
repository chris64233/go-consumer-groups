package consumergroups

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// DefaultSessionTimeout 是未显式指定时的默认会话超时。
const DefaultSessionTimeout = 30 * time.Second

// Coordinator 是消费组协调器：管理组成员、静态成员身份与会话栅栏、
// 分配版本（generation）、协作式两阶段再均衡与位点提交。
//
// 版本状态机（单调，只会沿一个方向推进）：
//
//	stable ──成员加入/真正退出/保留期届满──▶ revoking ──全部旧所有者确认撤销──▶ stable
//	           generation +=1                 （同一 generation 内收敛）
//
// 每次「真正的」成员变化都使 generation +1 并重新计算目标所有权（target）：
//   - 新旧版本中所有者不变的分区「未受影响」，始终由原成员继续消费；
//   - 无主分区立即分派给新所有者；
//   - 离开/超时的动态成员、主动退出或保留期届满的静态实例，其名下分区被协调器
//     强制回收；静态实例的原持有分区作为**一整批**立即移交给唯一后继；
//   - 在两个在线成员之间转移的分区进入「待撤销」：旧所有者确认撤销之前，
//     新所有者不能取得该分区（生效所有权仍指向旧所有者）。
//
// 静态成员的「暂时离线」**不**推进版本状态机：会话超时后实例在保留期内仍占有
// 分区，分区既不进入待撤销也不安排新所有者；保留期内以更高会话版本重连则继续
// 原分配。
//
// 并发模型：所有变更操作（心跳、离开、超时扫描、撤销确认、加入、提交）
// 在同一把互斥锁下串行执行，因此无论以何种顺序并发到达，都只会形成一条
// 单调的版本状态机序列；携带旧版本或旧会话版本的迟到操作在版本/会话校验处
// 即被拒绝，无法复活成员、无法提前完成再均衡，也无法覆盖新生效的所有权。
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
		instances:      make(map[string]*staticInstance),
		deadSessions:   make(map[string]deadSession),
		assignment:     empty,
		target:         empty.clone(),
		offsets:        make(map[int]*Offset),
	}
	return c.persistLocked()
}

// Join 将一个普通（动态）成员加入组，触发一次协作式再均衡：分配版本 +1。
// 重复加入同一成员 ID、或该 ID 已被某静态实例（含保留期内离线者）占用时
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
	// 所有权字符串命名空间里动态成员 ID 与静态实例 ID 不得重名。
	if _, ok := g.instances[memberID]; ok {
		return JoinResult{}, fmt.Errorf("%w: group=%q id=%q is a static instance", ErrMemberAlreadyExists, groupName, memberID)
	}

	now := c.clock.Now()
	g.members[memberID] = &member{
		id:              memberID,
		joinedAt:        now,
		lastHeartbeatAt: now,
		requests:        make(map[string]requestRecord),
	}
	c.rebalanceLocked(g, now, nil)
	if err := c.persistLocked(); err != nil {
		return JoinResult{}, err
	}
	return g.joinResultLocked(memberID), nil
}

// JoinStatic 让一个静态成员以稳定实例标识加入组。
//
//   - 实例首次加入：分配会话版本 1，像普通加入一样触发一次协作式再均衡，
//     返回 Rejoined=false；
//   - 实例保留期内断线后重连（或同一实例的新进程接管在线会话）：会话版本 +1，
//     旧会话立即被栅栏，**不触发再均衡**，实例继续原分配，返回 Rejoined=true；
//   - 超过保留期（实例已被清退）后重连等价于首次加入，会话版本重新从 1 开始。
//
// 同一实例的两个进程并发加入时，只允许会话版本更高（后到）的会话成为当前会话。
func (c *Coordinator) JoinStatic(groupName string, opts StaticJoinOptions) (StaticJoinResult, error) {
	if opts.InstanceID == "" {
		return StaticJoinResult{}, fmt.Errorf("%w: static instance id is required", ErrInvalidArgument)
	}
	if opts.SessionID == "" {
		return StaticJoinResult{}, fmt.Errorf("%w: static session id is required", ErrInvalidArgument)
	}
	if opts.Retention <= 0 {
		return StaticJoinResult{}, fmt.Errorf("%w: static retention must be positive, got %s", ErrInvalidArgument, opts.Retention)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return StaticJoinResult{}, err
	}
	// 所有权字符串命名空间：动态成员 ID、静态实例 ID、活动会话 ID 两两不得重名。
	// 唯一例外是实例以自己的实例 ID 作为会话 ID（S == I）或带着当前会话重入。
	if m, ok := g.members[opts.InstanceID]; ok && (!m.static || m.instance != opts.InstanceID) {
		return StaticJoinResult{}, fmt.Errorf("%w: group=%q id=%q already in use", ErrMemberAlreadyExists, groupName, opts.InstanceID)
	}
	if opts.SessionID != opts.InstanceID {
		if m, ok := g.members[opts.SessionID]; ok && (!m.static || m.instance != opts.InstanceID) {
			return StaticJoinResult{}, fmt.Errorf("%w: group=%q session=%q", ErrMemberAlreadyExists, groupName, opts.SessionID)
		}
		if _, isInstance := g.instances[opts.SessionID]; isInstance {
			return StaticJoinResult{}, fmt.Errorf("%w: group=%q session=%q is another static instance id",
				ErrMemberAlreadyExists, groupName, opts.SessionID)
		}
	}

	now := c.clock.Now()
	inst, exists := g.instances[opts.InstanceID]
	fresh := !exists
	var expiryBatches map[string][]int

	// 保留期已过但扫描尚未发生：按真正退出处理——旧分区作为整批在本次再均衡
	// 中移交唯一后继，实例身份重新从会话版本 1 开始。
	if exists && !inst.online && now.After(inst.retainUntil) {
		expiryBatches = c.evictInstanceLocked(g, inst)
		fresh = true
	}

	if fresh {
		inst = &staticInstance{
			id:        opts.InstanceID,
			joinedAt:  now,
			retention: opts.Retention,
			version:   1,
			online:    true,
			sessionID: opts.SessionID,
			requests:  make(map[string]requestRecord),
		}
		inst.lastHeartbeatAt = now
		g.instances[inst.id] = inst
		g.members[opts.SessionID] = newSessionMember(inst, now)
		c.rebalanceLocked(g, now, expiryBatches)
		if err := c.persistLocked(); err != nil {
			return StaticJoinResult{}, err
		}
		return g.staticJoinResultLocked(inst, false), nil
	}

	// 已存在实例：重连或并发接管。
	if inst.online && inst.sessionID == opts.SessionID {
		// 同会话幂等重入（加入响应丢失后的重试）：不提升版本、不改变任何状态。
		return g.staticJoinResultLocked(inst, true), nil
	}
	rejoined := true
	if inst.online {
		// 并发接管：旧会话退场为死会话并被栅栏。
		g.supersedeSessionLocked(inst, now)
	} else {
		// 保留期内重连：实例回到在线。离线时旧会话已入坟场用于栅栏断线后的
		// 迟到操作；若新进程沿用同一会话 ID，则把它从坟场移除（会话重新激活，
		// 旧进程的在途请求仍会因会话版本递增被栅栏）；换用新会话 ID 时旧记录
		// 保留，继续拒绝旧进程。
		oldSession := inst.sessionID
		if oldSession != "" && oldSession == opts.SessionID {
			delete(g.deadSessions, oldSession)
		}
		inst.online = true
		inst.offlineAt = time.Time{}
		inst.retainUntil = time.Time{}
	}
	if opts.Retention > 0 {
		inst.retention = opts.Retention
	}
	inst.version++
	inst.sessionID = opts.SessionID
	inst.lastHeartbeatAt = now
	g.members[opts.SessionID] = newSessionMember(inst, now)
	g.leader = pickLeaderPrincipals(g.principalsLocked())
	if err := c.persistLocked(); err != nil {
		return StaticJoinResult{}, err
	}
	return g.staticJoinResultLocked(inst, rejoined), nil
}

// Heartbeat 上报成员心跳。动态成员传 memberID + generation（SessionVersion=0）；
// 静态成员传当前会话 ID + generation + 加入时获得的会话版本。
//
// 只有携带当前分配版本、且（静态成员）当前会话版本的心跳才被接受：
// 旧版本返回 ErrIllegalGeneration；被接管的旧进程、断线后未重连的旧会话返回
// ErrFencedSession；成员/会话不存在返回 ErrMemberNotFound。
// 心跳本身不推进版本状态机，但会刷新会话截止时间。
func (c *Coordinator) Heartbeat(groupName, memberID string, generation int64) (HeartbeatResult, error) {
	return c.HeartbeatV2(groupName, HeartbeatRequest{MemberID: memberID, Generation: generation})
}

// HeartbeatV2 是支持静态会话版本的心跳入口，语义见 Heartbeat。
func (c *Coordinator) HeartbeatV2(groupName string, req HeartbeatRequest) (HeartbeatResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return HeartbeatResult{}, err
	}
	// 先校验分配版本：迟到的心跳即使指向仍存在的会话也不得生效。
	if err := g.checkGeneration(req.Generation); err != nil {
		return HeartbeatResult{}, err
	}
	principalID, _, err := g.resolveSessionLocked(req.MemberID, req.SessionVersion)
	if err != nil {
		return HeartbeatResult{}, err
	}
	g.members[req.MemberID].lastHeartbeatAt = c.clock.Now()
	if inst := g.instances[principalID]; inst != nil {
		inst.lastHeartbeatAt = g.members[req.MemberID].lastHeartbeatAt
	}
	if err := c.persistLocked(); err != nil {
		return HeartbeatResult{}, err
	}
	return g.heartbeatResultLocked(principalID), nil
}

// Leave 让动态成员主动离开组，触发再均衡（分配版本 +1）。其名下剩余分区
// （含尚待撤销的分区）被强制回收并立即移交目标所有者。
// 静态实例请使用 LeaveStatic：对静态会话 ID 调用本方法返回 ErrInvalidArgument。
func (c *Coordinator) Leave(groupName, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return err
	}
	m, ok := g.members[memberID]
	if !ok {
		return fmt.Errorf("%w: group=%q member=%q", ErrMemberNotFound, groupName, memberID)
	}
	if m.static {
		return fmt.Errorf("%w: group=%q session=%q belongs to static instance %q, use LeaveStatic",
			ErrInvalidArgument, groupName, memberID, m.instance)
	}
	delete(g.members, memberID)
	c.rebalanceLocked(g, c.clock.Now(), nil)
	return c.persistLocked()
}

// LeaveStatic 让静态实例主动退出组（无论当前在线还是保留期内暂时离线）。
// 实例立即真正退出：其名下分区作为一整批强制回收、在本次再均衡中整体移交给
// 唯一后继；会话版本与保留身份一并清除，旧会话之后的一切操作都按未知成员拒绝。
func (c *Coordinator) LeaveStatic(groupName, instanceID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return err
	}
	inst, ok := g.instances[instanceID]
	if !ok {
		return fmt.Errorf("%w: group=%q static instance=%q", ErrMemberNotFound, groupName, instanceID)
	}
	batches := c.evictInstanceLocked(g, inst)
	c.rebalanceLocked(g, c.clock.Now(), batches)
	return c.persistLocked()
}

// RemoveInstance 供管理员移除静态实例（在线或保留期内离线均可）。
// 清退规则与 LeaveStatic 完全相同：分区整批移交唯一后继，立即触发协作式再均衡。
func (c *Coordinator) RemoveInstance(groupName, instanceID string) error {
	return c.LeaveStatic(groupName, instanceID)
}

// ExpireGroup 扫描单个组：
//   - 动态成员超过 SessionTimeout 未心跳：彻底剔除并强制回收分区；
//   - 静态会话超过 SessionTimeout 未心跳：实例转为「暂时离线」，保留期内继续
//     占有分区（不推进版本、不转移）；
//   - 离线静态实例超过保留期仍未以更高会话版本重连：真正清退，原持有分区整批
//     移交唯一后继，纳入本次协作式再均衡。
//
// 返回真正退出（动态超时 + 静态保留期届满）的成员/实例 ID（升序）；
// 仅转为暂时离线的静态会话不计入返回值，但状态同样会持久化。
func (c *Coordinator) ExpireGroup(groupName string, now time.Time) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return nil, err
	}
	removed, offlined, batches := c.expireLocked(g, now)
	if len(removed) == 0 && !offlined {
		return nil, nil
	}
	if len(removed) > 0 {
		c.rebalanceLocked(g, now, batches)
	}
	return removed, c.persistLocked()
}

// ExpireAll 对所有组执行一次超时扫描（组名升序处理，保证行为确定）。
// 返回发生真正成员退出或保留期届满清退的组名。典型用法是由后台定时器周期调用。
func (c *Coordinator) ExpireAll(now time.Time) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	names := make([]string, 0, len(c.groups))
	for name := range c.groups {
		names = append(names, name)
	}
	sort.Strings(names)

	var changed []string
	dirty := false
	for _, name := range names {
		g := c.groups[name]
		removed, offlined, batches := c.expireLocked(g, now)
		if len(removed) > 0 {
			c.rebalanceLocked(g, now, batches)
			changed = append(changed, name)
			dirty = true
		} else if offlined {
			// 仅静态离线转换：不推进版本、不计入返回值，但状态需要落盘。
			dirty = true
		}
	}
	if !dirty {
		return nil, nil
	}
	return changed, c.persistLocked()
}

// AckRevocation 确认成员已在指定分配版本下停止消费给定分区集合、可以移交。
// 静态成员须携带当前会话 ID 与会话版本；旧会话的确认返回 ErrFencedSession。
//
// 其余校验与语义同协作式再均衡：分配版本严格匹配；确认集合与应撤销集合精确
// 相等（漏项/多项返回 ErrRevocationMismatch 且本次不转移任何分区）；
// 同版本内重复确认幂等成功；全体义务成员确认完毕后版本在同一 generation 内收敛。
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
	// 会话栅栏先于 stable 幂等捷径：旧会话绝不能确认（哪怕是重复确认）新版本
	// 的撤销集合。
	principalID, _, err := g.resolveSessionLocked(req.MemberID, req.SessionVersion)
	if err != nil {
		return RevocationAckResult{}, err
	}

	// 稳定阶段的同版本确认视为已完成确认的幂等重放（响应可能在网络中丢失）。
	if g.phase == PhaseStable {
		return RevocationAckResult{
			Generation:         g.generation,
			Phase:              PhaseStable,
			CompletedRebalance: true,
		}, nil
	}

	required, hasObligation := g.revokeRequired[principalID]
	if !hasObligation || len(required) == 0 {
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
			Member:   principalID,
			WantGen:  g.generation,
			Expected: sortedSet(required),
			Got:      sortedSet(got),
			Missing:  missing,
			Extra:    extra,
		}
	}

	// 幂等重试：该主体已确认过（其他人尚未确认，版本仍在撤销阶段）。
	already := g.revokeAcked[principalID]
	if already != nil && len(already) == len(required) {
		return RevocationAckResult{
			Generation:          g.generation,
			Phase:               g.phase,
			RemainingPartitions: nil,
			CompletedRebalance:  false,
		}, nil
	}

	acked := make(map[int]struct{}, len(required))
	for p := range required {
		g.assignment.Owners[p] = g.target.Owners[p]
		acked[p] = struct{}{}
	}
	g.revokeAcked[principalID] = acked

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
// 静态成员以当前会话 ID + 会话版本提交，分区生效所有者按其实例标识判定；
// 被更高会话版本接管的旧进程在会话栅栏处直接返回 ErrFencedSession，且静态
// 成员的幂等请求记录归属于实例本身（跨重连仍生效），因此新会话的提交与旧会话
// 的最终提交在同一条单调序列上比较，位点不可能倒退。
//
// 其余语义不变：只认生效所有权（ErrNotOwner）；请求号幂等（重放/冲突）；
// 默认位点单调（ErrOffsetBacktrack）。
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
	principalID, inst, err := g.resolveSessionLocked(req.MemberID, req.SessionVersion)
	if err != nil {
		return CommitResult{}, err
	}
	if req.Partition < 0 || req.Partition >= g.partitions {
		return CommitResult{}, fmt.Errorf("%w: group=%q partition=%d (partitions=%d)",
			ErrInvalidPartition, groupName, req.Partition, g.partitions)
	}
	// 生效所有权栅栏：静态实例按实例标识判定，待撤销期间旧主可提交最终位点。
	if owner := g.assignment.OwnerOf(req.Partition); owner != principalID {
		return CommitResult{}, &NotPartitionOwnerError{
			Group: groupName, Partition: req.Partition, Owner: owner, Member: principalID,
		}
	}

	// 幂等请求号归属于「所有权主体」：动态成员为其在组生命周期，静态实例跨重连。
	requests := g.members[req.MemberID].requests
	if inst != nil {
		requests = inst.requests
	}
	if rec, seen := requests[req.RequestID]; seen {
		if rec.partition != req.Partition || rec.offset != req.Offset {
			return CommitResult{}, &RequestConflictError{
				Group: groupName, Member: principalID, RequestID: req.RequestID,
				ExistingPartition: rec.partition, ExistingOffset: rec.offset,
				GotPartition: req.Partition, GotOffset: req.Offset,
			}
		}
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
	requests[req.RequestID] = requestRecord{
		partition: req.Partition, offset: req.Offset, metadata: req.Metadata, committedAt: now,
	}
	if err := c.persistLocked(); err != nil {
		return CommitResult{}, err
	}
	return CommitResult{Offset: cur.Offset}, nil
}

// Status 返回组的完整状态快照（深拷贝）：动态成员、静态实例（含保留期内离线者）、
// 生效/目标所有权、待撤销集合、待转移分区、确认进度、位点等全部协调状态。
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

// group 是单个组的全部运行时状态。
type group struct {
	name           string
	partitions     int
	sessionTimeout time.Duration
	generation     int64
	phase          RebalancePhase
	leader         string
	// members 当前活动会话：动态成员（static=false，key==成员 ID）与在线静态
	// 实例的当前会话（static=true，key==会话 ID）。静态实例断线后其会话从此移除。
	members map[string]*member
	// instances 全部静态实例（含保留期内暂时离线者），key 为稳定实例标识；
	// 分区所有权字符串对静态成员使用该标识。
	instances map[string]*staticInstance
	// deadSessions 已退场会话 ID -> 其所属实例与退场时版本，用于把旧进程的
	// 迟到操作稳定地拒绝为 ErrFencedSession（实例被清退后记录随之删除）。
	deadSessions map[string]deadSession
	// assignment 当前生效所有权，其 Generation 始终等于组当前 generation。
	assignment Assignment
	// target 本版本的目标所有权，发布后不可变；stable 时与 assignment 一致。
	target Assignment
	// revokeRequired 所有权主体 ID（动态成员 ID 或静态实例 ID）-> 本版本它必须
	// 撤销的分区集合（两个在线主体间转移的分区）。
	revokeRequired map[string]map[int]struct{}
	// revokeAcked 主体 ID -> 已确认撤销的分区集合（revokeRequired 的子集）。
	revokeAcked   map[string]map[int]struct{}
	offsets       map[int]*Offset
	lastRebalance time.Time
}

type member struct {
	id              string
	static          bool
	instance        string // static=true 时指向 instances 中的实例标识
	sessionVersion  int64  // 静态会话版本；动态成员为 0
	joinedAt        time.Time
	lastHeartbeatAt time.Time
	// requests 仅动态成员使用；静态成员的幂等记录归属于 staticInstance。
	requests map[string]requestRecord
}

// staticInstance 是一个静态成员的持久身份，跨多次会话/重连存活。
type staticInstance struct {
	id        string
	joinedAt  time.Time
	retention time.Duration
	// version 单调会话版本：每次成功加入（重连/接管）+1。
	version int64
	online  bool
	// sessionID 当前在线会话 ID；离线时保留最后会话 ID 用于展示。
	sessionID       string
	lastHeartbeatAt time.Time
	offlineAt       time.Time
	// retainUntil 离线保留期限；在线时为零值。
	retainUntil time.Time
	// requests 幂等请求记录，作用域为实例本身，跨重连持续生效。
	requests map[string]requestRecord
}

type deadSession struct {
	instance string
	version  int64
}

type requestRecord struct {
	partition   int
	offset      int64
	metadata    string
	committedAt time.Time
}

func newSessionMember(inst *staticInstance, now time.Time) *member {
	return &member{
		id:              inst.sessionID,
		static:          true,
		instance:        inst.id,
		sessionVersion:  inst.version,
		joinedAt:        inst.joinedAt,
		lastHeartbeatAt: now,
	}
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

// resolveSessionLocked 把请求中的 (会话ID, 会话版本) 解析为所有权主体 ID，
// 并对静态会话做版本栅栏。动态成员返回其成员 ID、inst=nil。
func (g *group) resolveSessionLocked(sessionID string, version int64) (string, *staticInstance, error) {
	m, ok := g.members[sessionID]
	if ok {
		if !m.static {
			return m.id, nil, nil
		}
		inst := g.instances[m.instance]
		if version != inst.version {
			return "", nil, g.fenceError(inst, sessionID, version)
		}
		return inst.id, inst, nil
	}
	if dead, ok := g.deadSessions[sessionID]; ok {
		var inst *staticInstance
		if live, exists := g.instances[dead.instance]; exists {
			inst = live
		}
		return "", nil, g.fenceError(inst, sessionID, version)
	}
	return "", nil, fmt.Errorf("%w: group=%q member=%q", ErrMemberNotFound, g.name, sessionID)
}

func (g *group) fenceError(inst *staticInstance, sessionID string, got int64) error {
	var instance string
	var want int64
	if inst != nil {
		instance = inst.id
		if inst.online {
			want = inst.version
		}
	} else if id, ok := g.deadSessions[sessionID]; ok {
		instance = id.instance
	}
	return &FencedSessionError{Group: g.name, Instance: instance, Member: sessionID, Want: want, Got: got}
}

// principalsLocked 列出参与分配的全部主体：动态成员与静态实例（含离线者）。
func (g *group) principalsLocked() []principal {
	out := make([]principal, 0, len(g.members)+len(g.instances))
	for id, m := range g.members {
		if !m.static {
			out = append(out, principal{id: id, joinedAt: m.joinedAt, online: true})
		}
	}
	for id, inst := range g.instances {
		out = append(out, principal{
			id: id, joinedAt: inst.joinedAt, online: inst.online, static: true,
		})
	}
	return out
}

// rebalanceLocked 推进一个分配版本：基于变更后的主体集合重新计算目标所有权，
// 并据此得到新的生效所有权与每主体撤销义务。
// evictedBatches 给出本次被真正清退的静态实例原持有分区批次（整批 -> 唯一后继）。
func (c *Coordinator) rebalanceLocked(g *group, now time.Time, evictedBatches map[string][]int) {
	g.generation++

	principals := g.principalsLocked()
	g.leader = pickLeaderPrincipals(principals)

	// 保留期内离线实例仍生效持有的分区在本版本钉住（不参与再分配）。
	retained := make(map[int]string)
	offline := make(map[string]bool)
	for _, p := range principals {
		if p.static && !p.online {
			offline[p.id] = true
		}
	}
	for p := 0; p < g.partitions; p++ {
		owner := g.assignment.OwnerOf(p)
		if owner != "" && offline[owner] {
			retained[p] = owner
		}
	}

	target := planTarget(g.generation, g.partitions, principals, retained, evictedBatches, now)
	g.target = target
	g.lastRebalance = now

	required := make(map[string]map[int]struct{})
	effective := make([]string, g.partitions)
	for p := 0; p < g.partitions; p++ {
		old := g.assignment.OwnerOf(p)
		new := target.OwnerOf(p)
		switch {
		case old == new:
			effective[p] = old
		case old == "":
			// 无主分区：新所有者立即生效取得。
			effective[p] = new
		default:
			if _, online := g.onlinePrincipalLocked(old); !online {
				// 旧主体已离开/超时/被清退，或本就保留期内离线（其分区已钉住，
				// 不会走到这里）：强制回收并立即移交目标所有者。
				effective[p] = new
			} else {
				// 两个在线主体之间的转移：旧主确认撤销前继续有效持有。
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
		c.completeRevocationLocked(g)
	} else {
		g.phase = PhaseRevoking
	}
}

// onlinePrincipalLocked 返回主体是否为当前在线主体（动态成员恒在线）。
func (g *group) onlinePrincipalLocked(id string) (*principal, bool) {
	if m, ok := g.members[id]; ok {
		if m.static {
			inst := g.instances[m.instance]
			return &principal{id: inst.id, online: true, static: true}, true
		}
		return &principal{id: id, online: true}, true
	}
	if inst, ok := g.instances[id]; ok {
		return &principal{id: id, online: inst.online, static: true}, inst.online
	}
	return nil, false
}

// principalLocked 判断 id 是否为现存所有权主体（动态成员或静态实例），
// 不要求当前在线——保留期内离线的静态实例仍是主体。
func (g *group) principalLocked(id string) (*principal, bool) {
	if m, ok := g.members[id]; ok {
		if m.static {
			inst := g.instances[m.instance]
			return &principal{id: inst.id, online: true, static: true}, true
		}
		return &principal{id: id, online: true}, true
	}
	if inst, ok := g.instances[id]; ok {
		return &principal{id: id, online: inst.online, static: true}, true
	}
	return nil, false
}

// completeRevocationLocked 令生效所有权追上目标所有权，版本收敛为 stable。
func (c *Coordinator) completeRevocationLocked(g *group) {
	g.phase = PhaseStable
	g.assignment = g.target.clone()
	g.revokeRequired = nil
	g.revokeAcked = nil
}

// supersedeSessionLocked 处理在线实例的并发接管：旧会话移入坟场。
func (g *group) supersedeSessionLocked(inst *staticInstance, now time.Time) {
	old := inst.sessionID
	delete(g.members, old)
	g.deadSessions[old] = deadSession{instance: inst.id, version: inst.version}
}

// evictInstanceLocked 真正清退静态实例：收集其仍生效持有的分区批次、删除身份
// 与会话/坟场记录。返回 instanceID -> 升序分区批次（供整批单后继移交）。
func (c *Coordinator) evictInstanceLocked(g *group, inst *staticInstance) map[string][]int {
	batch := make([]int, 0)
	for p := 0; p < g.partitions; p++ {
		if g.assignment.OwnerOf(p) == inst.id {
			batch = append(batch, p)
		}
	}
	c.deleteInstanceLocked(g, inst)
	if len(batch) == 0 {
		return nil
	}
	return map[string][]int{inst.id: batch}
}

// deleteInstanceLocked 删除静态实例的活动会话、身份与相关坟场记录。
func (c *Coordinator) deleteInstanceLocked(g *group, inst *staticInstance) {
	if inst.online {
		delete(g.members, inst.sessionID)
	}
	for sid, dead := range g.deadSessions {
		if dead.instance == inst.id {
			delete(g.deadSessions, sid)
		}
	}
	delete(g.instances, inst.id)
}

// expireLocked 执行一次扫描，返回 真正退出的主体 ID（升序）、是否有静态会话
// 仅转为暂时离线、以及清退实例的分区批次。调用方据此决定是否再均衡/持久化。
func (c *Coordinator) expireLocked(g *group, now time.Time) (removed []string, offlineTransition bool, batches map[string][]int) {
	// 1) 会话超时：动态成员直接剔除；静态会话转入保留期离线。
	sessionIDs := make([]string, 0, len(g.members))
	for id := range g.members {
		sessionIDs = append(sessionIDs, id)
	}
	sort.Strings(sessionIDs)
	for _, id := range sessionIDs {
		m := g.members[id]
		if now.Sub(m.lastHeartbeatAt) <= g.sessionTimeout {
			continue
		}
		if !m.static {
			delete(g.members, id)
			removed = append(removed, id)
			continue
		}
		inst := g.instances[m.instance]
		delete(g.members, id)
		inst.online = false
		inst.sessionID = m.id
		inst.offlineAt = now
		inst.retainUntil = now.Add(inst.retention)
		g.deadSessions[id] = deadSession{instance: inst.id, version: inst.version}
		offlineTransition = true
	}

	// 静态会话转离线可能使原 leader 出缺：在不推进版本的前提下重算 leader
	// （若随后还有真正清退，rebalanceLocked 会再算一次，结果一致）。
	if offlineTransition {
		g.leader = pickLeaderPrincipals(g.principalsLocked())
	}

	// 2) 保留期届满：离线静态实例真正清退。
	instanceIDs := make([]string, 0, len(g.instances))
	for id := range g.instances {
		instanceIDs = append(instanceIDs, id)
	}
	sort.Strings(instanceIDs)
	for _, id := range instanceIDs {
		inst := g.instances[id]
		if inst.online || !now.After(inst.retainUntil) {
			continue
		}
		batch := c.evictInstanceLocked(g, inst)
		removed = append(removed, id)
		if batch != nil {
			if batches == nil {
				batches = make(map[string][]int)
			}
			batches[id] = batch[id]
		}
	}
	sort.Strings(removed)
	return removed, offlineTransition, batches
}

// pendingLocked 返回主体尚未确认撤销的分区（升序）。
func (g *group) pendingLocked(principalID string) []int {
	required := g.revokeRequired[principalID]
	if len(required) == 0 {
		return nil
	}
	acked := g.revokeAcked[principalID]
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

func (g *group) staticJoinResultLocked(inst *staticInstance, rejoined bool) StaticJoinResult {
	return StaticJoinResult{
		InstanceID:       inst.id,
		SessionID:        inst.sessionID,
		SessionVersion:   inst.version,
		Rejoined:         rejoined,
		Generation:       g.generation,
		Phase:            g.phase,
		Leader:           g.leader,
		Assignment:       g.assignment.clone(),
		TargetAssignment: g.target.clone(),
	}
}

func (g *group) heartbeatResultLocked(principalID string) HeartbeatResult {
	return HeartbeatResult{
		Generation:       g.generation,
		Phase:            g.phase,
		Assignment:       g.assignment.clone(),
		TargetAssignment: g.target.clone(),
		Revoking:         g.pendingLocked(principalID),
	}
}

func (g *group) status() GroupStatus {
	members := make([]Member, 0, len(g.members))
	for _, m := range g.members {
		vm := Member{
			ID:              m.id,
			Static:          m.static,
			Online:          true,
			SessionVersion:  m.sessionVersion,
			JoinedAt:        m.joinedAt,
			LastHeartbeatAt: m.lastHeartbeatAt,
		}
		if m.static {
			vm.InstanceID = m.instance
		}
		members = append(members, vm)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })

	instances := make([]StaticInstanceStatus, 0, len(g.instances))
	for _, inst := range g.instances {
		held := make([]int, 0)
		for p := 0; p < g.partitions; p++ {
			if g.assignment.OwnerOf(p) == inst.id {
				held = append(held, p)
			}
		}
		instances = append(instances, StaticInstanceStatus{
			InstanceID:      inst.id,
			Online:          inst.online,
			SessionID:       inst.sessionID,
			SessionVersion:  inst.version,
			JoinedAt:        inst.joinedAt,
			LastHeartbeatAt: inst.lastHeartbeatAt,
			OfflineAt:       inst.offlineAt,
			RetainUntil:     inst.retainUntil,
			HeldPartitions:  held,
		})
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].InstanceID < instances[j].InstanceID })

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

	transfers := make([]PendingTransfer, 0)
	for p := 0; p < g.partitions; p++ {
		eff := g.assignment.OwnerOf(p)
		tgt := g.target.OwnerOf(p)
		if eff == "" {
			continue
		}
		if inst, ok := g.instances[eff]; ok && !inst.online {
			tr := PendingTransfer{
				Partition:      p,
				CurrentOwner:   eff,
				Online:         false,
				InstanceID:     inst.id,
				SessionVersion: inst.version,
			}
			if eff != tgt {
				// 本版本已指定新所有者但旧实例离线：撤销义务挂起。
				tr.TargetOwner = tgt
				tr.Reason = TransferReasonSuspended
			} else {
				// 分区钉住，等待重连或保留期届满。
				tr.Reason = TransferReasonRetained
			}
			transfers = append(transfers, tr)
			continue
		}
		if eff != tgt {
			tr := PendingTransfer{
				Partition:    p,
				CurrentOwner: eff,
				TargetOwner:  tgt,
				Online:       true,
				Reason:       TransferReasonRevoking,
			}
			if inst, ok := g.instances[eff]; ok {
				tr.InstanceID = inst.id
				tr.SessionVersion = inst.version
			}
			transfers = append(transfers, tr)
		}
	}

	return GroupStatus{
		Name:               g.name,
		Partitions:         g.partitions,
		Generation:         g.generation,
		Phase:              g.phase,
		Leader:             g.leader,
		Members:            members,
		StaticInstances:    instances,
		Assignment:         g.assignment.clone(),
		TargetAssignment:   g.target.clone(),
		PendingRevocations: pending,
		RevocationProgress: progress,
		PendingTransfers:   transfers,
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
