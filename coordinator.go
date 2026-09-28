package consumergroups

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// DefaultSessionTimeout 是未显式指定时的默认会话超时。
const DefaultSessionTimeout = 30 * time.Second

// DefaultStaticRetention 是静态成员未显式指定离线保留期时的默认值。
const DefaultStaticRetention = 5 * time.Minute

// Coordinator 是消费组协调器：管理组成员、分配版本（generation）、
// 协作式两阶段再均衡、静态成员身份/会话栅栏与位点提交。
//
// 版本状态机（单调，只会沿一个方向推进）：
//
//	stable ──成员加入/离开/超时/静态成员清退──▶ revoking ──全部旧所有者确认撤销──▶ stable
//	           generation +=1                   （同一 generation 内收敛）
//
// 每次成员变化都使 generation +1 并重新计算目标所有权（target）：
//   - 新旧版本中所有者不变的分区「未受影响」，始终由原成员继续消费；
//   - 无主分区立即分派给新所有者；
//   - 离开/超时/被清退成员名下的剩余分区被协调器强制回收，立即移交目标所有者；
//   - 在两个在线存活成员之间转移的分区进入「待撤销」：旧所有者确认撤销之前，
//     新所有者不能取得该分区（生效所有权仍指向旧所有者）。
//
// 静态成员（static membership）：
//   - 成员以稳定实例 ID 加入，每次成功加入获得严格递增的会话版本
//     （session version）；同一实例两个进程并发加入时，只有会话版本更高者
//     成为当前会话，其余立即收到 ErrStaleSession；
//   - 静态实例断线（会话超时）后在保留期（retention）内**不**触发再均衡、
//     分区原样保留；它以更高会话版本重连即可继续原分配，分配版本不变；
//   - 保留期内旧进程的心跳、撤销确认、位点提交全部被会话版本栅栏拒绝；
//   - 超过保留期、主动离开或被管理员移除时，实例才被清退，其分区作为一个
//     **整批**纳入协作式再均衡，且每批只能转移给同一个新所有者。
//
// 并发模型：所有变更操作（心跳、离开、移除、超时扫描、撤销确认、加入、提交）
// 在同一把互斥锁下串行执行，因此无论以何种顺序并发到达，都只会形成一条
// 单调的版本状态机序列；携带旧分配版本或旧会话版本的迟到操作在校验处即被
// 拒绝，无法复活成员、无法提前完成再均衡，也无法覆盖新生效的所有权或位点。
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
	retention := opts.StaticRetention
	if retention <= 0 {
		retention = DefaultStaticRetention
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
		name:            opts.Name,
		partitions:      opts.Partitions,
		sessionTimeout:  timeout,
		staticRetention: retention,
		generation:      0,
		phase:           PhaseStable,
		members:         make(map[string]*member),
		assignment:      empty,
		target:          empty.clone(),
		offsets:         make(map[int]*Offset),
	}
	return c.persistLocked()
}

// Join 以动态成员身份加入组（等价于 JoinMember(JoinOptions{Static:false})）：
// 成员 ID 为进程临时标识，断线即被剔除。保留它以兼容动态成员场景。
func (c *Coordinator) Join(groupName, memberID string) (JoinResult, error) {
	return c.JoinMember(JoinOptions{Group: groupName, MemberID: memberID})
}

// JoinMember 将成员加入组。
//
// 动态成员（Static=false）：触发一次协作式再均衡，分配版本 +1，重新计算
// 目标所有权；重复加入同一成员 ID 返回 ErrMemberAlreadyExists。
//
// 静态成员（Static=true，见 JoinOptions 的会话版本规则）：
//   - 首次加入：创建稳定实例身份，获得会话版本（SessionVersion>0 时用它，
//     否则协调器分配 1），并像普通加入一样触发一次再均衡；
//   - 保留期内以**更高**会话版本重连（实例在线时即接管，或离线后重连）：
//     不推进分配版本、不转移任何分区，新会话直接继续原分配，
//     返回 Reconnected=true；
//   - 用不高于当前会话版本的值加入返回 ErrStaleSession：同一实例的两个进程
//     并发加入时只有版本更高者成为当前会话，失败者立即获知，不会有两个
//     当前会话并存。
func (c *Coordinator) JoinMember(opts JoinOptions) (JoinResult, error) {
	if opts.MemberID == "" {
		return JoinResult{}, fmt.Errorf("%w: member id is required", ErrInvalidArgument)
	}
	if opts.SessionVersion < 0 {
		return JoinResult{}, fmt.Errorf("%w: session version must not be negative, got %d",
			ErrInvalidArgument, opts.SessionVersion)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(opts.Group)
	if err != nil {
		return JoinResult{}, err
	}

	now := c.clock.Now()
	if existing, ok := g.members[opts.MemberID]; ok {
		// 身份类型冲突：同一 ID 不能在动态/静态两种身份间切换。
		if existing.static != opts.Static {
			return JoinResult{}, fmt.Errorf("%w: group=%q member=%q existing static=%v join static=%v",
				ErrStaticIdentityConflict, opts.Group, opts.MemberID, existing.static, opts.Static)
		}
		if !opts.Static {
			return JoinResult{}, fmt.Errorf("%w: group=%q member=%q",
				ErrMemberAlreadyExists, opts.Group, opts.MemberID)
		}

		// 静态实例：会话版本栅栏，只有更高版本才能接管/重连。
		want := opts.SessionVersion
		if want <= existing.sessionVersion {
			return JoinResult{}, &StaleSessionError{
				Group: opts.Group, Member: opts.MemberID,
				Want: existing.sessionVersion, Got: opts.SessionVersion,
				Offline: !existing.online,
			}
		}
		reconnected := true
		wasOffline := !existing.online
		existing.sessionVersion = want
		existing.online = true
		existing.offlineAt = time.Time{}
		existing.retainUntil = time.Time{}
		existing.lastHeartbeatAt = now
		if opts.Retention > 0 {
			g.staticRetention = opts.Retention
		}
		// 保留期内重连或在线接管：分配版本不变，分区不转移。
		// 若该实例离线期间 leader 已空缺（其余在线成员都离开），重连后恢复 leader。
		if wasOffline {
			if cur, ok := g.members[g.leader]; !ok || (cur.static && !cur.online) {
				g.leader = pickLeader(g.members)
			}
		}
		// （若保留期已过，身份已被超时扫描清退，走到的是下面的新建分支。）
		if err := c.persistLocked(); err != nil {
			return JoinResult{}, err
		}
		res := g.joinResultLocked(opts.MemberID)
		res.Reconnected = reconnected
		return res, nil
	}

	// 身份不存在：动态新成员，或静态实例首次加入（也含保留期过期清退后的重新加入）。
	m := &member{
		id:              opts.MemberID,
		static:          opts.Static,
		online:          true,
		joinedAt:        now,
		lastHeartbeatAt: now,
		requests:        make(map[string]requestRecord),
	}
	if opts.Static {
		m.sessionVersion = opts.SessionVersion
		if m.sessionVersion <= 0 {
			m.sessionVersion = 1
		}
		if opts.Retention > 0 {
			g.staticRetention = opts.Retention
		}
	}
	g.members[opts.MemberID] = m
	if g.leader == "" {
		g.leader = opts.MemberID
	}
	c.rebalanceLocked(g, nil, now)
	if err := c.persistLocked(); err != nil {
		return JoinResult{}, err
	}
	res := g.joinResultLocked(opts.MemberID)
	res.SessionVersion = m.sessionVersion
	return res, nil
}

// Heartbeat 上报动态成员心跳（等价于会话版本 0 的 HeartbeatSession）。
func (c *Coordinator) Heartbeat(groupName, memberID string, generation int64) (HeartbeatResult, error) {
	return c.HeartbeatSession(groupName, HeartbeatRequest{
		MemberID: memberID, Generation: generation,
	})
}

// HeartbeatRequest 是一次心跳请求；静态成员必须回传 JoinMember 赋予的
// 当前会话版本，动态成员传 0。
type HeartbeatRequest struct {
	MemberID       string
	Generation     int64
	SessionVersion int64
}

// HeartbeatSession 上报成员心跳。
//
// 只有同时携带当前分配版本与当前会话版本的心跳才被接受：
//   - 分配版本过旧/超前返回 ErrIllegalGeneration；
//   - 静态成员的会话版本与当前会话不一致（已被更高版本接管的旧进程），
//     或实例正处于离线保留期，返回 ErrStaleSession；
//   - 成员已被清退返回 ErrMemberNotFound。
//
// 被接受的心跳刷新会话截止时间。撤销阶段心跳同样被接受（成员一边继续消费
// 未受影响分区、一边等待撤销确认），返回值携带生效所有权、目标所有权与
// 该成员尚待撤销的分区列表。心跳本身不推进版本状态机。
func (c *Coordinator) HeartbeatSession(groupName string, req HeartbeatRequest) (HeartbeatResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return HeartbeatResult{}, err
	}
	// 先校验分配版本：迟到的心跳即使指向仍存在的成员也不得生效。
	if err := g.checkGeneration(req.Generation); err != nil {
		return HeartbeatResult{}, err
	}
	m, ok := g.members[req.MemberID]
	if !ok {
		return HeartbeatResult{}, fmt.Errorf("%w: group=%q member=%q",
			ErrMemberNotFound, groupName, req.MemberID)
	}
	if err := g.checkSession(m, req.SessionVersion); err != nil {
		return HeartbeatResult{}, err
	}
	m.lastHeartbeatAt = c.clock.Now()
	if err := c.persistLocked(); err != nil {
		return HeartbeatResult{}, err
	}
	return g.heartbeatResultLocked(req.MemberID), nil
}

// Leave 让成员主动离开组，触发再均衡（分配版本 +1）。
//
// 动态成员：直接剔除，其名下分区（含尚待撤销分区）被强制回收并立即移交。
// 静态成员：主动退出意味着身份**立即清退**（不是短暂断线，不进入保留期），
// 其名下分区作为一个整批强制回收，在本次再均衡中只转移给同一个新所有者。
// 成员不存在时返回 ErrMemberNotFound。
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
	departedStatic := c.departMemberLocked(g, m)
	c.rebalanceLocked(g, departedStatic, c.clock.Now())
	return c.persistLocked()
}

// RemoveMember 由管理员移除成员，语义与主动离开一致：动态成员直接剔除，
// 静态成员立即清退（不等保留期），整批分区只移交给一个新所有者。
// 成员不存在时返回 ErrMemberNotFound。
func (c *Coordinator) RemoveMember(groupName, memberID string) error {
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
	departedStatic := c.departMemberLocked(g, m)
	c.rebalanceLocked(g, departedStatic, c.clock.Now())
	return c.persistLocked()
}

// ExpireGroup 扫描单个组：
//   - 在线动态成员会话超时（now - lastHeartbeat > SessionTimeout）：直接剔除；
//   - 在线静态成员会话超时：标记为「暂时离线」，进入离线保留期——
//     不触发再均衡、分区原样保留；
//   - 离线静态成员超过保留期（now > RetainUntil）：清退身份，其整批分区
//     纳入协作式再均衡。
//
// 只要有成员被**真正移除**（动态超时或静态保留期过期）就触发一次再均衡；
// 仅静态成员进入离线保留期不推进版本。返回被移除成员 ID（升序）；
// 转为离线保留期的实例可通过 Status().PendingTransfers / StaticInstances 查询。
func (c *Coordinator) ExpireGroup(groupName string, now time.Time) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, err := c.groupLocked(groupName)
	if err != nil {
		return nil, err
	}
	expired, stateChanged := c.expireLocked(g, now)
	if len(expired) == 0 && !stateChanged {
		return nil, nil
	}
	return expired, c.persistLocked()
}

// ExpireAll 对所有组执行一次超时扫描（组名升序处理，保证行为确定）。
// 返回本次扫描中状态发生变化并已持久化的组名：既包括发生成员移除并再均衡
// 的组，也包括仅有静态成员转入离线保留期（不推进版本）的组。
// 典型用法是由后台定时器周期调用。
func (c *Coordinator) ExpireAll(now time.Time) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	names := make([]string, 0, len(c.groups))
	for name := range c.groups {
		names = append(names, name)
	}
	sort.Strings(names)

	var changedGroups []string
	for _, name := range names {
		expired, stateChanged := c.expireLocked(c.groups[name], now)
		if len(expired) > 0 || stateChanged {
			changedGroups = append(changedGroups, name)
		}
	}
	if len(changedGroups) == 0 {
		return nil, nil
	}
	return changedGroups, c.persistLocked()
}

// AckRevocation 确认成员已在指定分配版本下停止消费给定分区集合、可以移交。
//
// 校验顺序：组存在 -> 分配版本严格匹配 -> 成员在组 -> 会话版本栅栏
// （静态成员的旧进程即使携带正确分配版本也被拒）-> 成员在本版本确有撤销义务
// -> 分区全部合法 -> 确认集合与应撤销集合精确相等。
//
// 漏项（少交）、额外分区（多交，含替别人确认）返回 ErrRevocationMismatch，
// 且不转移任何分区；旧版本确认返回 ErrIllegalGeneration，不能提前完成再均衡；
// 旧会话确认返回 ErrStaleSession，不能确认新会话版本下的撤销集合；
// 稳定阶段或无撤销义务的确认返回 ErrNoRevocationInProgress。
//
// 确认可安全重试：同一版本、同一会话内重复确认（无论再均衡是否已随本次
// 确认收敛）都按幂等成功返回，不会重复转移或报错。确认一旦受理，对应分区
// 的生效所有权立即从旧所有者切到目标所有者；全体义务成员确认完毕后版本收敛。
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
	m, ok := g.members[req.MemberID]
	if !ok {
		return RevocationAckResult{}, fmt.Errorf("%w: group=%q member=%q",
			ErrMemberNotFound, groupName, req.MemberID)
	}
	// 会话栅栏先于一切状态机推进：旧会话不能确认（哪怕是幂等重放）当前版本。
	if err := g.checkSession(m, req.SessionVersion); err != nil {
		return RevocationAckResult{}, err
	}

	// 稳定阶段的同版本、同会话确认视为已完成确认的幂等重放（响应可能在网络中丢失）。
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
// 校验顺序：组存在 -> 分配版本匹配 -> 成员在组 -> 会话版本栅栏 ->
// 分区合法 -> 成员是当前生效所有者 -> 幂等请求号 -> 位点单调性。
//
// 会话栅栏：静态成员被更高会话版本接管后，旧进程的提交（即使携带正确的
// 分配版本、即使是原请求号重放）立即返回 ErrStaleSession；离线保留期内
// 没有当前会话，任何提交都被拒。新会话接管、旧会话最终位点提交与超时扫描
// 即使并发相撞，也因锁内串行 + 会话栅栏 + 位点单调性而不会让位点倒退。
//
// 所有权栅栏：提交只认「生效所有权」。待撤销分区在旧所有者确认撤销前仍归
// 其有效持有，因此它可以提交最终位点；一旦确认受理、分区转移完成，旧成员
// 的迟到提交（即使携带当前版本）立即返回 ErrNotOwner。
//
// 幂等性：同一成员对同一请求号的重放（分区与位点完全一致）直接成功且
// 不重复推进；同请求号携带不同分区或位点返回 ErrRequestConflict。
// 静态成员的幂等记录跨重连接管保留（同一实例身份），旧会话重放仍先被
// 会话栅栏阻止，新会话可安全重试。
// 单调性：默认位点不得后退，后退返回 ErrOffsetBacktrack。
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
		return CommitResult{}, fmt.Errorf("%w: group=%q member=%q",
			ErrMemberNotFound, groupName, req.MemberID)
	}
	if err := g.checkSession(m, req.SessionVersion); err != nil {
		return CommitResult{}, err
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
// 目标所有权、待撤销集合、各成员确认进度、动态/静态成员（含离线静态实例的
// 在线状态、当前会话版本、保留期限）、待转移分区批次与位点等全部协调状态。
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
	name            string
	partitions      int
	sessionTimeout  time.Duration
	staticRetention time.Duration
	generation      int64
	phase           RebalancePhase
	leader          string
	members         map[string]*member
	// assignment 当前生效所有权：未受影响分区与已移交完成的分区指向当前可消费方，
	// 待撤销分区仍指向旧所有者。其 Generation 始终等于组当前 generation。
	assignment Assignment
	// target 本版本的目标所有权，发布后不可变；stable 时与 assignment 一致。
	target Assignment
	// revokeRequired 成员 -> 本版本它必须撤销的分区集合（两在线成员间转移的分区）。
	revokeRequired map[string]map[int]struct{}
	// revokeAcked 成员 -> 已确认撤销的分区集合（revokeRequired 的子集）。
	revokeAcked   map[string]map[int]struct{}
	offsets       map[int]*Offset
	lastRebalance time.Time
}

type member struct {
	id     string
	static bool
	// online 是否有活跃会话。静态成员离线保留期内为 false 但仍是成员；
	// 动态成员在组期间恒为 true。
	online bool
	// sessionVersion 当前会话版本。静态成员每次成功加入严格递增，用于栅栏旧进程；
	// 动态成员恒为 0。
	sessionVersion  int64
	joinedAt        time.Time
	lastHeartbeatAt time.Time
	offlineAt       time.Time
	retainUntil     time.Time
	// requests 记录该成员已受理的幂等请求号及其提交内容，成员被清退后随之清除。
	// 静态成员跨重连接管保留（同一实例身份）：新会话可看到旧会话的最终提交记录，
	// 旧会话重放则先被会话版本栅栏阻止。
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

// checkSession 校验请求来自成员的当前会话。
// 动态成员没有会话版本概念，恒通过；静态成员必须在线且版本严格相等。
func (g *group) checkSession(m *member, sessionVersion int64) error {
	if !m.static {
		return nil
	}
	if !m.online {
		return &StaleSessionError{
			Group: g.name, Member: m.id,
			Want: m.sessionVersion, Got: sessionVersion, Offline: true,
		}
	}
	if sessionVersion != m.sessionVersion {
		return &StaleSessionError{
			Group: g.name, Member: m.id,
			Want: m.sessionVersion, Got: sessionVersion,
		}
	}
	return nil
}

// departMemberLocked 真正移除一个成员身份。静态成员通过 departedStatic 返回
// 给 rebalanceLocked，使其分区按整批规则规划。调用方负责随后再均衡与持久化。
func (c *Coordinator) departMemberLocked(g *group, m *member) []string {
	var departedStatic []string
	if m.static {
		departedStatic = []string{m.id}
	}
	delete(g.members, m.id)
	return departedStatic
}

// rebalanceLocked 推进一个分配版本：基于变更后的成员集合重新计算目标所有权，
// 并据此得到新的生效所有权与每成员撤销义务。
//
// departedStatic 为本代被清退的静态成员 ID（已从 g.members 删除，但变更前的
// 生效所有权仍指向它）；其分区按整批单目标规则规划。
func (c *Coordinator) rebalanceLocked(g *group, departedStatic []string, now time.Time) {
	g.generation++
	baseline := g.assignment.Owners
	target := planTarget(g, baseline, departedStatic, now)
	g.target = target
	g.leader = pickLeader(g.members)
	g.lastRebalance = now

	// 新版本一律作废旧版本的全部撤销确认，按新生效/目标差异重新计算。
	required := make(map[string]map[int]struct{})
	effective := make([]string, g.partitions)
	for p := 0; p < g.partitions; p++ {
		old := baseline[p]
		new := target.OwnerOf(p)
		switch {
		case old == new:
			// 未受影响：原成员继续消费（也覆盖双方都为空串、离线静态锚点的情形）。
			effective[p] = old
		case old == "":
			// 无主分区：新所有者立即生效取得。
			effective[p] = new
		default:
			if _, alive := g.members[old]; !alive {
				// 旧所有者已离开/超时/被清退：强制回收其剩余分区，立即移交目标
				// 所有者，不等待任何确认（new 为空串时分区回到无主）。
				effective[p] = new
			} else {
				// 能走到这里的旧主必为在线成员：离线静态成员的分区在 planTarget
				// 中被钉住，old==new，不会产生撤销义务（离线会话也无人能确认）。
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
		// 没有需要在线成员交出的分区：版本在本代内立即收敛。
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

// expireLocked 执行一次扫描：动态超时成员剔除、静态超时成员转入离线保留期、
// 保留期过期的静态实例清退。有真正移除时触发一次再均衡。
// 返回 (被移除成员 ID 升序（不含仅转入离线保留期者）, 是否有任何状态变化——
// 后者用于让调用方把「静态成员转入保留期」这一不 bump 版本的变化落盘)。
func (c *Coordinator) expireLocked(g *group, now time.Time) ([]string, bool) {
	stateChanged := false
	var dynamicExpired, staticDeparted []string
	for id, m := range g.members {
		if m.static && !m.online {
			// 已在离线保留期：检查保留期是否届满。
			if now.After(m.retainUntil) {
				staticDeparted = append(staticDeparted, id)
			}
			continue
		}
		if now.Sub(m.lastHeartbeatAt) > g.sessionTimeout {
			if m.static {
				// 静态实例断线：进入保留期，分区保留，不触发再均衡。
				m.online = false
				m.offlineAt = now
				m.retainUntil = now.Add(g.staticRetention)
				stateChanged = true
			} else {
				dynamicExpired = append(dynamicExpired, id)
			}
		}
	}

	removed := dynamicExpired
	for _, id := range dynamicExpired {
		if m, ok := g.members[id]; ok {
			c.departMemberLocked(g, m)
		}
	}
	for _, id := range staticDeparted {
		if m, ok := g.members[id]; ok {
			c.departMemberLocked(g, m)
			removed = append(removed, id)
		}
	}
	if len(removed) == 0 {
		// 可能仅有静态成员转入离线保留期：不推进版本，但若离线的是 leader，
		// 需把 leader 移交给其余在线成员。
		if stateChanged {
			g.leader = pickLeader(g.members)
		}
		return nil, stateChanged
	}
	sort.Strings(removed)
	c.rebalanceLocked(g, staticDeparted, now)
	return removed, true
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

// ownedPartitionsLocked 返回当前生效所有权下该成员持有的分区（升序）。
func (g *group) ownedPartitionsLocked(memberID string) []int {
	var out []int
	for p, owner := range g.assignment.Owners {
		if owner == memberID {
			out = append(out, p)
		}
	}
	return out
}

func (g *group) joinResultLocked(memberID string) JoinResult {
	sv := int64(0)
	if m, ok := g.members[memberID]; ok {
		sv = m.sessionVersion
	}
	return JoinResult{
		MemberID:         memberID,
		Generation:       g.generation,
		Phase:            g.phase,
		Leader:           g.leader,
		SessionVersion:   sv,
		Assignment:       g.assignment.clone(),
		TargetAssignment: g.target.clone(),
	}
}

func (g *group) heartbeatResultLocked(memberID string) HeartbeatResult {
	sv := int64(0)
	if m, ok := g.members[memberID]; ok {
		sv = m.sessionVersion
	}
	return HeartbeatResult{
		Generation:       g.generation,
		SessionVersion:   sv,
		Online:           true,
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
			Static:          m.static,
			Online:          m.online,
			SessionVersion:  m.sessionVersion,
			OfflineAt:       m.offlineAt,
			RetainUntil:     m.retainUntil,
			OwnedPartitions: g.ownedPartitionsLocked(m.id),
		})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })

	var staticInstances []StaticInstanceStatus
	var pendingTransfers []PendingTransfer
	for _, m := range members {
		if !m.Static {
			continue
		}
		staticInstances = append(staticInstances, StaticInstanceStatus{
			InstanceID:      m.ID,
			Online:          m.Online,
			SessionVersion:  m.SessionVersion,
			JoinedAt:        m.JoinedAt,
			LastHeartbeatAt: m.LastHeartbeatAt,
			OfflineAt:       m.OfflineAt,
			RetainUntil:     m.RetainUntil,
			OwnedPartitions: append([]int(nil), m.OwnedPartitions...),
		})
		if !m.Online {
			pendingTransfers = append(pendingTransfers, PendingTransfer{
				InstanceID:  m.ID,
				Partitions:  append([]int(nil), m.OwnedPartitions...),
				RetainUntil: m.RetainUntil,
				OfflineAt:   m.OfflineAt,
			})
		}
	}

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
		StaticInstances:    staticInstances,
		PendingTransfers:   pendingTransfers,
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
