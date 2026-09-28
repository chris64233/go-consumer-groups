package consumergroups

import "time"

// RebalancePhase 是组在单调版本状态机上所处的阶段。
type RebalancePhase string

const (
	// PhaseStable 稳定阶段：当前所有权与目标所有权一致，没有待撤销分区。
	// 成员持有的分区都可以正常消费与提交位点。
	PhaseStable RebalancePhase = "stable"
	// PhaseRevoking 撤销阶段：新分配版本已发布，部分分区正等待旧所有者
	// 确认撤销。此阶段内：待撤销分区仍由旧所有者有效持有，新所有者尚不能
	// 取得；未受影响分区继续由原成员消费。
	PhaseRevoking RebalancePhase = "revoking"
)

// Member 是组内一个消费者成员的运行时状态。
//
// 动态成员（Static=false）以进程生成的成员 ID 标识，断线即被剔除；
// 静态成员（Static=true）以稳定实例 ID 标识，其 ID 在多次会话间保持不变，
// 会话版本（SessionVersion）单调递增用于栅栏旧进程，离线后在保留期内
// 仍占用分区、仍作为成员参与分配。
type Member struct {
	// ID 成员唯一标识；静态成员即其实例 ID，跨多次会话保持不变。
	ID string
	// JoinedAt 该身份首次加入时间（静态成员重连/接管不刷新，用于 leader 年资排序）。
	JoinedAt time.Time
	// LastHeartbeatAt 最近一次被当前会话接受的心跳时间。
	LastHeartbeatAt time.Time

	// Static 为 true 表示静态成员（稳定实例身份 + 会话版本 + 离线保留期）。
	Static bool
	// Online 当前是否有活跃会话在线。静态成员离线保留期内为 false，但身份与
	// 分区所有权都保留；动态成员恒为 true（在组期间）。
	Online bool
	// SessionVersion 当前会话版本。静态成员每次成功加入获得一个严格递增的
	// 新版本；旧进程携带更小版本发起的心跳/撤销确认/位点提交一律被栅栏拒绝。
	// 动态成员为 0。
	SessionVersion int64
	// OfflineAt 静态成员进入离线保留期的时间；在线时为零值。
	OfflineAt time.Time
	// RetainUntil 离线保留截止时间：超过该时刻仍未重连，实例被清退，
	// 其分区才纳入协作式再均衡。在线时为零值。
	RetainUntil time.Time
	// OwnedPartitions 当前生效所有权下该成员持有的全部分区（升序）。
	// 离线静态成员据此可查到保留期内仍由其占用、到期才会转移的分区。
	OwnedPartitions []int
}

// Assignment 是某个分配版本下「分区 -> 成员」的完整快照。
// 它一经产生即不可变：再均衡结果整体生成、整体发布，
// 任何客户端要么看到整个旧版本，要么看到整个新版本，不会读到半套分配。
type Assignment struct {
	// Generation 分配版本，从 1 开始单调递增。
	Generation int64
	// CreatedAt 该版本发布时间。
	CreatedAt time.Time
	// Owners 下标为分区编号，值为成员 ID；未被任何成员认领的分区为空串。
	Owners []string
}

// OwnerOf 返回指定分区在本快照下的所有者，空串表示无所有者。
func (a *Assignment) OwnerOf(partition int) string {
	if partition < 0 || partition >= len(a.Owners) {
		return ""
	}
	return a.Owners[partition]
}

// Offset 是单个分区已提交位点的元数据。
type Offset struct {
	Partition int
	// Offset 已提交位点（单调递增，默认不得后退）。
	Offset int64
	// Metadata 可选的不透明元数据（随位点一起提交，如重置标记）。
	Metadata string
	// CommittedAt 最近一次真正推进位点的时间。
	CommittedAt time.Time
	// LastRequestID 最近一次推进位点所用的幂等请求号，便于排障。
	LastRequestID string
}

// GroupStatus 是组的完整状态快照，用于状态查询。
type GroupStatus struct {
	Name       string
	Partitions int
	Generation int64
	// Phase 组在版本状态机上的当前阶段（stable / revoking）。
	Phase RebalancePhase
	// Leader 第一个加入组的成员；它退出后由当前在线成员中加入最早者接任，
	// 为空表示组内没有在线成员（离线保留期内的静态实例不计）。
	Leader  string
	Members []Member
	// StaticInstances 组内全部静态实例的查询视图（含保留期内暂时离线的实例），
	// 按实例 ID 升序。动态成员不在此列。
	StaticInstances []StaticInstanceStatus
	// PendingTransfers 处于「离线保留期」的静态实例仍占有的分区批次：
	// 这些分区在实例重连后继续由其消费；超过保留期未重连才会被清退并
	// 作为一个整批转移给唯一的新所有者。按实例 ID 升序。
	PendingTransfers []PendingTransfer
	// Assignment 当前生效的所有权快照。协作式再均衡期间它只包含「已经可以
	// 消费」的分区：未受影响分区保持原成员，待撤销分区仍是旧所有者，
	// 新分配的分区在旧所有者确认撤销前不会出现在这里。
	Assignment Assignment
	// TargetAssignment 本版本最终要达成的目标所有权；已稳定（PhaseStable）时
	// 与 Assignment 完全一致。撤销阶段二者的差异即尚在转移途中的分区。
	TargetAssignment Assignment
	// PendingRevocations 撤销阶段各成员仍需确认撤销的分区集合（成员 ID -> 升序分区号）。
	// 已全部确认的成员不会作为 key 出现；PhaseStable 时为空 map。
	PendingRevocations map[string][]int
	// RevocationProgress 各成员对当前版本撤销集合的确认进度：
	// Required 为该成员本版本应撤销的全部分区，Acked 为已确认的子集（均升序）。
	// 仅撤销阶段、且在本版本中需要撤销分区的成员会出现。
	RevocationProgress []RevocationProgress
	Offsets            []Offset
	LastRebalance      time.Time
}

// RevocationProgress 是单个成员在当前分配版本下的撤销确认进度。
type RevocationProgress struct {
	MemberID string
	// Required 本版本要求该成员撤销的全部分区（升序）。
	Required []int
	// Acked 该成员已确认撤销的分区（升序，是 Required 的子集）。
	Acked []int
	// Done 为 true 表示该成员的撤销义务已全部完成。
	Done bool
}

// StaticInstanceStatus 是一个静态实例（稳定成员身份）的查询视图。
type StaticInstanceStatus struct {
	// InstanceID 稳定实例标识，跨多次会话不变，也是成员 ID。
	InstanceID string
	// Online 当前是否有活跃会话在线；false 表示处于离线保留期内。
	Online bool
	// SessionVersion 当前（最近一次成功加入的）会话版本。
	SessionVersion int64
	// JoinedAt 该实例身份首次加入组的时间。
	JoinedAt time.Time
	// LastHeartbeatAt 当前会话最近一次被接受的心跳时间（离线时为断线前最后一次）。
	LastHeartbeatAt time.Time
	// OfflineAt 进入离线保留期的时间；在线时为零值。
	OfflineAt time.Time
	// RetainUntil 保留截止时间；在线时为零值。
	RetainUntil time.Time
	// OwnedPartitions 当前生效所有权下该实例仍占有的分区（升序）：
	// 在线时是其正在消费的分区，离线保留期内是其暂存、到期才会整批转移的分区。
	OwnedPartitions []int
}

// PendingTransfer 描述一个离线保留期内的静态实例所占有的分区批次。
// 超过保留期（或主动退出/管理员移除）后，该批分区在同一次再均衡中
// 只能整体转移给一个新的目标所有者。
type PendingTransfer struct {
	// InstanceID 暂时离线的静态实例 ID。
	InstanceID string
	// Partitions 该实例仍占有的整批分区（升序）。
	Partitions []int
	// RetainUntil 保留截止时间；扫描时刻超过它，该批分区才会被清退转移。
	RetainUntil time.Time
	// OfflineAt 进入离线保留期的时间。
	OfflineAt time.Time
}

// RevocationAckRequest 是一次撤销确认请求。
type RevocationAckRequest struct {
	// MemberID 确认撤销的成员。
	MemberID string
	// Generation 成员执行撤销所针对的分配版本，必须与协调器当前版本严格一致。
	Generation int64
	// SessionVersion 静态成员当前会话版本，必须与该实例登记的当前会话版本
	// 严格一致；旧进程（较小版本）的确认会被栅栏拒绝，即使它携带了正确的
	// 分配版本，也不能确认新会话版本下的撤销集合。动态成员传 0。
	SessionVersion int64
	// Partitions 成员本次确认已停止消费、可以移交的分区集合。
	// 必须与其在该版本下应撤销的集合**精确相等**：漏项、额外分区或确认一个
	// 不属于自己撤销义务的分区都会被拒绝。空集合（或 nil）表示成员在本版本
	// 没有撤销义务，仅用于核对，会以 ErrNoRevocationNeeded 明确拒绝。
	Partitions []int
}

// RevocationAckResult 是一次撤销确认的返回值。
type RevocationAckResult struct {
	Generation int64
	// Phase 确认受理后组所处的阶段；全部成员确认完毕时为 stable。
	Phase RebalancePhase
	// RemainingPartitions 该成员仍未确认撤销的分区（升序）；为空表示其义务完成。
	RemainingPartitions []int
	// CompletedRebalance 为 true 表示本次确认让整个版本收敛，所有权已整体就位。
	CompletedRebalance bool
}

// JoinResult 是加入组的返回值：成员立即进入新版本并获得该版本的所有权视图。
// 若新版本需要其他成员撤销分区（Phase=revoking），Assignment 仍是当前生效
// 所有权（新成员此时可能尚未生效持有任何分区），TargetAssignment 是最终目标。
type JoinResult struct {
	MemberID   string
	Generation int64
	// Phase 加入后组所处阶段。
	Phase  RebalancePhase
	Leader string
	// SessionVersion 本次加入获得的会话版本。静态成员每次成功加入严格递增；
	// 动态成员为 0。后续心跳/撤销确认/位点提交都必须回传它。
	SessionVersion int64
	// Reconnected 为 true 表示这是静态实例在保留期内以更高会话版本重连：
	// 协调器未推进分配版本，新会话直接继续原分配（离线期间分区未被转移）。
	Reconnected bool
	// Assignment 当前生效所有权快照。
	Assignment Assignment
	// TargetAssignment 本版本的目标所有权快照；Phase=stable 时与 Assignment 一致。
	TargetAssignment Assignment
}

// HeartbeatResult 是心跳的返回值。
type HeartbeatResult struct {
	Generation int64
	// SessionVersion 该成员当前会话版本（静态成员重连后递增），
	// 供客户端核对并在后续请求中回传。
	SessionVersion int64
	// Phase 组当前阶段。
	Phase RebalancePhase
	// Online 发送心跳的会话是否仍是该实例的当前会话（恒为 true——
	// 旧会话的心跳在栅栏处直接被拒，不会返回成功结果）。
	Online bool
	// Assignment 当前生效所有权快照，客户端可据此核对本地视图。
	Assignment Assignment
	// TargetAssignment 本版本的目标所有权；撤销阶段与 Assignment 可能不同。
	TargetAssignment Assignment
	// Revoking 本成员在当前版本下必须确认撤销的分区（升序）；
	// 为空表示该成员本版本无需撤销，但其消费的分区也可能正由别人移交而来。
	Revoking []int
}

// CommitResult 是位点提交的返回值。
type CommitResult struct {
	// Offset 提交后生效的位点（可能大于请求值，见并发提交说明）。
	Offset int64
	// Replayed 为 true 表示请求号是重放：位点与首次提交一致，未重复推进。
	Replayed bool
}

// CreateGroupOptions 创建组时的选项。
type CreateGroupOptions struct {
	// Name 组名，必填。
	Name string
	// Partitions 主题固定分区数，必须 > 0。
	Partitions int
	// SessionTimeout 会话超时：在线成员超过该时长未发送有效心跳即会被
	// 判定为断线（静态成员进入离线保留期，动态成员直接剔除）。<= 0 时使用默认值。
	SessionTimeout time.Duration
	// StaticRetention 静态成员离线保留期：静态实例断线后在此期限内以更高会话
	// 版本重连可继续原分配，不转移分区；超过该期限才被清退并把整批分区
	// 纳入协作式再均衡。<= 0 时使用默认值。
	StaticRetention time.Duration
}

// JoinOptions 是一次加入组请求。
type JoinOptions struct {
	// Group 组名，必填。
	Group string
	// MemberID 成员标识。
	// 动态成员：进程生成的临时 ID，断线即被剔除。
	// 静态成员：稳定实例 ID（Static=true），跨进程重启保持不变。
	MemberID string
	// Static 为 true 时以静态成员身份加入：启用会话版本栅栏与离线保留期。
	Static bool
	// SessionVersion 客户端期望使用的会话版本，仅静态成员有意义：
	//   - 首次加入传 0（或 <=0），协调器分配初始会话版本 1；
	//   - 重连接管必须传严格大于当前会话版本的值（同一实例两个进程并发加入时，
	//     只有版本更高者成为当前会话）；
	//   - 传一个不大于当前会话版本的值返回 ErrStaleSession，无法抢占当前会话。
	// 动态成员忽略该字段（恒为 0）。
	SessionVersion int64
	// Retention 仅静态成员有效：本次身份使用的离线保留期；<= 0 使用组默认值。
	// 重连时可调整（续期/缩短），对当前及之后的离线周期生效。
	Retention time.Duration
}

// CommitRequest 是一次位点提交请求。
type CommitRequest struct {
	// MemberID 提交成员，必须是 Partition 在当前生效所有权下的所有者。
	// 处于待撤销状态的分区在旧所有者确认撤销前仍归其有效持有，可提交最终位点；
	// 一旦分区转移完成，旧成员的迟到提交会被所有权栅栏（ErrNotOwner）阻止。
	MemberID string
	// Generation 成员持有的分配版本，与协调器不一致则拒绝。
	Generation int64
	// SessionVersion 静态成员当前会话版本，与 JoinMember 返回值严格一致；
	// 旧进程（较小版本）或离线保留期内的提交返回 ErrStaleSession。动态成员传 0。
	SessionVersion int64
	// Partition 目标分区编号，范围 [0, 主题分区数)。
	Partition int
	// Offset 待提交位点，默认不得小于已存位点。
	Offset int64
	// Metadata 可选不透明元数据。
	Metadata string
	// RequestID 幂等请求号（必填）：
	// 同成员 + 同请求号 + 同分区 + 同位点 => 视为重放，原样成功；
	// 同成员 + 同请求号但分区/位点不同 => ErrRequestConflict。
	RequestID string
	// AllowBacktrack 显式允许位点后退（如重置场景），默认 false。
	// 即便允许，请求号的内容一致性约束仍然生效。
	AllowBacktrack bool
}

// Clock 用于在测试中推进时间。
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
