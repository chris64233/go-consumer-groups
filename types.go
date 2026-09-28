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
// 普通（动态）成员：Static=false、Online 恒为 true、SessionVersion=0，
// 离开或会话超时即被彻底剔除。
// 静态成员：Static=true，以稳定的 InstanceID（== ID）标识；Online=false 表示
// 会话暂时断线但仍在保留期内占有所分区，此时 SessionVersion 是它上一个会话的
// 版本、RetainUntil 是保留期限。
type Member struct {
	// ID 成员唯一标识；静态成员即其实例标识。
	ID string
	// Static 为 true 表示这是一个带保留期的静态成员。
	Static bool
	// InstanceID 静态实例标识；静态成员与 ID 相同，动态成员为空。
	InstanceID string
	// Online 会话是否在线。静态成员断线后的保留期内为 false 但仍占有分区。
	Online bool
	// SessionVersion 静态成员当前（或离线前最后一个）会话的单调递增版本；
	// 每次成功加入（含保留期内重连、并发进程接管）+1。动态成员恒为 0。
	SessionVersion int64
	// JoinedAt 首次加入时间（单调时钟读数，用于审计/展示与 leader 选举）。
	// 静态成员重连不改变该值。
	JoinedAt time.Time
	// LastHeartbeatAt 最近一次被当前版本接受的心跳时间。
	LastHeartbeatAt time.Time
	// OfflineAt 静态成员最近一次由在线转为暂时离线的时间；在线时为零值。
	OfflineAt time.Time
	// RetainUntil 静态成员离线后的保留期限：超过该时刻仍未以更高会话版本
	// 重连，就会被超时扫描彻底清退；在线时为零值。
	RetainUntil time.Time
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
	// Leader 第一个加入组的成员；它退出后由当前存活成员中加入最早者接任，
	// 为空表示组内没有在线成员。静态成员保留期内离线时不计入在线集合。
	Leader  string
	Members []Member
	// StaticInstances 组内全部静态实例（含保留期内暂时离线者），按实例标识排序。
	// 动态成员不出现在这里。
	StaticInstances []StaticInstanceStatus
	// Assignment 当前生效的所有权快照。协作式再均衡期间它只包含「已经可以
	// 消费」的分区：未受影响分区保持原成员，待撤销分区仍是旧所有者，
	// 新分配的分区在旧所有者确认撤销前不会出现在这里。
	Assignment Assignment
	// TargetAssignment 本版本最终要达成的目标所有权；已稳定（PhaseStable）时
	// 与 Assignment 完全一致。撤销阶段二者的差异即尚在转移途中的分区。
	TargetAssignment Assignment
	// PendingRevocations 撤销阶段各成员仍需确认撤销的分区集合（会话 ID -> 升序分区号）。
	// 已全部确认的成员不会作为 key 出现；PhaseStable 时为空 map。
	PendingRevocations map[string][]int
	// RevocationProgress 各成员对当前版本撤销集合的确认进度：
	// Required 为该成员本版本应撤销的全部分区，Acked 为已确认的子集（均升序）。
	// 仅撤销阶段、且在本版本中需要撤销分区的成员会出现。
	RevocationProgress []RevocationProgress
	// PendingTransfers 所有权尚未最终落地、等待转移的分区：既包括协作式撤销阶段
	// 等待旧所有者确认的分区，也包括保留期内暂时离线的静态实例仍占有的分区
	// （后者尚未安排新所有者，TargetOwner 为空，等待重连或保留期届满）。
	PendingTransfers []PendingTransfer
	Offsets          []Offset
	LastRebalance    time.Time
}

// StaticInstanceStatus 是单个静态实例的状态视图。
type StaticInstanceStatus struct {
	// InstanceID 稳定实例标识（分区所有权也挂在该标识下，重连不变）。
	InstanceID string
	// Online 当前会话是否在线；false 表示保留期内暂时离线但仍占有分区。
	Online bool
	// SessionID 当前（在线时）或离线前最后一个会话的进程标识。
	SessionID string
	// SessionVersion 单调递增的会话版本：每次成功加入 +1。
	SessionVersion int64
	// JoinedAt 实例首次加入时间（重连不改变）。
	JoinedAt time.Time
	// LastHeartbeatAt 当前/最后会话最近一次被接受的心跳时间。
	LastHeartbeatAt time.Time
	// OfflineAt 最近一次转为暂时离线的时间；在线时为零值。
	OfflineAt time.Time
	// RetainUntil 保留期限：超过该时刻仍未以更高会话版本重连即被彻底清退。
	RetainUntil time.Time
	// HeldPartitions 该实例当前生效持有的分区（升序）。
	HeldPartitions []int
}

// PendingTransfer 描述一个等待转移的分区。
type PendingTransfer struct {
	Partition int
	// CurrentOwner 当前生效所有者：撤销阶段为旧所有者，保留期内为离线实例标识。
	CurrentOwner string
	// TargetOwner 这批分区唯一的新所有者；保留期待定期间为空串。
	TargetOwner string
	// Online 当前所有者会话是否在线（保留期内离线为 false）。
	Online bool
	// InstanceID 当前所有者为静态实例时的实例标识，动态成员为空。
	InstanceID string
	// SessionVersion 当前所有者静态会话的版本；动态成员为 0。
	SessionVersion int64
	// Reason 转移等待的原因。
	Reason PendingTransferReason
}

// PendingTransferReason 是待转移分区的等待原因。
type PendingTransferReason string

const (
	// TransferReasonRevoking 协作式撤销中：旧所有者在线，尚未确认撤销。
	TransferReasonRevoking PendingTransferReason = "revoking"
	// TransferReasonSuspended 旧所有者是保留期内暂时离线的静态实例，且本版本已
	// 为该分区指定新所有者：撤销义务挂起，等待旧实例以更高会话版本重连后确认，
	// 或保留期届满由协调器强制回收。分区在挂起期间仍由离线实例生效持有。
	TransferReasonSuspended PendingTransferReason = "suspended"
	// TransferReasonRetained 所有者为保留期内暂时离线的静态实例，本版本未安排
	// 新所有者：分区被钉住等待重连或保留期届满清退，TargetOwner 为空。
	TransferReasonRetained PendingTransferReason = "retained"
)

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

// RevocationAckRequest 是一次撤销确认请求。
type RevocationAckRequest struct {
	// MemberID 确认撤销的成员：动态成员为其成员 ID；静态成员为**当前会话 ID**
	// （加入时返回的 SessionID），不是稳定实例标识。
	MemberID string
	// SessionVersion 静态成员当前会话版本（加入返回值）；动态成员传 0。
	// 与协调器记录的当前会话不一致时返回 ErrFencedSession。
	SessionVersion int64
	// Generation 成员执行撤销所针对的分配版本，必须与协调器当前版本严格一致。
	Generation int64
	// Partitions 成员本次确认已停止消费、可以移交的分区集合。
	// 必须与其在该版本下应撤销的集合**精确相等**：漏项、额外分区或确认一个
	// 不属于自己撤销义务的分区都会被拒绝。空集合（或 nil）表示成员在本版本
	// 没有撤销义务，仅用于核对，会以 ErrNoRevocationInProgress 明确拒绝。
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
	// Assignment 当前生效所有权快照。
	Assignment Assignment
	// TargetAssignment 本版本的目标所有权快照；Phase=stable 时与 Assignment 一致。
	TargetAssignment Assignment
}

// StaticJoinOptions 是静态成员加入组的参数。
type StaticJoinOptions struct {
	// InstanceID 稳定实例标识（必填）：同一逻辑消费者进程的多次生命周期使用
	// 相同标识。分区所有权挂在该标识下，保留期内断线不会转移其分区。
	InstanceID string
	// SessionID 本次会话（进程）标识（必填）：同一实例的新旧进程应使用不同
	// 会话 ID。心跳、撤销确认、位点提交均以 (SessionID, SessionVersion) 栅栏，
	// 被接管的旧进程操作一律被拒绝。
	SessionID string
	// Retention 断线保留期（>0）：会话超时后该实例在此期限内仍占有分区、
	// 不参与再均衡；超过期限仍未重连才被彻底清退。
	Retention time.Duration
}

// StaticJoinResult 是静态成员加入组的返回值。
type StaticJoinResult struct {
	// InstanceID 稳定实例标识。
	InstanceID string
	// SessionID 本次会话 ID（== 入参）。
	SessionID string
	// SessionVersion 分配给本次会话的单调递增版本；每次成功加入 +1。
	// 后续心跳/撤销确认/位点提交都必须携带它。
	SessionVersion int64
	// Rejoined 为 true 表示这是保留期内（或并发进程接管时）的重连：
	// 实例继续原分配，不触发再均衡；false 表示实例首次（或清退后重新）加入。
	Rejoined   bool
	Generation int64
	// Phase 加入后组所处阶段。
	Phase  RebalancePhase
	Leader string
	// Assignment 当前生效所有权快照（包含该实例继续持有的分区）。
	Assignment Assignment
	// TargetAssignment 本版本的目标所有权快照。
	TargetAssignment Assignment
}

// HeartbeatRequest 是一次心跳请求。
type HeartbeatRequest struct {
	// MemberID 动态成员为成员 ID；静态成员为当前会话 ID。
	MemberID string
	// Generation 当前分配版本。
	Generation int64
	// SessionVersion 静态成员当前会话版本；动态成员传 0。
	SessionVersion int64
}

// HeartbeatResult 是心跳的返回值。
type HeartbeatResult struct {
	Generation int64
	// Phase 组当前阶段。
	Phase RebalancePhase
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
	// SessionTimeout 会话超时：成员超过该时长未发送有效心跳即会被
	// 超时扫描剔除。<= 0 时使用默认值。
	SessionTimeout time.Duration
}

// CommitRequest 是一次位点提交请求。
type CommitRequest struct {
	// MemberID 提交成员：动态成员为成员 ID；静态成员为**当前会话 ID**。
	// 处于待撤销状态的分区在旧所有者确认撤销前仍归其有效持有，可提交最终位点；
	// 一旦分区转移完成，旧成员的迟到提交会被所有权栅栏（ErrNotOwner）阻止；
	// 静态成员被更高会话版本接管后，旧进程的提交会被会话栅栏（ErrFencedSession）
	// 阻止。
	MemberID string
	// SessionVersion 静态成员当前会话版本；动态成员传 0。
	SessionVersion int64
	// Generation 成员持有的分配版本，与协调器不一致则拒绝。
	Generation int64
	// Partition 目标分区编号，范围 [0, 主题分区数)。
	Partition int
	// Offset 待提交位点，默认不得小于已存位点。
	Offset int64
	// Metadata 可选不透明元数据。
	Metadata string
	// RequestID 幂等请求号（必填）：
	// 同实例 + 同请求号 + 同分区 + 同位点 => 视为重放，原样成功；
	// 同实例 + 同请求号但分区/位点不同 => ErrRequestConflict。
	// 其作用域为静态实例本身（跨重连仍生效），动态成员则为一次在组生命周期。
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
