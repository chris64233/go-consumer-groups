package consumergroups

import "time"

// Member 是组内一个消费者成员的运行时状态。
type Member struct {
	// ID 成员唯一标识。
	ID string
	// JoinedAt 加入时间（单调时钟读数，用于审计/展示）。
	JoinedAt time.Time
	// LastHeartbeatAt 最近一次被当前版本接受的心跳时间。
	LastHeartbeatAt time.Time
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
	// Leader 第一个加入组的成员；它退出后由当前存活成员中加入最早者接任，
	// 为空表示组内没有成员。
	Leader        string
	Members       []Member
	Assignment    Assignment
	Offsets       []Offset
	LastRebalance time.Time
}

// JoinResult 是加入组的返回值：成员立即进入新版本并获得整份分配。
type JoinResult struct {
	MemberID   string
	Generation int64
	Leader     string
	// Assignment 当前版本（含新成员）的完整分配快照。
	Assignment Assignment
}

// HeartbeatResult 是心跳的返回值。
type HeartbeatResult struct {
	Generation int64
	// Assignment 当前版本的完整分配快照，客户端可据此核对本地视图。
	Assignment Assignment
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
	// MemberID 提交成员，必须是 Partition 在当前版本下的所有者。
	MemberID string
	// Generation 成员持有的分配版本，与协调器不一致则拒绝。
	Generation int64
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
