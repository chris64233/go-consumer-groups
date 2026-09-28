package consumergroups

import (
	"errors"
	"fmt"
)

// Sentinel errors. Callers can classify failures with errors.Is, regardless of
// the concrete wrapped type carrying contextual fields.
var (
	// ErrGroupNotFound 组不存在。
	ErrGroupNotFound = errors.New("consumer group not found")
	// ErrGroupAlreadyExists 同名组已存在。
	ErrGroupAlreadyExists = errors.New("consumer group already exists")
	// ErrMemberNotFound 成员不属于当前组（可能已被新版本淘汰）。
	ErrMemberNotFound = errors.New("group member not found")
	// ErrMemberAlreadyExists 成员 ID 已在组内。
	ErrMemberAlreadyExists = errors.New("group member already exists")
	// ErrIllegalGeneration 操作携带的分配版本与当前版本不一致（过旧或超前）。
	// 心跳与位点提交携带旧版本时一律被拒绝。
	ErrIllegalGeneration = errors.New("illegal generation: stale allocation version")
	// ErrNotOwner 成员不是该分区在当前版本下的所有者。
	ErrNotOwner = errors.New("member is not the current partition owner")
	// ErrRequestConflict 同一请求号被用于提交不同的分区或位点。
	ErrRequestConflict = errors.New("idempotency request id conflict")
	// ErrOffsetBacktrack 位点默认单调递增，提交更小的位点会被拒绝。
	ErrOffsetBacktrack = errors.New("offset commit rejected: offset cannot move backwards")
	// ErrInvalidPartition 分区编号越界（主题分区数固定）。
	ErrInvalidPartition = errors.New("invalid partition")
	// ErrInvalidArgument 参数非法。
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrNoRevocationInProgress 组当前处于 stable 阶段、没有待确认的撤销，
	// 或成员在当前版本下没有任何撤销义务，撤销确认无的放矢。
	ErrNoRevocationInProgress = errors.New("no revocation in progress")
	// ErrRevocationMismatch 撤销确认的分区集合与协调器为该成员/版本计算出的
	// 应撤销集合不精确相等（漏项或携带额外分区）。
	ErrRevocationMismatch = errors.New("revocation acknowledgement partition set mismatch")
	// ErrStaleSession 操作来自已被栅栏的旧会话：静态实例已被更高会话版本接管，
	// 请求携带的会话版本与当前会话不一致，或实例正处于离线保留期（尚无当前会话）。
	// 旧进程的心跳、撤销确认与位点提交一律返回此错误，不能确认新版本的撤销集合，
	// 也不能在新会话提交位点后把位点改回旧值。
	ErrStaleSession = errors.New("stale session: fenced by a newer session version")
	// ErrStaticIdentityConflict 同一成员 ID 已以另一种身份类型（动态/静态）在组内，
	// 不能用冲突的身份类型重复加入。
	ErrStaticIdentityConflict = errors.New("member id already in use with a different membership kind")
)

// GenerationMismatchError 在操作携带的分配版本与当前版本不一致时返回。
// errors.Is(err, ErrIllegalGeneration) 成立，同时可通过字段明确判别双方版本。
type GenerationMismatchError struct {
	Group string
	// Want 协调器当前分配版本。
	Want int64
	// Got 请求携带的分配版本。
	Got int64
}

func (e *GenerationMismatchError) Error() string {
	return fmt.Sprintf("%s: group=%q current generation=%d, request generation=%d",
		ErrIllegalGeneration, e.Group, e.Want, e.Got)
}

// Is 让 errors.Is(err, ErrIllegalGeneration) 成立。
func (e *GenerationMismatchError) Is(target error) bool { return target == ErrIllegalGeneration }

// NotPartitionOwnerError 在非当前所有者提交位点时返回。
type NotPartitionOwnerError struct {
	Group     string
	Partition int
	// Owner 当前版本下该分区的所有者；组为空时为空串。
	Owner string
	// Member 发起提交的成员。
	Member string
}

func (e *NotPartitionOwnerError) Error() string {
	return fmt.Sprintf("%s: group=%q partition=%d owner=%q member=%q",
		ErrNotOwner, e.Group, e.Partition, e.Owner, e.Member)
}

func (e *NotPartitionOwnerError) Is(target error) bool { return target == ErrNotOwner }

// RequestConflictError 在同一请求号携带不同内容时返回。
type RequestConflictError struct {
	Group     string
	Member    string
	RequestID string
	// ExistingPartition / ExistingOffset 为该请求号首次提交时记录的内容。
	ExistingPartition int
	ExistingOffset    int64
	// GotPartition / GotOffset 为本次冲突请求携带的内容。
	GotPartition int
	GotOffset    int64
}

func (e *RequestConflictError) Error() string {
	return fmt.Sprintf("%s: group=%q member=%q request=%q existing=(partition=%d, offset=%d) got=(partition=%d, offset=%d)",
		ErrRequestConflict, e.Group, e.Member, e.RequestID,
		e.ExistingPartition, e.ExistingOffset, e.GotPartition, e.GotOffset)
}

func (e *RequestConflictError) Is(target error) bool { return target == ErrRequestConflict }

// OffsetBacktrackError 在提交位点小于已存位点且未显式允许后退时返回。
type OffsetBacktrackError struct {
	Group     string
	Partition int
	// Current 已持久化的较大位点。
	Current int64
	// Requested 本次被拒绝的较小位点。
	Requested int64
}

func (e *OffsetBacktrackError) Error() string {
	return fmt.Sprintf("%s: group=%q partition=%d current=%d requested=%d",
		ErrOffsetBacktrack, e.Group, e.Partition, e.Current, e.Requested)
}

func (e *OffsetBacktrackError) Is(target error) bool { return target == ErrOffsetBacktrack }

// RevocationMismatchError 在撤销确认的分区集合与该成员本版本应撤销集合
// 不精确相等时返回：Missing 为漏掉的分区，Extra 为多确认的分区。
// errors.Is(err, ErrRevocationMismatch) 成立。
type RevocationMismatchError struct {
	Group    string
	Member   string
	WantGen  int64
	Expected []int
	Got      []int
	Missing  []int
	Extra    []int
}

func (e *RevocationMismatchError) Error() string {
	return fmt.Sprintf("%s: group=%q member=%q generation=%d expected=%v got=%v missing=%v extra=%v",
		ErrRevocationMismatch, e.Group, e.Member, e.WantGen, e.Expected, e.Got, e.Missing, e.Extra)
}

func (e *RevocationMismatchError) Is(target error) bool { return target == ErrRevocationMismatch }

// StaleSessionError 在请求来自已被栅栏的旧会话时返回：
// 静态实例已被更高会话版本接管，或实例处于离线保留期。
// errors.Is(err, ErrStaleSession) 成立，字段可区分两种情形。
type StaleSessionError struct {
	Group  string
	Member string
	// Want 该实例登记的当前会话版本（离线保留期内为最近一次会话版本）。
	Want int64
	// Got 请求携带的会话版本。
	Got int64
	// Offline 为 true 表示实例当前没有任何活跃会话（处于离线保留期），
	// 任何旧进程的请求都不应被接受。
	Offline bool
}

func (e *StaleSessionError) Error() string {
	if e.Offline {
		return fmt.Sprintf("%s: group=%q member=%q is offline within retention (current session=%d, request session=%d)",
			ErrStaleSession, e.Group, e.Member, e.Want, e.Got)
	}
	return fmt.Sprintf("%s: group=%q member=%q current session=%d, request session=%d",
		ErrStaleSession, e.Group, e.Member, e.Want, e.Got)
}

func (e *StaleSessionError) Is(target error) bool { return target == ErrStaleSession }
